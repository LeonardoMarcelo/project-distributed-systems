"""Worker que roda FORA do Docker, na maquina do avaliador (item G).

O enunciado pede dois tipos diferentes de virtualizacao, e que "o ambiente
virtualizado que nao for conteiner deve ser iniciado por um script documentado
no README", funcionando o sistema "normalmente mesmo antes de esse trabalhador
se registrar, absorvendo-o assim que ele aparecer no servico de nomes".

Este e esse worker. Ele executa as tarefas numa **VM de processo** — um
interpretador isolado (worker/sandbox.py), que e o segundo tipo de virtualizacao
do item D. Isso tambem da heterogeneidade de *sistema operacional* de graca no
relatorio: Darwin ou Linux do host contra Linux dos conteineres.

A restricao que molda este arquivo: **nenhuma instalacao na maquina**. Nem
`pip`, nem venv, nem `brew`. O unico pre-requisito e um `python3` qualquer. Isso
elimina o `pika`, e e por isso que a conversa com o RabbitMQ aqui e STOMP
escrito a mao sobre socket TCP (minigrid/stomp.py) em vez de AMQP. O Consul ja
e HTTP/JSON, resolvido com `urllib`.

Consequencia visivel no relatorio: a *mesma fila duravel* e consumida por dois
protocolos diferentes — AMQP pelos workers em conteiner, STOMP por este. A
tabela de classificacao de fluxos ganha uma linha que nenhum dos dois caminhos
teria sozinho.

Compativel com Python 3.9+ de proposito: roda no interpretador que o avaliador
tiver, nao no mais novo.
"""
import json
import os
import platform
import signal
import sys
import time

# Caminho do projeto no sys.path quando chamado como script, nao como modulo.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from minigrid import envelope, naming, stomp          # noqa: E402
from worker import executor, registry, sandbox        # noqa: E402


class HostConfig(object):
    def __init__(self, env=None):
        env = env or os.environ
        self.worker_id = env.get("MINIGRID_WORKER_ID") or (
            "host-" + platform.node().split(".")[0]
        )
        self.consul_url = env.get("MINIGRID_CONSUL", "http://localhost:8500")
        self.stomp_host = env.get("MINIGRID_STOMP_HOST", "localhost")
        self.stomp_port = int(env.get("MINIGRID_STOMP_PORT", "61613"))
        self.stomp_user = env.get("MINIGRID_STOMP_USER", "guest")
        self.stomp_pass = env.get("MINIGRID_STOMP_PASS", "guest")
        self.queue = env.get("MINIGRID_QUEUE", "minigrid.tasks")
        self.results_queue = env.get("MINIGRID_RESULTS_QUEUE", "minigrid.results")
        self.events_exchange = env.get("MINIGRID_EVENTS_EXCHANGE", "minigrid.events")
        self.kv_prefix = env.get("MINIGRID_KV_PREFIX", "minigrid/")
        self.capabilities = [c for c in env.get("MINIGRID_CAPS", "generic").split(",") if c]
        self.checkpoint_every = int(env.get("MINIGRID_CHECKPOINT_EVERY", "1"))
        self.ttl = int(env.get("MINIGRID_TTL", "10"))
        self.heartbeat_every = int(env.get("MINIGRID_HEARTBEAT", "4"))
        self.yield_after_steps = int(env.get("MINIGRID_YIELD_AFTER_STEPS", "0"))
        self.limits = sandbox.Limits(
            cpu_seconds=int(env.get("MINIGRID_CPU_SECONDS", "30")),
            address_space_mb=int(env.get("MINIGRID_MEM_MB", "1024")),
            file_size_mb=int(env.get("MINIGRID_FSIZE_MB", "16")),
            wall_seconds=int(env.get("MINIGRID_WALL_SECONDS", "120")),
        )


class StompTransport(executor.Transport):
    """O mesmo contrato do AmqpTransport, falado em STOMP.

    E esta simetria que permite ao `worker/executor.py` ser literalmente o
    mesmo codigo nos dois ambientes: o executor nao sabe que protocolo esta
    embaixo, so que existe um Transport.
    """

    def __init__(self, client, consul, config):
        self.client = client
        self.consul = consul
        self.config = config

    def _checkpoint_key(self, task_id):
        return self.config.kv_prefix + "tasks/" + task_id + "/checkpoint"

    def publish_event(self, kind, payload):
        body = dict(payload)
        body.setdefault("at", time.time())
        body.setdefault("kind", kind)
        routing = kind if "." in kind else "task." + kind
        try:
            self.client.send(
                "/exchange/" + self.config.events_exchange + "/" + routing,
                json.dumps(body, default=str).encode("utf-8"),
                persistent=False,
            )
        except Exception:
            # Evento e transiente por desenho; nao derruba a tarefa.
            pass

    def save_checkpoint(self, task_id, state):
        self.consul.kv_put(self._checkpoint_key(task_id),
                           json.dumps(state, default=str).encode("utf-8"))

    def load_checkpoint(self, task_id):
        raw = self.consul.kv_get(self._checkpoint_key(task_id))
        if not raw:
            return None
        try:
            return json.loads(raw.decode("utf-8"))
        except ValueError:
            return None

    def clear_checkpoint(self, task_id):
        try:
            self.consul.kv_delete(self._checkpoint_key(task_id))
        except Exception:
            pass

    def _report(self, task_id, state, result, error, meta):
        import base64
        body = json.dumps({
            "task_id": task_id,
            "state": state,
            "error": error or "",
            "result_b64": base64.b64encode(result or b"").decode("ascii"),
            "meta": {str(k): str(v) for k, v in (meta or {}).items()},
        }).encode("utf-8")
        self.client.send("/amq/queue/" + self.config.results_queue, body,
                         persistent=True)

    def send_result(self, task_id, result, meta):
        self._report(task_id, "done", result, "", meta)

    def report_failure(self, task_id, error, meta):
        self._report(task_id, "failed", b"", error, meta)


class HostWorker(object):
    def __init__(self, config, consul):
        self.config = config
        self.consul = consul
        self.registration = registry.WorkerRegistration(
            consul, config.worker_id,
            virt="process-vm",
            capabilities=config.capabilities,
            ttl=config.ttl, heartbeat_every=config.heartbeat_every,
            kv_prefix=config.kv_prefix,
            extra_tags=["os:" + platform.system()],
        )
        self._stopping = False
        self._stop_reason = "shutdown"
        self._steps_seen = 0
        self.client = None

    def request_stop(self, reason="shutdown"):
        self._stop_reason = reason
        self._stopping = True

    def install_signal_handlers(self):
        def handler(signum, frame):
            self.request_stop("shutdown")
        signal.signal(signal.SIGTERM, handler)
        signal.signal(signal.SIGINT, handler)

    def _should_stop(self):
        if self._stopping:
            return True
        if self.config.yield_after_steps:
            self._steps_seen += 1
            if self._steps_seen >= self.config.yield_after_steps:
                self._stop_reason = "overload"
                return True
        return False

    # -------------------------------------------------------------------- laco

    def run(self):
        self.registration.start()
        print("worker do host " + self.config.worker_id + " registrado: arch="
              + self.registration.arch + " virt=process-vm os=" + platform.system(),
              flush=True)
        try:
            while not self._stopping:
                try:
                    self._session()
                except Exception as exc:
                    # Broker fora do ar, ou a fila ainda nao foi declarada pelo
                    # coordenador. Tentar em laco com recuo e o que faz o item G
                    # funcionar: este worker pode subir antes de tudo.
                    print("aguardando broker/fila: " + str(exc), flush=True)
                    self._sleep(2)
        finally:
            self.registration.stop()
            if self.client is not None:
                self.client.close()
            print("worker do host " + self.config.worker_id + " encerrado",
                  flush=True)

    def _sleep(self, seconds):
        deadline = time.time() + seconds
        while time.time() < deadline and not self._stopping:
            time.sleep(0.2)

    def _session(self):
        self.client = stomp.StompClient(
            self.config.stomp_host, self.config.stomp_port,
            self.config.stomp_user, self.config.stomp_pass,
        )
        self.client.connect()
        # `/amq/queue/<nome>` assina uma fila AMQP que *ja existe*: e a mesma
        # fila duravel que os workers em conteiner consomem, nao uma paralela.
        self.client.subscribe("/amq/queue/" + self.config.queue,
                              ack="client", prefetch=1)
        transport = StompTransport(self.client, self.consul, self.config)
        print("assinado /amq/queue/" + self.config.queue, flush=True)

        try:
            while not self._stopping:
                frame = self.client.receive(timeout=1.0)
                if frame is None:
                    continue              # tique: permite checar a parada
                self._handle(frame, transport)
        finally:
            try:
                self.client.close()
            except Exception:
                pass
            self.client = None

    def _handle(self, frame, transport):
        try:
            task = envelope.decode(frame.body)
        except envelope.MalformedEnvelope as exc:
            print("envelope ilegivel, descartado: " + str(exc), flush=True)
            self.client.ack(frame)
            return

        task_id = task["task_id"]
        if not self.registration.acquire_task(task_id):
            print("tarefa " + task_id + " ja tem dono; descartando copia", flush=True)
            self.client.ack(frame)
            return

        self._steps_seen = 0
        try:
            outcome = executor.run_task(
                task, transport, self.config.worker_id,
                limits=self.config.limits,
                checkpoint_every=self.config.checkpoint_every,
                should_stop=self._should_stop,
                stop_reason=lambda: self._stop_reason,
                virt="process-vm",
            )
        except Exception as exc:
            print("falha do worker na tarefa " + task_id + ": " + str(exc), flush=True)
            self.registration.release_task(task_id)
            self.client.nack(frame, requeue=True)
            return

        if outcome == "yielded":
            self.registration.release_task(task_id)
            self.client.nack(frame, requeue=True)
        else:
            self.client.ack(frame)
            self.registration.release_task(task_id)


def main():
    config = HostConfig()
    consul = naming.Consul(config.consul_url)
    worker = HostWorker(config, consul)
    worker.install_signal_handlers()
    worker.run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
