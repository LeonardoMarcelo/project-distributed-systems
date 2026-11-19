"""Worker em conteiner: consome tarefas por AMQP e as executa na VM de processo.

O ciclo de uma tarefa aqui:

  1. consome uma mensagem da fila duravel, com `prefetch_count=1`
  2. tenta tomar a posse no Consul; se outro worker e dono, confirma e descarta
  3. roda `executor.run_task`, que executa passo a passo na VM de processo e
     grava checkpoint
  4. `done`/`failed` -> `basic_ack`; `yielded` -> `basic_nack(requeue=True)`,
     e a tarefa volta para a fila para outro worker retomar do checkpoint

O passo 4 e a migracao do item E vista do lado do worker. Dois gatilhos levam a
`yielded`:

  desligamento  SIGTERM; o handler marca a parada e o laco cede entre passos,
                em vez de morrer no meio e deixar a mensagem presa
  sobrecarga    carga acima do limiar configurado

O terceiro gatilho — queda abrupta, em que nao ha chance de ceder nada — e
responsabilidade do reaper do coordenador, via expiracao do TTL no Consul.

Nota sobre a deteccao de sobrecarga: `os.getloadavg()` dentro de um conteiner
reporta a carga do *host*, nao a do cgroup. Isso serve para a decisao real
(um host afogado e um host afogado), mas nao serve para demonstracao, porque o
numero depende do que mais estiver rodando na maquina de quem assiste. Por isso
existe tambem `MINIGRID_YIELD_AFTER_STEPS`, um gatilho deterministico usado pelo
roteiro do video e pelos testes. As duas vias publicam `migrated` com `reason`
distinto, e o relatorio diz qual e qual.
"""
import json
import os
import platform
import signal
import sys
import threading
import time

import pika

from minigrid import envelope, events
from worker import executor, progress, registry, sandbox


class WorkerConfig(object):
    def __init__(self, env=None):
        env = env or os.environ
        self.worker_id = env.get("MINIGRID_WORKER_ID") or (
            "worker-" + platform.node() + "-" + str(os.getpid())
        )
        self.consul_url = env.get("MINIGRID_CONSUL", "http://consul:8500")
        self.amqp_url = env.get("MINIGRID_AMQP",
                                "amqp://guest:guest@rabbitmq:5672/%2F")
        self.queue = env.get("MINIGRID_QUEUE", "minigrid.tasks")
        self.results_queue = env.get("MINIGRID_RESULTS_QUEUE", "minigrid.results")
        self.events_exchange = env.get("MINIGRID_EVENTS_EXCHANGE", "minigrid.events")
        self.kv_prefix = env.get("MINIGRID_KV_PREFIX", "minigrid/")
        self.virt = env.get("MINIGRID_VIRT", "container")
        self.capabilities = [c for c in env.get("MINIGRID_CAPS", "generic").split(",") if c]
        self.checkpoint_every = int(env.get("MINIGRID_CHECKPOINT_EVERY", "1"))
        self.ttl = int(env.get("MINIGRID_TTL", "10"))
        self.heartbeat_every = int(env.get("MINIGRID_HEARTBEAT", "4"))

        # 0 desliga. Comparado com a carga normalizada por CPU.
        self.overload_threshold = float(env.get("MINIGRID_OVERLOAD_THRESHOLD", "0"))
        # 0 desliga. Gatilho deterministico de cessao, para demo e teste.
        self.yield_after_steps = int(env.get("MINIGRID_YIELD_AFTER_STEPS", "0"))

        self.limits = sandbox.Limits(
            cpu_seconds=int(env.get("MINIGRID_CPU_SECONDS", "30")),
            address_space_mb=int(env.get("MINIGRID_MEM_MB", "1024")),
            file_size_mb=int(env.get("MINIGRID_FSIZE_MB", "16")),
            wall_seconds=int(env.get("MINIGRID_WALL_SECONDS", "120")),
        )


class AmqpTransport(executor.Transport):
    """Liga o executor ao RabbitMQ e ao Consul.

    O executor nao conhece nenhum dos dois: ele recebe este objeto. A mesma
    classe existe em STOMP puro no worker do host (host_worker/worker.py), e e
    por isso que o laco de execucao e identico nos dois ambientes.
    """

    def __init__(self, publisher, consul, config, channel_sender):
        self.publisher = publisher
        self.consul = consul
        self.config = config
        self._send = channel_sender

    def _checkpoint_key(self, task_id):
        return self.config.kv_prefix + "tasks/" + task_id + "/checkpoint"

    def publish_event(self, kind, payload):
        self.publisher.publish(kind, payload)
        # O evento tambem e projetado no servico de nomes. Sem isto `GetStatus`
        # nao reporta progresso nenhum: salta de `received` para `done`, com
        # worker_id vazio durante toda a execucao. Ver worker/progress.py.
        progress.project(self.consul, self.config.kv_prefix, kind, payload)

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
        self._send(self.config.results_queue, body)

    def send_result(self, task_id, result, meta):
        self._report(task_id, "done", result, "", meta)

    def report_failure(self, task_id, error, meta):
        # Mesma fila duravel do resultado, de proposito: e o que faz a falha
        # chegar ao estado autoritativo no servico de nomes.
        self._report(task_id, "failed", b"", error, meta)


class Worker(object):
    def __init__(self, config, consul):
        self.config = config
        self.consul = consul
        self.publisher = events.Publisher(config.amqp_url, config.events_exchange)
        self.registration = registry.WorkerRegistration(
            consul, config.worker_id, virt=config.virt,
            capabilities=config.capabilities, ttl=config.ttl,
            heartbeat_every=config.heartbeat_every, kv_prefix=config.kv_prefix,
        )
        self._stop = threading.Event()
        self._stop_reason = "shutdown"
        self._connection = None
        self._channel = None
        self._steps_seen = 0

    # ------------------------------------------------------------------ sinais

    def request_stop(self, reason="shutdown"):
        """Pede a parada. Nao mata nada no meio: o laco cede entre passos."""
        self._stop_reason = reason
        self._stop.set()

    def install_signal_handlers(self):
        def handler(signum, frame):
            self.request_stop("shutdown")
        signal.signal(signal.SIGTERM, handler)
        signal.signal(signal.SIGINT, handler)

    # ------------------------------------------------------------- sobrecarga

    def _overloaded(self):
        if self.config.overload_threshold <= 0:
            return False
        try:
            carga = os.getloadavg()[0] / float(os.cpu_count() or 1)
        except (OSError, AttributeError):
            return False
        return carga > self.config.overload_threshold

    def _should_stop(self):
        if self._stop.is_set():
            return True
        if self.config.yield_after_steps:
            self._steps_seen += 1
            if self._steps_seen >= self.config.yield_after_steps:
                self._stop_reason = "overload"
                return True
        if self._overloaded():
            self._stop_reason = "overload"
            return True
        return False

    # ------------------------------------------------------------------- laco

    def run(self):
        self.registration.start()
        print("worker " + self.config.worker_id + " registrado: arch="
              + self.registration.arch + " virt=" + self.config.virt
              + " fila=" + self.config.queue, flush=True)
        try:
            while not self._stop.is_set():
                try:
                    self._consume_once()
                except Exception as exc:
                    # Fila ainda nao declarada pelo coordenador, ou broker fora
                    # do ar. Tentar em laco com recuo e o que faz o item G
                    # funcionar: a ordem de subida deixa de importar.
                    print("worker aguardando o broker/fila: " + str(exc), flush=True)
                    self._stop.wait(2)
        finally:
            self.registration.stop()
            self.publisher.close()
            self._discard()
            print("worker " + self.config.worker_id + " encerrado", flush=True)

    def _discard(self):
        for obj in (self._channel, self._connection):
            try:
                if obj is not None and obj.is_open:
                    obj.close()
            except Exception:
                pass
        self._channel = None
        self._connection = None

    def _send_to_queue(self, queue, body):
        self._channel.basic_publish(
            exchange="", routing_key=queue, body=body,
            properties=pika.BasicProperties(content_type="application/json",
                                            delivery_mode=2),
        )

    def _consume_once(self):
        self._connection = pika.BlockingConnection(
            pika.URLParameters(self.config.amqp_url)
        )
        self._channel = self._connection.channel()
        # passive: quem declara a topologia e o coordenador. Se a fila nao
        # existe ainda, levanta e o laco de fora espera — em vez de criar uma
        # fila paralela com configuracao divergente.
        self._channel.queue_declare(queue=self.config.queue, durable=True, passive=True)
        self._channel.basic_qos(prefetch_count=1)

        transport = AmqpTransport(self.publisher, self.consul, self.config,
                                  self._send_to_queue)

        try:
            for method, properties, body in self._channel.consume(
                self.config.queue, auto_ack=False, inactivity_timeout=1.0
            ):
                if method is None:
                    if self._stop.is_set():
                        return
                    continue
                if self._stop.is_set():
                    self._channel.basic_nack(method.delivery_tag, requeue=True)
                    return
                self._handle(method, body, transport)
                if self._stop.is_set():
                    return
        finally:
            self._discard()

    def _handle(self, method, body, transport):
        try:
            task = envelope.decode(body)
        except envelope.MalformedEnvelope as exc:
            # Nao devolve para a fila: reentregar um envelope ilegivel para
            # sempre entupiria o worker.
            print("envelope ilegivel, descartado: " + str(exc), flush=True)
            self._channel.basic_ack(method.delivery_tag)
            return

        task_id = task["task_id"]
        if not self.registration.acquire_task(task_id):
            # Outro worker detem a posse. Confirma e sai do caminho: esta e a
            # defesa contra execucao dupla depois de um requeue, quando o worker
            # antigo pode ainda estar vivo.
            print("tarefa " + task_id + " ja tem dono; descartando copia",
                  flush=True)
            self._channel.basic_ack(method.delivery_tag)
            return

        self._steps_seen = 0
        try:
            outcome = executor.run_task(
                task, transport, self.config.worker_id,
                limits=self.config.limits,
                checkpoint_every=self.config.checkpoint_every,
                should_stop=self._should_stop,
                # Invocavel, nao valor: o motivo so e decidido dentro de
                # `_should_stop`, depois desta chamada. Ver executor.run_task.
                stop_reason=lambda: self._stop_reason,
                virt=self.config.virt,
            )
        except Exception as exc:
            # Falha inesperada do proprio worker (nao da tarefa): devolve a
            # tarefa para a fila em vez de perde-la.
            print("falha do worker na tarefa " + task_id + ": " + str(exc),
                  flush=True)
            self.registration.release_task(task_id)
            self._channel.basic_nack(method.delivery_tag, requeue=True)
            return

        if outcome == "yielded":
            self.registration.release_task(task_id)
            self._channel.basic_nack(method.delivery_tag, requeue=True)
        else:
            self._channel.basic_ack(method.delivery_tag)
            self.registration.release_task(task_id)


def main():
    from minigrid import naming

    config = WorkerConfig()
    consul = naming.Consul(config.consul_url)
    worker = Worker(config, consul)
    worker.install_signal_handlers()
    worker.run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
