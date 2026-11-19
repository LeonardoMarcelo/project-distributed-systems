"""Servidor gRPC do coordenador: a fronteira entre o item B e o item C.

Do lado do cliente, chamadas remotas com interface uniforme. Do lado dos
workers, uma fila duravel. O cliente nunca ve um worker; os workers nunca veem
um cliente.

Duas replicas sobem iguais. Quem adquire o lock no Consul vira lider e passa a
aceitar *escritas* (`Submit`), a coletar resultados e a rodar o reaper. A
replica seguidora continua respondendo *leituras* (`GetStatus`, `GetResult`,
`Watch`, `ListWorkers`) — util porque o painel nao precisa saber quem e o lider
para funcionar. Submissao na replica errada devolve FAILED_PRECONDITION, e a
biblioteca do cliente reconsulta o servico de nomes e tenta a outra: e dai que
sai a transparencia de falhas do item A.

Onde cada coisa e guardada, e por que:

  Consul KV      metadado da tarefa e checkpoint — pequeno, precisa ser legivel
                 por qualquer replica. Limite de 512 KB por valor.
  volume         envelope submetido e blob de resultado — vao a 8 MB, nao cabem
                 no KV. Compartilhado pelas duas replicas.
  RabbitMQ       a tarefa em si, em fila duravel com mensagem persistente.
"""
import concurrent.futures
import json
import os
import threading
import time
import uuid

import grpc
import pika

from coordinator import dispatch, reaper as reaper_mod, results as results_mod
from coordinator import topology
from coordinator.election import LeaderElection
from coordinator.interceptor import TraceInterceptor
from minigrid import envelope, events
from minigrid._pb2 import minigrid_pb2 as pb
from minigrid._pb2 import minigrid_pb2_grpc as pb_grpc

#: kind textual do evento -> enum do protobuf. Os nomes sao os mesmos que o
#: executor publica, de proposito: o painel le o texto, o cliente le o enum.
STATE_BY_KIND = {
    "received": pb.RECEIVED,
    "running": pb.RUNNING,
    "checkpoint": pb.CHECKPOINT,
    "migrated": pb.MIGRATED,
    "done": pb.DONE,
    "failed": pb.FAILED,
}
TERMINAL_KINDS = ("done", "failed")


class Config(object):
    def __init__(self, kv_prefix="minigrid/",
                 tasks_queue="minigrid.tasks",
                 legacy_queue="minigrid.tasks.legacy",
                 results_queue="minigrid.results",
                 events_exchange="minigrid.events",
                 election_key="minigrid/coordinator/leader",
                 worker_service="minigrid-worker"):
        self.kv_prefix = kv_prefix
        self.tasks_queue = tasks_queue
        self.legacy_queue = legacy_queue
        self.results_queue = results_queue
        self.events_exchange = events_exchange
        self.election_key = election_key
        self.worker_service = worker_service


class MiniGridServer(object):
    def __init__(self, consul, config=None, name="coord-1",
                 internal_addr="localhost:50051", external_addr="localhost:50051",
                 blob_dir=None, port=50051, amqp_url=None, reap_every=5):
        self.consul = consul
        self.config = config or Config()
        self.name = name
        self.blob_dir = blob_dir
        self.amqp_url = amqp_url or os.environ.get(
            "MINIGRID_AMQP", "amqp://guest:guest@localhost:5672/%2F"
        )
        self.reap_every = reap_every

        self.publisher = events.Publisher(self.amqp_url, self.config.events_exchange)
        self.election = LeaderElection(
            consul, name,
            addresses={"internal": internal_addr, "external": external_addr},
            key=self.config.election_key,
        )
        self.election.on_change(self._on_leadership_change)

        self.reaper = reaper_mod.Reaper(
            consul, self.config, self._republish,
            publish_event=self.publisher.publish,
            worker_service=self.config.worker_service,
        )
        self.collector = results_mod.ResultCollector(
            self.amqp_url, consul, self.config, blob_dir,
            publish_event=self.publisher.publish,
        )

        self._amqp_lock = threading.Lock()
        self._amqp_conn = None
        self._amqp_channel = None
        self._reap_stop = threading.Event()
        self._reap_thread = None
        self._grpc = None
        self.port = port
        self._requested_port = port

    # ------------------------------------------------------------ ciclo de vida

    def start(self):
        # Topologia declarada por qualquer replica, nao so pelo lider: se ela
        # dependesse da eleicao, um worker que subisse antes do primeiro lider
        # nao teria fila para assinar, e o item G exige o contrario.
        self._with_channel(lambda channel: topology.declare(channel, self.config))

        self._grpc = grpc.server(
            concurrent.futures.ThreadPoolExecutor(max_workers=16),
            interceptors=[TraceInterceptor(self.publisher.publish)],
            # Sem isto o teto real seria o padrao do gRPC, 4 MB, e o limite de
            # 8 MB que o sistema anuncia seria falso. Ver envelope.GRPC_MAX_MESSAGE.
            options=list(envelope.GRPC_CHANNEL_OPTIONS),
        )
        pb_grpc.add_MiniGridServicer_to_server(_Servicer(self), self._grpc)
        bound = self._grpc.add_insecure_port("0.0.0.0:" + str(self._requested_port))
        self.port = bound
        self._grpc.start()
        self.election.start()
        return self

    def stop(self, grace=0):
        self._stop_leader_duties()
        try:
            self.election.stop()
        except Exception:
            pass
        if self._grpc is not None:
            self._grpc.stop(grace).wait()
            self._grpc = None
        self.publisher.close()
        with self._amqp_lock:
            self._discard_amqp()

    def wait_for_termination(self):
        if self._grpc is not None:
            self._grpc.wait_for_termination()

    def is_leader(self):
        return self.election.is_leader()

    # ----------------------------------------------------------- lideranca

    def _on_leadership_change(self, is_leader):
        if is_leader:
            self._start_leader_duties()
        else:
            self._stop_leader_duties()

    def _start_leader_duties(self):
        try:
            self._with_channel(lambda channel: topology.declare(channel, self.config))
        except Exception:
            pass
        self.collector.start()
        self._reap_stop.clear()
        if self._reap_thread is None:
            self._reap_thread = threading.Thread(
                target=self._reap_loop, name="reaper", daemon=True
            )
            self._reap_thread.start()

    def _stop_leader_duties(self):
        self._reap_stop.set()
        thread = self._reap_thread
        self._reap_thread = None
        if thread is not None:
            thread.join(timeout=3)
        self.collector.stop()

    def _reap_loop(self):
        while not self._reap_stop.wait(self.reap_every):
            if not self.is_leader():
                return
            try:
                self.reaper.tick()
            except Exception:
                pass

    # ---------------------------------------------------------------- AMQP

    def _with_channel(self, action):
        """Executa `action(channel)` com a conexao AMQP de despacho.

        Serializado por lock porque `pika.BlockingConnection` nao e segura
        entre threads e aqui publicam varias threads do pool do gRPC.
        """
        with self._amqp_lock:
            for tentativa in (1, 2):
                try:
                    if self._amqp_channel is None or not self._amqp_channel.is_open:
                        self._amqp_conn = pika.BlockingConnection(
                            pika.URLParameters(self.amqp_url)
                        )
                        self._amqp_channel = self._amqp_conn.channel()
                    return action(self._amqp_channel)
                except Exception:
                    self._discard_amqp()
                    if tentativa == 2:
                        raise

    def _discard_amqp(self):
        for obj in (self._amqp_channel, self._amqp_conn):
            try:
                if obj is not None and obj.is_open:
                    obj.close()
            except Exception:
                pass
        self._amqp_channel = None
        self._amqp_conn = None

    # ------------------------------------------------------------------- KV

    def _meta_key(self, task_id):
        return self.config.kv_prefix + "tasks/" + task_id + "/meta"

    def read_meta(self, task_id):
        raw = self.consul.kv_get(self._meta_key(task_id))
        if not raw:
            return None
        try:
            return json.loads(raw.decode("utf-8"))
        except ValueError:
            return None

    def write_meta(self, task_id, record):
        record["updated_at"] = time.time()
        self.consul.kv_put(self._meta_key(task_id),
                           json.dumps(record, default=str).encode("utf-8"))
        return record

    def set_task_state(self, task_id, state, **fields):
        record = self.read_meta(task_id) or {"task_id": task_id}
        record["state"] = state
        record.update(fields)
        return self.write_meta(task_id, record)

    # -------------------------------------------------------------- volume

    def _envelope_path(self, task_id):
        if not self.blob_dir:
            return None
        return os.path.join(self.blob_dir, "env", task_id)

    def _store_envelope(self, task_id, raw):
        path = self._envelope_path(task_id)
        if path is None:
            return None
        directory = os.path.dirname(path)
        if not os.path.isdir(directory):
            os.makedirs(directory)
        with open(path, "wb") as handle:
            handle.write(raw)
        return path

    def _load_envelope(self, task_id):
        path = self._envelope_path(task_id)
        if path is None or not os.path.exists(path):
            return None
        with open(path, "rb") as handle:
            return handle.read()

    def read_result_blob(self, task_id):
        if not self.blob_dir:
            return b""
        path = os.path.join(self.blob_dir, "out", task_id)
        if not os.path.exists(path):
            return b""
        with open(path, "rb") as handle:
            return handle.read()

    # ------------------------------------------------------------ operacoes

    def submit_task(self, recipe, code, data, params, requires, client_id=""):
        """Aceita a tarefa, grava o metadado, despacha e devolve o id."""
        task_id = uuid.uuid4().hex
        raw = envelope.encode(task_id, recipe, code, data, params, requires)
        queue = dispatch.route(requires, self.config)

        self._store_envelope(task_id, raw)
        self.write_meta(task_id, {
            "task_id": task_id,
            "recipe": recipe,
            "state": "received",
            "worker_id": "",
            "arch": "",
            "virt": "",
            "step": 0,
            "resumed_from_step": 0,
            "migrations": 0,
            "error": "",
            "requires": list(requires or []),
            "queue": queue,
            "client_id": client_id,
            "submitted_at": time.time(),
        })
        self._with_channel(lambda channel: dispatch.submit(channel, raw, queue, task_id))
        self.publisher.publish("received", {
            "task_id": task_id, "recipe": recipe, "queue": queue,
            "worker_id": "", "arch": "", "virt": "", "step": 0,
        })
        return task_id

    def _republish(self, task_id, meta, checkpoint):
        """Usado pelo reaper: devolve a tarefa a fila a partir do envelope salvo.

        O checkpoint nao vai na mensagem: ele continua no Consul KV, de onde o
        proximo worker o carrega. A mensagem so precisa levar codigo e dados.
        """
        raw = self._load_envelope(task_id)
        if raw is None:
            raise RuntimeError("envelope de " + task_id + " nao esta no volume")
        queue = meta.get("queue") or dispatch.route(meta.get("requires"), self.config)
        self._with_channel(lambda channel: dispatch.submit(channel, raw, queue, task_id))
        return queue

    def publish_result(self, task_id, result, meta, state="done", error=""):
        """Publica um resultado na fila de resultados.

        Em producao quem chama isto e o worker. Exposto aqui porque o teste do
        coletor precisa fazer o papel de um worker que concluiu.
        """
        import base64
        body = json.dumps({
            "task_id": task_id,
            "state": state,
            "error": error,
            "result_b64": base64.b64encode(result or b"").decode("ascii"),
            "meta": {str(k): str(v) for k, v in (meta or {}).items()},
        }).encode("utf-8")
        self._with_channel(lambda channel: channel.basic_publish(
            exchange="", routing_key=self.config.results_queue, body=body,
            properties=pika.BasicProperties(content_type="application/json",
                                            delivery_mode=2),
        ))

    def list_workers(self):
        out = []
        for svc in self.consul.list_services(self.config.worker_service):
            tags = svc.get("Tags") or []
            out.append({
                "id": svc.get("ID") or "",
                "arch": _tag_value(tags, "arch:"),
                "virt": _tag_value(tags, "virt:"),
                "status": svc.get("Status") or "",
                "capabilities": [t[len("cap:"):] for t in tags if t.startswith("cap:")],
                "tags": tags,
            })
        return out


def _tag_value(tags, prefix):
    for tag in tags:
        if tag.startswith(prefix):
            return tag[len(prefix):]
    return ""


def _event_from_meta(meta):
    kind = meta.get("state") or "received"
    return pb.TaskEvent(
        task_id=meta.get("task_id", ""),
        state=STATE_BY_KIND.get(kind, pb.STATE_UNSPECIFIED),
        kind=kind,
        worker_id=meta.get("worker_id", "") or "",
        arch=meta.get("arch", "") or "",
        virt=meta.get("virt", "") or "",
        step=int(meta.get("step", 0) or 0),
        error=meta.get("error", "") or "",
        at=float(meta.get("updated_at", 0) or 0),
    )


class _Servicer(pb_grpc.MiniGridServicer):
    def __init__(self, server):
        self.server = server

    # ---------------------------------------------------------------- escrita

    def Submit(self, request, context):
        if not self.server.is_leader():
            leader = self.server.election.current_leader() or {}
            context.abort(
                grpc.StatusCode.FAILED_PRECONDITION,
                "esta replica nao e o lider; lider atual: "
                + str(leader.get("internal") or "desconhecido"),
            )
        if not request.code:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT,
                          "a tarefa precisa trazer o codigo a executar")
        if len(request.data) > envelope.MAX_PAYLOAD:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                "payload de " + str(len(request.data)) + " bytes excede o limite de "
                + str(envelope.MAX_PAYLOAD) + " bytes",
            )
        try:
            task_id = self.server.submit_task(
                request.recipe, request.code, request.data,
                dict(request.params), list(request.requires), request.client_id,
            )
        except envelope.PayloadTooLarge as exc:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(exc))
        except Exception as exc:
            context.abort(grpc.StatusCode.UNAVAILABLE,
                          "falha ao despachar a tarefa: " + str(exc))
        return pb.SubmitReply(task_id=task_id)

    # ---------------------------------------------------------------- leitura

    def GetStatus(self, request, context):
        meta = self.server.read_meta(request.task_id)
        if meta is None:
            context.abort(grpc.StatusCode.NOT_FOUND,
                          "tarefa desconhecida: " + request.task_id)
        return pb.TaskStatus(
            task_id=request.task_id,
            state=STATE_BY_KIND.get(meta.get("state"), pb.STATE_UNSPECIFIED),
            recipe=meta.get("recipe", "") or "",
            worker_id=meta.get("worker_id", "") or "",
            arch=meta.get("arch", "") or "",
            virt=meta.get("virt", "") or "",
            step=int(meta.get("step", 0) or 0),
            resumed_from_step=int(meta.get("resumed_from_step", 0) or 0),
            migrations=int(meta.get("migrations", 0) or 0),
            error=meta.get("error", "") or "",
            submitted_at=float(meta.get("submitted_at", 0) or 0),
            updated_at=float(meta.get("updated_at", 0) or 0),
        )

    def GetResult(self, request, context):
        meta = self.server.read_meta(request.task_id)
        if meta is None:
            context.abort(grpc.StatusCode.NOT_FOUND,
                          "tarefa desconhecida: " + request.task_id)
        return pb.TaskResult(
            task_id=request.task_id,
            state=STATE_BY_KIND.get(meta.get("state"), pb.STATE_UNSPECIFIED),
            result=self.server.read_result_blob(request.task_id),
            meta={str(k): str(v) for k, v in (meta.get("meta") or {}).items()},
        )

    def Watch(self, request, context):
        """Stream de servidor com as transicoes da tarefa (item B, assincrono).

        Ordem importa: primeiro valida a existencia, depois assina, e so depois
        emite o estado atual. Assinar antes de ler evita a janela em que uma
        transicao acontece entre a leitura e a assinatura e some.
        """
        meta = self.server.read_meta(request.task_id)
        if meta is None:
            context.abort(grpc.StatusCode.NOT_FOUND,
                          "tarefa desconhecida: " + request.task_id)

        subscriber = events.Subscriber(
            self.server.amqp_url, self.server.config.events_exchange, pattern="task.#"
        )
        try:
            subscriber.open()
        except Exception:
            # Sem broker de eventos ainda da para responder o estado atual e
            # encerrar — melhor que abortar a chamada do cliente.
            yield _event_from_meta(self.server.read_meta(request.task_id) or meta)
            return

        try:
            current = self.server.read_meta(request.task_id) or meta
            yield _event_from_meta(current)
            if current.get("state") in TERMINAL_KINDS:
                return

            for kind, payload in subscriber.listen(inactivity_timeout=1.0):
                if not context.is_active():
                    return
                if kind is None:
                    # Tique de inatividade: confere se a tarefa terminou por um
                    # caminho que nao publicou evento (reinicio do coletor).
                    latest = self.server.read_meta(request.task_id)
                    if latest and latest.get("state") in TERMINAL_KINDS:
                        yield _event_from_meta(latest)
                        return
                    continue
                if payload.get("task_id") != request.task_id:
                    continue
                yield pb.TaskEvent(
                    task_id=request.task_id,
                    state=STATE_BY_KIND.get(kind, pb.STATE_UNSPECIFIED),
                    kind=kind,
                    worker_id=payload.get("worker_id", "") or "",
                    arch=payload.get("arch", "") or "",
                    virt=payload.get("virt", "") or "",
                    step=int(payload.get("step", 0) or 0),
                    reason=payload.get("reason", "") or "",
                    error=payload.get("error", "") or "",
                    at=float(payload.get("at", 0) or 0),
                )
                if kind in TERMINAL_KINDS:
                    return
        finally:
            subscriber.close()

    def ListWorkers(self, request, context):
        leader = self.server.election.current_leader() or {}
        return pb.WorkerList(
            workers=[pb.Worker(**worker) for worker in self.server.list_workers()],
            leader=str(leader.get("internal") or ""),
        )


def serve():
    """Ponto de entrada do conteiner. Configuracao por variavel de ambiente."""
    from minigrid import naming

    name = os.environ.get("MINIGRID_NAME", "coord-1")
    port = int(os.environ.get("MINIGRID_PORT", "50051"))
    consul = naming.Consul(os.environ.get("MINIGRID_CONSUL", "http://consul:8500"))
    server = MiniGridServer(
        consul=consul,
        config=Config(),
        name=name,
        internal_addr=os.environ.get("MINIGRID_INTERNAL_ADDR", name + ":" + str(port)),
        external_addr=os.environ.get("MINIGRID_EXTERNAL_ADDR",
                                     "localhost:" + str(port)),
        blob_dir=os.environ.get("MINIGRID_BLOBS", "/blobs"),
        port=port,
        amqp_url=os.environ.get("MINIGRID_AMQP",
                                "amqp://guest:guest@rabbitmq:5672/%2F"),
    )

    import signal

    def _shutdown(signum, frame):
        # Parada graciosa: destruir a sessao do Consul libera a chave na hora e
        # o failover sai de ~20 s (expiracao por TTL) para ~0,2 s.
        server.stop(grace=2)
        raise SystemExit(0)

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    print("coordenador " + name + " ouvindo em :" + str(port), flush=True)
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
