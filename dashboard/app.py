"""Painel de monitoramento ao vivo (item C).

O enunciado pede que as mudancas de estado sejam divulgadas em pub/sub "de modo
que varios clientes e um painel de monitoramento possam acompanha-las". Este e o
painel. Ele e mais um assinante como qualquer outro: cria a propria fila
exclusiva ligada ao exchange de eventos e nao interfere em quem mais estiver
ouvindo.

Duas fontes, por motivos diferentes:

  *push*  — eventos de tarefa e rastreios de RPC chegam pelo exchange, e sao
            reenviados ao navegador por Server-Sent Events. Sem polling no
            caminho que precisa ser instantaneo.
  *poll*  — presenca de worker e quem e o lider vem do Consul a cada 2 s. Essas
            coisas nao geram evento: um worker que morre nao avisa, e e
            justamente por isso que existe health check por TTL.

Nota sobre a profundidade de fila: aqui ela vem de `messages_ready` da API de
gerenciamento, que e uma estatistica coletada por intervalo e atrasa alguns
segundos. Nos testes a leitura e autoritativa (`POST .../get` com requeue),
porque la a exatidao importa mais que a gentileza. Num painel o contrario e
verdade: espiar a fila a cada 2 s marcaria toda mensagem como reentregue.
"""
import base64
import json
import os
import pathlib
import queue
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

from fastapi import FastAPI
from fastapi.responses import FileResponse, StreamingResponse

from minigrid import events, naming

HERE = pathlib.Path(__file__).resolve().parent
STATIC = HERE / "static"

CONSUL_URL = os.environ.get("MINIGRID_CONSUL", "http://consul:8500")
AMQP_URL = os.environ.get("MINIGRID_AMQP", "amqp://guest:guest@rabbitmq:5672/%2F")
RABBIT_MGMT = os.environ.get("MINIGRID_RABBIT_MGMT",
                             "http://guest:guest@rabbitmq:15672/api")
EXCHANGE = os.environ.get("MINIGRID_EVENTS_EXCHANGE", "minigrid.events")
KV_PREFIX = os.environ.get("MINIGRID_KV_PREFIX", "minigrid/")
LEADER_KEY = KV_PREFIX + "coordinator/leader"
QUEUES = ["minigrid.tasks", "minigrid.tasks.legacy", "minigrid.results"]

MAX_EVENTS = 300
MAX_TRACES = 60
MAX_TASKS = 80

TERMINAL = ("done", "failed")


class Hub(object):
    """Estado em memoria do painel, e o fan-out para os navegadores ligados."""

    def __init__(self):
        self.lock = threading.Lock()
        self.events = []
        self.traces = []
        self.tasks = {}          # task_id -> resumo, na ordem de chegada
        self.workers = []
        self.leader = None
        self.queue_depth = {}
        self.started_at = time.time()
        self.broker_ok = None
        self.consul_ok = None
        self._listeners = []

    # ------------------------------------------------------------- fan-out

    def subscribe(self):
        q = queue.Queue(maxsize=200)
        with self.lock:
            self._listeners.append(q)
        return q

    def unsubscribe(self, q):
        with self.lock:
            if q in self._listeners:
                self._listeners.remove(q)

    def _broadcast(self, message):
        encoded = json.dumps(message, default=str)
        with self.lock:
            ouvintes = list(self._listeners)
        for q in ouvintes:
            try:
                q.put_nowait(encoded)
            except queue.Full:
                # Navegador lento nao pode segurar o painel; descarta o evento
                # para ele e segue. O /api/state o reconcilia no proximo carregamento.
                pass

    # -------------------------------------------------------------- eventos

    def on_event(self, kind, payload):
        if kind == "rpc.trace":
            with self.lock:
                self.traces.append(payload)
                del self.traces[:-MAX_TRACES]
            self._broadcast({"tipo": "rpc", "dados": payload})
            return

        task_id = payload.get("task_id")
        if not task_id:
            return

        with self.lock:
            self.events.append(payload)
            del self.events[:-MAX_EVENTS]

            resumo = self.tasks.get(task_id)
            if resumo is None:
                resumo = {
                    "task_id": task_id,
                    "recipe": payload.get("recipe") or "",
                    "state": kind,
                    "worker_id": "",
                    "arch": "",
                    "virt": "",
                    "step": 0,
                    "migrations": 0,
                    "error": "",
                    "first_at": payload.get("at") or time.time(),
                    "trail": [],      # alimenta as raias: por onde a tarefa passou
                }
                self.tasks[task_id] = resumo
                if len(self.tasks) > MAX_TASKS:
                    for antigo in list(self.tasks)[:len(self.tasks) - MAX_TASKS]:
                        self.tasks.pop(antigo, None)

            resumo["state"] = kind
            resumo["at"] = payload.get("at") or time.time()
            for campo in ("worker_id", "arch", "virt", "recipe"):
                if payload.get(campo):
                    resumo[campo] = payload[campo]
            if payload.get("step"):
                resumo["step"] = payload["step"]
            if payload.get("error"):
                resumo["error"] = payload["error"]
            if kind == "migrated":
                resumo["migrations"] += 1
                resumo["reason"] = payload.get("reason") or ""

            # A trilha e o que desenha a raia: um trecho por worker que executou.
            trilha = resumo["trail"]
            worker = resumo["worker_id"]
            if worker and (not trilha or trilha[-1]["worker_id"] != worker
                           or trilha[-1].get("closed")):
                trilha.append({
                    "worker_id": worker, "arch": resumo["arch"],
                    "virt": resumo["virt"], "from_step": resumo["step"],
                    "to_step": resumo["step"], "at": resumo["at"],
                    "state": kind, "closed": False,
                })
            elif trilha:
                trilha[-1]["to_step"] = resumo["step"]
                trilha[-1]["state"] = kind
            if trilha and kind in ("migrated",) + TERMINAL:
                trilha[-1]["closed"] = True
                trilha[-1]["state"] = kind

        self._broadcast({"tipo": "tarefa", "dados": dict(resumo),
                         "evento": payload})

    # ---------------------------------------------------------------- estado

    def snapshot(self):
        with self.lock:
            return {
                "workers": list(self.workers),
                "leader": self.leader,
                "queue_depth": dict(self.queue_depth),
                "tasks": [dict(t) for t in self.tasks.values()],
                "traces": list(self.traces),
                "uptime": round(time.time() - self.started_at, 1),
                "broker_ok": self.broker_ok,
                "consul_ok": self.consul_ok,
            }


hub = Hub()
app = FastAPI(title="MiniGrid — painel de monitoramento")


def _consume_events():
    """Assina o exchange e repassa. Reabre sozinho se o broker cair."""
    while True:
        subscriber = events.Subscriber(AMQP_URL, EXCHANGE, pattern="#")
        try:
            subscriber.open()
            with hub.lock:
                hub.broker_ok = True
            for kind, payload in subscriber.listen(inactivity_timeout=1.0):
                if kind is not None:
                    hub.on_event(kind, payload)
        except Exception:
            with hub.lock:
                hub.broker_ok = False
            time.sleep(2)
        finally:
            subscriber.close()


def _split_credentials(url):
    """Separa `http://user:senha@host/api` em (base_sem_credencial, header_basic).

    `urllib` nao usa a credencial embutida na URL: ela e simplesmente ignorada,
    e o RabbitMQ responde 401. Isso fazia a profundidade das filas aparecer como
    indisponivel no painel — com o broker perfeitamente no ar.
    """
    partes = urllib.parse.urlsplit(url)
    if not partes.username:
        return url, None
    porta = ":" + str(partes.port) if partes.port else ""
    base = urllib.parse.urlunsplit((
        partes.scheme, partes.hostname + porta, partes.path, "", "",
    ))
    credencial = (partes.username or "") + ":" + (partes.password or "")
    header = "Basic " + base64.b64encode(credencial.encode("utf-8")).decode("ascii")
    return base, header


MGMT_BASE, MGMT_AUTH = _split_credentials(RABBIT_MGMT)


def _mgmt_json(path):
    request = urllib.request.Request(MGMT_BASE + path)
    if MGMT_AUTH:
        request.add_header("Authorization", MGMT_AUTH)
    with urllib.request.urlopen(request, timeout=5) as response:
        return json.loads(response.read().decode("utf-8"))


def _poll_presence():
    """Presenca de worker e lideranca: coisas que nenhum evento anuncia."""
    consul = naming.Consul(CONSUL_URL)
    while True:
        try:
            registrados = consul.list_services("minigrid-worker")
            workers = []
            for svc in registrados:
                tags = svc.get("Tags") or []
                workers.append({
                    "id": svc.get("ID"),
                    "arch": _tag(tags, "arch:"),
                    "virt": _tag(tags, "virt:"),
                    "os": _tag(tags, "os:"),
                    "status": svc.get("Status"),
                    "capabilities": [t[4:] for t in tags if t.startswith("cap:")],
                })
            workers.sort(key=lambda w: (w["virt"] or "", w["id"] or ""))

            leader = None
            raw = consul.kv_get(LEADER_KEY)
            if raw:
                try:
                    leader = json.loads(raw.decode("utf-8"))
                except ValueError:
                    leader = None

            with hub.lock:
                hub.workers = workers
                hub.leader = leader
                hub.consul_ok = True
        except Exception:
            with hub.lock:
                hub.consul_ok = False

        depths = {}
        for nome in QUEUES:
            try:
                info = _mgmt_json("/queues/%2F/" + nome)
                depths[nome] = info.get("messages_ready", 0)
            except urllib.error.HTTPError as exc:
                depths[nome] = 0 if exc.code == 404 else None
            except Exception:
                depths[nome] = None
        with hub.lock:
            hub.queue_depth = depths

        hub._broadcast({"tipo": "presenca", "dados": {
            "workers": hub.workers, "leader": hub.leader,
            "queue_depth": hub.queue_depth,
            "broker_ok": hub.broker_ok, "consul_ok": hub.consul_ok,
        }})
        time.sleep(2)


def _tag(tags, prefix):
    for tag in tags:
        if tag.startswith(prefix):
            return tag[len(prefix):]
    return ""


@app.on_event("startup")
def _start_threads():
    threading.Thread(target=_consume_events, name="eventos", daemon=True).start()
    threading.Thread(target=_poll_presence, name="presenca", daemon=True).start()


@app.get("/api/state")
def state():
    return hub.snapshot()


@app.get("/events")
def stream():
    def gerador():
        q = hub.subscribe()
        try:
            yield "retry: 2000\n\n"
            yield "data: " + json.dumps({"tipo": "inicial",
                                         "dados": hub.snapshot()},
                                        default=str) + "\n\n"
            while True:
                try:
                    yield "data: " + q.get(timeout=15) + "\n\n"
                except queue.Empty:
                    # Comentario SSE: segura a conexao viva atravessando proxies
                    # sem produzir um evento falso no navegador.
                    yield ": ping\n\n"
        finally:
            hub.unsubscribe(q)

    return StreamingResponse(gerador(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache",
                                      "X-Accel-Buffering": "no"})


@app.get("/")
def index():
    return FileResponse(str(STATIC / "index.html"))
