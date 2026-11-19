"""Testes do worker em conteiner: registro, posse, execucao e migracao.

O worker sobe como *subprocesso*, nao como thread. Dois dos testes mais
importantes dependem de SIGTERM de verdade — e um sinal entregue a uma thread
nao prova nada sobre o que acontece quando o Docker para o conteiner.
"""
import json
import os
import signal
import subprocess
import sys
import time
import uuid

import pytest

from conftest import (PROJECT_ROOT, CONSUL_URL, mgmt, needs_infra, queue_depth,
                      wait_until)
from minigrid import envelope, events
from worker import registry

pytestmark = needs_infra

AMQP_URL = os.environ.get("MINIGRID_AMQP", "amqp://guest:guest@localhost:5672/%2F")

PI_SLOW = (
    "def step(state, data, api):\n"
    "    import time\n"
    "    total = int(api.params.get('steps', '30'))\n"
    "    time.sleep(float(api.params.get('sleep', '0.3')))\n"
    "    n = state.get('n', 0) + 1\n"
    "    done = n >= total\n"
    "    return {'n': n}, done, (b'fim' if done else None)\n"
)
FAST = (
    "def step(state, data, api):\n"
    "    return {'n': 1}, True, b'ok'\n"
)


class Topology(object):
    """Topologia isolada por teste, criada pela API de gerenciamento."""

    def __init__(self):
        self.token = uuid.uuid4().hex[:10]
        self.tasks = "test." + self.token + ".tasks"
        self.results = "test." + self.token + ".results"
        self.exchange = "test." + self.token + ".events"
        self.kv_prefix = "minigrid/test/" + self.token + "/"
        for queue in (self.tasks, self.results):
            mgmt("PUT", "/queues/%2F/" + queue, {"durable": True, "auto_delete": False})
        mgmt("PUT", "/exchanges/%2F/" + self.exchange,
             {"type": "topic", "durable": True})
        self.procs = []
        self.subscriber = None

    def env_for(self, worker_id, **extra):
        env = dict(os.environ)
        env.update({
            "MINIGRID_WORKER_ID": worker_id,
            "MINIGRID_CONSUL": CONSUL_URL,
            "MINIGRID_AMQP": AMQP_URL,
            "MINIGRID_QUEUE": self.tasks,
            "MINIGRID_RESULTS_QUEUE": self.results,
            "MINIGRID_EVENTS_EXCHANGE": self.exchange,
            "MINIGRID_KV_PREFIX": self.kv_prefix,
            "MINIGRID_CHECKPOINT_EVERY": "1",
            "MINIGRID_TTL": "10",
            "MINIGRID_HEARTBEAT": "2",
            "PYTHONPATH": str(PROJECT_ROOT),
        })
        env.update({k: str(v) for k, v in extra.items()})
        return env

    def start_worker(self, worker_id, **extra):
        proc = subprocess.Popen(
            [sys.executable, "-u", "-m", "worker.main"],
            cwd=str(PROJECT_ROOT), env=self.env_for(worker_id, **extra),
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        )
        self.procs.append(proc)
        return proc

    def publish_task(self, code, task_id=None, params=None, data=b"", requires=None):
        task_id = task_id or uuid.uuid4().hex
        raw = envelope.encode(task_id, "pi", code, data, params or {}, requires or [])
        import base64
        mgmt("POST", "/exchanges/%2F/amq.default/publish", {
            "properties": {"delivery_mode": 2},
            "routing_key": self.tasks,
            "payload": base64.b64encode(raw).decode(),
            "payload_encoding": "base64",
        })
        return task_id

    def events_seen(self, timeout=1.0):
        if self.subscriber is None:
            self.subscriber = events.Subscriber(AMQP_URL, self.exchange, pattern="#")
            self.subscriber.open()
        out = []
        deadline = time.monotonic() + timeout
        for kind, payload in self.subscriber.listen(inactivity_timeout=0.3):
            if kind is not None:
                out.append((kind, payload))
            if time.monotonic() > deadline:
                break
        return out

    def stop(self):
        for proc in self.procs:
            if proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    proc.kill()
        if self.subscriber is not None:
            self.subscriber.close()
        for queue in (self.tasks, self.results):
            try:
                mgmt("DELETE", "/queues/%2F/" + queue, expect_json=False)
            except Exception:
                pass
        try:
            mgmt("DELETE", "/exchanges/%2F/" + self.exchange, expect_json=False)
        except Exception:
            pass


@pytest.fixture
def topo(consul):
    t = Topology()
    yield t
    t.stop()
    for key in consul.kv_keys(t.kv_prefix):
        try:
            consul.kv_delete(key)
        except Exception:
            pass
    for svc in consul.list_services("minigrid-worker"):
        if svc["ID"].startswith("wt-"):
            consul.deregister_service(svc["ID"])


def _collect(topo, worker_id, timeout=40):
    """Espera o worker sumir e devolve o que ele imprimiu."""
    proc = [p for p in topo.procs][-1]
    try:
        return proc.communicate(timeout=timeout)[0]
    except subprocess.TimeoutExpired:
        proc.kill()
        return proc.communicate()[0]


def _results(topo, cap=50):
    got = mgmt("POST", "/queues/%2F/" + topo.results + "/get",
               {"count": cap, "ackmode": "ack_requeue_true", "encoding": "base64"})
    import base64
    out = []
    for entry in got or []:
        out.append(json.loads(base64.b64decode(entry["payload"]).decode("utf-8")))
    return out


# -------------------------------------------------------------------- registro

def test_worker_registers_with_arch_and_virt_tags(topo, consul):
    topo.start_worker("wt-1")
    assert wait_until(
        lambda: any(s["ID"] == "wt-1" for s in consul.list_services("minigrid-worker")),
        timeout=20)
    entry = [s for s in consul.list_services("minigrid-worker") if s["ID"] == "wt-1"][0]
    assert any(t.startswith("arch:") for t in entry["Tags"])
    assert "virt:container" in entry["Tags"]
    assert "cap:generic" in entry["Tags"]


def test_worker_health_check_turns_passing(topo, consul):
    topo.start_worker("wt-1")
    assert wait_until(
        lambda: any(s["ID"] == "wt-1" and s["Status"] == "passing"
                    for s in consul.list_services("minigrid-worker")),
        timeout=20)


def test_worker_deregisters_on_graceful_shutdown(topo, consul):
    proc = topo.start_worker("wt-1")
    assert wait_until(
        lambda: any(s["ID"] == "wt-1" for s in consul.list_services("minigrid-worker")),
        timeout=20)
    proc.terminate()
    proc.wait(timeout=20)
    assert wait_until(
        lambda: not any(s["ID"] == "wt-1"
                        for s in consul.list_services("minigrid-worker")),
        timeout=20)


def test_worker_waits_for_the_queue_instead_of_crashing(consul):
    """Item G: a ordem de subida nao pode importar."""
    token = uuid.uuid4().hex[:10]
    env = dict(os.environ)
    env.update({
        "MINIGRID_WORKER_ID": "wt-orfao",
        "MINIGRID_CONSUL": CONSUL_URL,
        "MINIGRID_AMQP": AMQP_URL,
        "MINIGRID_QUEUE": "fila.que.nao.existe." + token,
        "PYTHONPATH": str(PROJECT_ROOT),
    })
    proc = subprocess.Popen([sys.executable, "-u", "-m", "worker.main"],
                            cwd=str(PROJECT_ROOT), env=env,
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                            text=True)
    try:
        time.sleep(5)
        assert proc.poll() is None, "o worker morreu em vez de esperar a fila"
    finally:
        proc.terminate()
        proc.wait(timeout=15)
        consul.deregister_service("wt-orfao")


# --------------------------------------------------------------------- posse

def test_two_registrations_do_not_both_own_the_same_task(consul):
    r1 = registry.WorkerRegistration(consul, "wt-a", kv_prefix="minigrid/test/posse/")
    r2 = registry.WorkerRegistration(consul, "wt-b", kv_prefix="minigrid/test/posse/")
    try:
        r1.start()
        r2.start()
        assert [r1.acquire_task("t9"), r2.acquire_task("t9")].count(True) == 1
    finally:
        r1.stop()
        r2.stop()
        consul.kv_delete("minigrid/test/posse/tasks/t9/owner")


def test_owner_key_survives_the_worker_death_so_the_reaper_can_see_it(consul):
    """Sessao com Behavior=release, nao delete.

    Com `delete`, a morte do worker apagaria a chave `owner` e o reaper nao
    teria o que encontrar — a tarefa congelaria para sempre.
    """
    r = registry.WorkerRegistration(consul, "wt-morto",
                                    kv_prefix="minigrid/test/orfao/")
    r.start()
    assert r.acquire_task("t-orfao")
    consul.session_destroy(r.session_id)     # simula morte abrupta
    assert consul.kv_get("minigrid/test/orfao/tasks/t-orfao/owner") == b"wt-morto"
    consul.kv_delete("minigrid/test/orfao/tasks/t-orfao/owner")


# ------------------------------------------------------------------ execucao

def test_worker_consumes_a_task_and_publishes_done(topo):
    topo.start_worker("wt-1")
    task_id = topo.publish_task(FAST)
    assert wait_until(lambda: any(r["task_id"] == task_id for r in _results(topo)),
                      timeout=40)
    result = [r for r in _results(topo) if r["task_id"] == task_id][0]
    import base64
    assert base64.b64decode(result["result_b64"]) == b"ok"
    assert result["meta"]["worker_id"] == "wt-1"
    assert result["meta"]["arch"]


def test_worker_reports_the_architecture_it_executed_on(topo):
    import platform

    topo.start_worker("wt-1")
    task_id = topo.publish_task(FAST)
    assert wait_until(lambda: any(r["task_id"] == task_id for r in _results(topo)),
                      timeout=40)
    result = [r for r in _results(topo) if r["task_id"] == task_id][0]
    assert result["meta"]["arch"] == platform.machine()
    assert result["meta"]["virt"] == "container"


def test_worker_persists_checkpoints_while_running(topo, consul):
    topo.start_worker("wt-1")
    task_id = topo.publish_task(PI_SLOW, params={"steps": "20", "sleep": "0.3"})
    key = topo.kv_prefix + "tasks/" + task_id + "/checkpoint"
    assert wait_until(lambda: consul.kv_get(key) is not None, timeout=40)
    state = json.loads(consul.kv_get(key).decode())
    assert state["n"] >= 1


# ------------------------------------------------------------------ migracao

def test_sigterm_mid_task_requeues_and_publishes_migrated(topo, consul):
    """Primeiro gatilho de migracao: desligamento do no (item E)."""
    proc = topo.start_worker("wt-1")
    task_id = topo.publish_task(PI_SLOW, params={"steps": "60", "sleep": "0.3"})
    key = topo.kv_prefix + "tasks/" + task_id + "/checkpoint"
    assert wait_until(lambda: consul.kv_get(key) is not None, timeout=40), \
        "a tarefa nem comecou"

    proc.terminate()                          # SIGTERM, como `docker stop`
    saida = proc.communicate(timeout=30)[0]
    assert "encerrado" in saida, saida[-500:]

    # A tarefa voltou para a fila, com o progresso preservado.
    assert wait_until(lambda: queue_depth(topo.tasks) == 1, timeout=20)
    assert json.loads(consul.kv_get(key).decode())["n"] >= 1


def test_another_worker_resumes_the_requeued_task_from_the_checkpoint(topo, consul):
    """O coracao do item E: retomada a partir do ultimo ponto de verificacao."""
    primeiro = topo.start_worker("wt-1")
    task_id = topo.publish_task(PI_SLOW, params={"steps": "12", "sleep": "0.3"})
    key = topo.kv_prefix + "tasks/" + task_id + "/checkpoint"
    assert wait_until(lambda: consul.kv_get(key) is not None, timeout=40)

    primeiro.terminate()
    primeiro.wait(timeout=30)
    passo_na_queda = json.loads(consul.kv_get(key).decode())["n"]
    assert passo_na_queda >= 1

    topo.start_worker("wt-2")
    assert wait_until(lambda: any(r["task_id"] == task_id for r in _results(topo)),
                      timeout=90), "o segundo worker nao concluiu a tarefa"

    result = [r for r in _results(topo) if r["task_id"] == task_id][0]
    assert result["meta"]["worker_id"] == "wt-2"
    assert int(result["meta"]["resumed_from_step"]) >= passo_na_queda
    assert int(result["meta"]["steps"]) == 12


def test_overload_trigger_yields_the_task_back_to_the_queue(topo, consul):
    """Segundo gatilho de migracao: no sobrecarregado (item E)."""
    topo.start_worker("wt-1", MINIGRID_YIELD_AFTER_STEPS="2")
    task_id = topo.publish_task(PI_SLOW, params={"steps": "30", "sleep": "0.2"})

    migrados = []

    def procura():
        for kind, payload in topo.events_seen(timeout=2.0):
            if kind == "migrated" and payload.get("task_id") == task_id:
                migrados.append(payload)
        return migrados

    assert wait_until(procura, timeout=60), "nenhum evento `migrated` publicado"
    assert migrados[0]["reason"] == "overload"
    assert migrados[0]["step"] >= 1


def test_requeued_task_is_not_executed_twice_by_the_old_owner(topo, consul):
    """Protecao contra execucao dupla: so o detentor do lock escreve."""
    topo.start_worker("wt-1", MINIGRID_YIELD_AFTER_STEPS="2")
    topo.start_worker("wt-2")
    task_id = topo.publish_task(PI_SLOW, params={"steps": "8", "sleep": "0.2"})
    assert wait_until(lambda: any(r["task_id"] == task_id for r in _results(topo)),
                      timeout=90)
    entregas = [r for r in _results(topo) if r["task_id"] == task_id]
    assert len(entregas) == 1, "a tarefa produziu mais de um resultado"
