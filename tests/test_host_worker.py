"""Testes do worker do host (item G).

Duas garantias que nenhum outro teste cobre:

1. *Zero dependencia de terceiro.* Se este worker puxar `pika` ou `grpc` por
   acidente, ele quebra na maquina do avaliador, que nao instalou nada. O teste
   de pureza roda o import sem site-packages.

2. *Ordem de subida irrelevante.* O sistema tem de funcionar antes dele e
   absorve-lo quando aparece.
"""
import base64
import json
import os
import subprocess
import sys
import time
import uuid

import pytest

from conftest import CONSUL_URL, PROJECT_ROOT, mgmt, needs_infra, wait_until
from minigrid import envelope

from test_worker import FAST, PI_SLOW, Topology

pytestmark = needs_infra

STOMP_HOST = os.environ.get("MINIGRID_STOMP_HOST", "localhost")
STOMP_PORT = os.environ.get("MINIGRID_STOMP_PORT", "61613")


class HostTopology(Topology):
    """Mesma topologia isolada, mas sobe o worker do host em vez do AMQP."""

    def env_for(self, worker_id, **extra):
        env = Topology.env_for(self, worker_id, **extra)
        env["MINIGRID_STOMP_HOST"] = STOMP_HOST
        env["MINIGRID_STOMP_PORT"] = STOMP_PORT
        env["MINIGRID_CONSUL"] = CONSUL_URL
        return env

    def start_worker(self, worker_id, **extra):
        proc = subprocess.Popen(
            [sys.executable, "-u", "-m", "host_worker.worker"],
            cwd=str(PROJECT_ROOT), env=self.env_for(worker_id, **extra),
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        )
        self.procs.append(proc)
        return proc


@pytest.fixture
def topo(consul):
    t = HostTopology()
    yield t
    t.stop()
    for key in consul.kv_keys(t.kv_prefix):
        try:
            consul.kv_delete(key)
        except Exception:
            pass
    for svc in consul.list_services("minigrid-worker"):
        if svc["ID"].startswith("hw-"):
            consul.deregister_service(svc["ID"])


def _results(topo, cap=50):
    got = mgmt("POST", "/queues/%2F/" + topo.results + "/get",
               {"count": cap, "ackmode": "ack_requeue_true", "encoding": "base64"})
    return [json.loads(base64.b64decode(e["payload"]).decode("utf-8"))
            for e in (got or [])]


# -------------------------------------------------------------------- pureza

def test_host_worker_imports_with_no_third_party_packages():
    """Review Focus 1. `-S -E`: sem site-packages, sem PYTHONPATH."""
    env = {k: v for k, v in os.environ.items() if k != "PYTHONPATH"}
    r = subprocess.run(
        [sys.executable, "-S", "-E", "-c", "import host_worker.worker"],
        cwd=str(PROJECT_ROOT), capture_output=True, text=True, env=env,
    )
    assert r.returncode == 0, r.stderr


def test_host_worker_does_not_import_pika_or_grpc_even_indirectly():
    """Mais forte que o import: verifica o sys.modules depois de carregar."""
    env = {k: v for k, v in os.environ.items() if k != "PYTHONPATH"}
    r = subprocess.run(
        [sys.executable, "-S", "-E", "-c",
         "import sys, host_worker.worker as w;"
         "ruins = [m for m in sys.modules if m.split('.')[0] in ('pika','grpc')];"
         "print(ruins); sys.exit(1 if ruins else 0)"],
        cwd=str(PROJECT_ROOT), capture_output=True, text=True, env=env,
    )
    assert r.returncode == 0, "modulos de terceiro carregados: " + r.stdout


# ------------------------------------------------------------------ registro

def test_host_worker_registers_itself_as_process_vm(topo, consul):
    topo.start_worker("hw-1")
    assert wait_until(
        lambda: any(s["ID"] == "hw-1" for s in consul.list_services("minigrid-worker")),
        timeout=25)
    entry = [s for s in consul.list_services("minigrid-worker") if s["ID"] == "hw-1"][0]
    assert "virt:process-vm" in entry["Tags"]
    assert any(t.startswith("arch:") for t in entry["Tags"])
    assert any(t.startswith("os:") for t in entry["Tags"]), (
        "heterogeneidade de SO deve aparecer no registro"
    )


def test_host_worker_waits_for_the_queue_instead_of_crashing(consul):
    """Item G: o sistema funciona antes dele, e ele espera em vez de morrer."""
    env = dict(os.environ)
    env.update({
        "MINIGRID_WORKER_ID": "hw-orfao",
        "MINIGRID_CONSUL": CONSUL_URL,
        "MINIGRID_STOMP_HOST": STOMP_HOST,
        "MINIGRID_STOMP_PORT": STOMP_PORT,
        "MINIGRID_QUEUE": "fila.inexistente." + uuid.uuid4().hex[:8],
        "PYTHONPATH": str(PROJECT_ROOT),
    })
    proc = subprocess.Popen([sys.executable, "-u", "-m", "host_worker.worker"],
                            cwd=str(PROJECT_ROOT), env=env,
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                            text=True)
    try:
        time.sleep(6)
        assert proc.poll() is None, "o worker do host morreu em vez de esperar"
    finally:
        proc.terminate()
        proc.wait(timeout=20)
        try:
            consul.deregister_service("hw-orfao")
        except Exception:
            pass


# ------------------------------------------------------------------ execucao

def test_host_worker_executes_a_task_end_to_end(topo):
    topo.start_worker("hw-1")
    task_id = topo.publish_task(FAST)
    assert wait_until(lambda: any(r["task_id"] == task_id for r in _results(topo)),
                      timeout=60), "a tarefa nao foi concluida pelo worker do host"
    result = [r for r in _results(topo) if r["task_id"] == task_id][0]
    assert base64.b64decode(result["result_b64"]) == b"ok"
    assert result["meta"]["virt"] == "process-vm"
    assert result["meta"]["worker_id"] == "hw-1"


def test_host_worker_consumes_the_same_durable_queue_as_amqp_workers(topo):
    """A mesma fila, dois protocolos — a linha extra na tabela de fluxos."""
    topo.start_worker("hw-1")
    ids = [topo.publish_task(FAST) for _ in range(3)]
    assert wait_until(
        lambda: len([r for r in _results(topo) if r["task_id"] in ids]) == 3,
        timeout=90)


def test_host_worker_publishes_events_to_the_same_exchange(topo):
    topo.start_worker("hw-1")
    task_id = topo.publish_task(FAST)
    vistos = []

    def procura():
        for kind, payload in topo.events_seen(timeout=2.0):
            if payload.get("task_id") == task_id:
                vistos.append(kind)
        return "done" in vistos

    assert wait_until(procura, timeout=60), vistos


# ------------------------------------------------------------------ migracao

def test_host_worker_requeues_on_sigterm(topo, consul):
    proc = topo.start_worker("hw-1")
    task_id = topo.publish_task(PI_SLOW, params={"steps": "60", "sleep": "0.3"})
    key = topo.kv_prefix + "tasks/" + task_id + "/checkpoint"
    assert wait_until(lambda: consul.kv_get(key) is not None, timeout=60)

    proc.terminate()
    saida = proc.communicate(timeout=40)[0]
    assert "encerrado" in saida, saida[-600:]

    from conftest import queue_depth
    assert wait_until(lambda: queue_depth(topo.tasks) == 1, timeout=25)


def test_task_migrates_from_the_host_worker_to_a_container_worker(topo, consul):
    """Migracao entre ambientes heterogeneos, que e o caso difícil do item E.

    O checkpoint e gravado pela VM de processo no macOS/Linux do host e retomado
    por um worker em conteiner Linux — funciona porque o que migra e neutro de
    plataforma: codigo e texto, checkpoint e JSON, dados sao bytes.
    """
    host = topo.start_worker("hw-1")
    task_id = topo.publish_task(PI_SLOW, params={"steps": "12", "sleep": "0.3"})
    key = topo.kv_prefix + "tasks/" + task_id + "/checkpoint"
    assert wait_until(lambda: consul.kv_get(key) is not None, timeout=60)

    host.terminate()
    host.wait(timeout=40)
    passo_na_queda = json.loads(consul.kv_get(key).decode())["n"]

    # Agora um worker em conteiner (AMQP) retoma a mesma tarefa.
    Topology.start_worker(topo, "wt-destino")
    assert wait_until(lambda: any(r["task_id"] == task_id for r in _results(topo)),
                      timeout=120), "o worker de destino nao concluiu a tarefa"

    result = [r for r in _results(topo) if r["task_id"] == task_id][0]
    assert result["meta"]["worker_id"] == "wt-destino"
    assert result["meta"]["virt"] == "container"
    assert int(result["meta"]["resumed_from_step"]) >= passo_na_queda
    assert int(result["meta"]["steps"]) == 12
