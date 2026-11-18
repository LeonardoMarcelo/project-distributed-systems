import base64
import json
import os
import pathlib
import sys
import time
import urllib.error
import urllib.request

import pytest

PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

CONSUL_URL = os.environ.get("MINIGRID_CONSUL", "http://localhost:8500")
RABBIT_MGMT = os.environ.get("MINIGRID_RABBIT_MGMT", "http://localhost:15672/api")
RABBIT_AUTH = base64.b64encode(b"guest:guest").decode()
STOMP_HOST = os.environ.get("MINIGRID_STOMP_HOST", "localhost")
STOMP_PORT = int(os.environ.get("MINIGRID_STOMP_PORT", "61613"))

TEST_KV_PREFIX = "minigrid/test/"


def mgmt(method, path, body=None, expect_json=True):
    """Chama a API HTTP de gerenciamento do RabbitMQ."""
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(RABBIT_MGMT + path, data=data, method=method)
    req.add_header("Authorization", "Basic " + RABBIT_AUTH)
    req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=15) as r:
        raw = r.read()
    if expect_json and raw:
        return json.loads(raw)
    return None


def queue_depth(name):
    """Mensagens prontas na fila, pela API de gerenciamento."""
    try:
        info = mgmt("GET", "/queues/%2F/" + name)
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return 0
        raise
    return info.get("messages_ready", 0)


def wait_until(predicate, timeout=10.0, interval=0.2):
    """Espera o predicado virar verdadeiro. Devolve o valor final (falsy no estouro)."""
    deadline = time.monotonic() + timeout
    result = predicate()
    while not result and time.monotonic() < deadline:
        time.sleep(interval)
        result = predicate()
    return result


def _infra_available():
    try:
        with urllib.request.urlopen(CONSUL_URL + "/v1/status/leader", timeout=3) as r:
            r.read()
        return True
    except Exception:
        return False


needs_infra = pytest.mark.skipif(
    not _infra_available(),
    reason="infra do compose indisponivel: rode `docker compose up -d` antes",
)


@pytest.fixture
def consul():
    """Cliente Consul apontando para a infra do compose, com limpeza do prefixo de teste."""
    from minigrid import naming

    client = naming.Consul(CONSUL_URL)
    sessions_before = set()
    try:
        sessions_before = {s["ID"] for s in client.session_list()}
    except Exception:
        pass

    yield client

    # Limpeza: chaves de teste, serviços de teste e sessões criadas no teste.
    try:
        for key in client.kv_keys(TEST_KV_PREFIX):
            client.kv_delete(key)
    except Exception:
        pass
    try:
        for svc in client.list_services("minigrid-worker"):
            if svc["ID"].startswith(("w-test", "w1", "w2", "ghost")):
                client.deregister_service(svc["ID"])
    except Exception:
        pass
    try:
        for s in client.session_list():
            if s["ID"] not in sessions_before:
                client.session_destroy(s["ID"])
    except Exception:
        pass
