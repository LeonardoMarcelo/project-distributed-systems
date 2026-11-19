"""Testes do executor: laco de passos, checkpoint e retomada (item E).

O executor e o que transforma "codigo + dados" em execucao retomavel. Ele nao
sabe de AMQP, de STOMP nem de Consul: recebe um `Transport` injetado, que e o
que permite o mesmo executor rodar no worker em conteiner e no worker do host.
"""
import json
import platform
import zlib

import pytest

from worker import executor, sandbox

L = sandbox.Limits(cpu_seconds=10, wall_seconds=30)

PI_STEP = (
    "def step(state, data, api):\n"
    "    total = int(api.params.get('steps', '3'))\n"
    "    n = state.get('n', 0) + 1\n"
    "    done = n >= total\n"
    "    return {'n': n}, done, (b'done' if done else None)\n"
)


class FakeTransport(object):
    """Transport em memoria. Nada de rede: o executor e testavel sozinho."""

    def __init__(self):
        self.events = []        # [(kind, payload)]
        self.checkpoints = {}   # task_id -> state
        self.results = []       # [(result_bytes, meta)]
        self.failures = []      # [(task_id, error, meta)]
        self.cleared = []

    def publish_event(self, kind, payload):
        self.events.append((kind, payload))

    def save_checkpoint(self, task_id, state):
        self.checkpoints[task_id] = state

    def load_checkpoint(self, task_id):
        return self.checkpoints.get(task_id)

    def clear_checkpoint(self, task_id):
        self.cleared.append(task_id)
        self.checkpoints.pop(task_id, None)

    def send_result(self, task_id, result, meta):
        self.results.append((result, meta))

    def report_failure(self, task_id, error, meta):
        self.failures.append((task_id, error, meta))

    def kinds(self):
        return [kind for kind, _ in self.events]

    def last(self, kind):
        for k, payload in reversed(self.events):
            if k == kind:
                return payload
        return None


@pytest.fixture
def fake():
    return FakeTransport()


def env(code, task_id="t1", data=b"", params=None, recipe="pi", requires=None):
    return {
        "v": 1,
        "task_id": task_id,
        "recipe": recipe,
        "code": code,
        "data": data,
        "params": params or {},
        "requires": requires or [],
        "submitted_at": 1.0,
    }


def after_n_calls(n):
    """Predicado que pede parada na n-esima consulta.

    O executor consulta `should_stop()` uma vez por passo concluido, entao
    `after_n_calls(2)` faz a tarefa ceder com exatamente 2 passos feitos.
    """
    box = {"calls": 0}

    def predicate():
        box["calls"] += 1
        return box["calls"] >= n

    return predicate


# ------------------------------------------------------------ caminho felizes

def test_runs_to_completion_and_sends_result(fake):
    assert executor.run_task(env(PI_STEP, params={"steps": "3"}), fake, "w1", L) == "done"
    assert fake.results[0][0] == b"done"
    assert fake.kinds() == ["running", "checkpoint", "checkpoint", "checkpoint", "done"]


def test_resumes_from_the_saved_checkpoint(fake):
    fake.checkpoints["t1"] = {"n": 2}
    executor.run_task(env(PI_STEP, params={"steps": "3"}), fake, "w1", L)
    assert fake.events[0][1]["resumed_from_step"] == 2
    assert fake.results[0][1]["steps"] == 3


def test_fresh_task_reports_resumed_from_step_zero(fake):
    executor.run_task(env(PI_STEP, params={"steps": "1"}), fake, "w1", L)
    assert fake.events[0][1]["resumed_from_step"] == 0


def test_checkpoint_written_every_n_steps(fake):
    executor.run_task(env(PI_STEP, params={"steps": "4"}), fake, "w1", L,
                      checkpoint_every=2)
    assert len([k for k in fake.kinds() if k == "checkpoint"]) == 2


def test_completion_clears_the_checkpoint(fake):
    """Entrega e ao-menos-uma-vez: um checkpoint de tarefa concluida que
    sobrevivesse poderia fazer uma reentrega reexecutar o que ja acabou."""
    executor.run_task(env(PI_STEP, params={"steps": "2"}), fake, "w1", L)
    assert fake.cleared == ["t1"] and "t1" not in fake.checkpoints


def test_worker_id_and_arch_travel_in_the_result_meta(fake):
    executor.run_task(env(PI_STEP, params={"steps": "2"}), fake, "w1", L)
    meta = fake.results[0][1]
    assert meta["worker_id"] == "w1"
    assert meta["arch"] == platform.machine()
    assert meta["steps"] == 2


# ----------------------------------------------------------------- migracao

def test_should_stop_yields_and_keeps_the_checkpoint(fake):
    assert executor.run_task(env(PI_STEP, params={"steps": "10"}), fake, "w1", L,
                             should_stop=after_n_calls(2)) == "yielded"
    assert fake.checkpoints["t1"]["n"] == 2
    assert fake.kinds()[-1] == "migrated"
    assert fake.results == []


def test_yield_forces_a_checkpoint_even_off_the_interval(fake):
    """Senao a migracao perde o progresso feito desde o ultimo intervalo."""
    executor.run_task(env(PI_STEP, params={"steps": "10"}), fake, "w1", L,
                      checkpoint_every=100, should_stop=after_n_calls(3))
    assert fake.checkpoints["t1"]["n"] == 3


def test_migrated_event_carries_the_reason(fake):
    executor.run_task(env(PI_STEP, params={"steps": "10"}), fake, "w1", L,
                      should_stop=after_n_calls(1), stop_reason="overload")
    assert fake.last("migrated")["reason"] == "overload"


def test_stop_reason_can_be_resolved_lazily(fake):
    """O motivo so e conhecido quando `should_stop` decide parar.

    O worker decide "sobrecarga" dentro do proprio `should_stop`, depois de
    `run_task` ja ter sido chamada. Passar o motivo como valor fixo congelava o
    padrao "shutdown" e o evento `migrated` mentia sobre a causa da migracao —
    justamente o dado que o item E pede para explicar.
    """
    motivo = {"valor": "shutdown"}

    def should_stop():
        motivo["valor"] = "overload"
        return True

    executor.run_task(env(PI_STEP, params={"steps": "10"}), fake, "w1", L,
                      should_stop=should_stop,
                      stop_reason=lambda: motivo["valor"])
    assert fake.last("migrated")["reason"] == "overload"


def test_resumed_task_continues_instead_of_restarting(fake):
    fake.checkpoints["t1"] = {"n": 8}
    executor.run_task(env(PI_STEP, params={"steps": "10"}), fake, "w1", L)
    assert fake.results[0][1]["steps"] == 10
    assert len([k for k in fake.kinds() if k == "checkpoint"]) == 2


# ------------------------------------------------------------------- falhas

def test_step_error_marks_the_task_failed(fake):
    code = "def step(state, data, api): raise ValueError('boom')\n"
    assert executor.run_task(env(code), fake, "w1", L) == "failed"
    assert fake.kinds()[-1] == "failed"
    assert "boom" in fake.last("failed")["error"]


def test_oversized_checkpoint_fails_the_task_readably(fake):
    """Review Focus 3: acima do limite de valor do Consul KV (512 KB)."""
    code = ("def step(state, data, api):\n"
            "    return {'blob': 'x' * 600000}, False, None\n")
    assert executor.run_task(env(code), fake, "w1", L) == "failed"
    assert "checkpoint" in fake.last("failed")["error"].lower()


def test_oversized_checkpoint_does_not_leave_a_partial_checkpoint(fake):
    code = ("def step(state, data, api):\n"
            "    return {'blob': 'x' * 600000}, False, None\n")
    executor.run_task(env(code), fake, "w1", L)
    assert "t1" not in fake.checkpoints


def test_missing_step_function_fails_the_task(fake):
    assert executor.run_task(env("x = 1\n"), fake, "w1", L) == "failed"
    assert fake.kinds()[-1] == "failed"


def test_step_limit_guards_against_a_recipe_that_never_finishes(fake):
    code = "def step(state, data, api): return {'n': state.get('n', 0) + 1}, False, None\n"
    assert executor.run_task(env(code), fake, "w1", L, max_steps=5) == "failed"
    assert "passos" in fake.last("failed")["error"].lower()


# -------------------------------------------------------------- integracao

def test_compress_recipe_style_task_roundtrips(fake):
    code = ("import zlib\n"
            "def step(state, data, api):\n"
            "    return {'n': len(data)}, True, zlib.compress(data)\n")
    executor.run_task(env(code, data=b"y" * 5000, recipe="compress"), fake, "w1", L)
    assert zlib.decompress(fake.results[0][0]) == b"y" * 5000


def test_events_carry_task_id_and_worker_identity(fake):
    executor.run_task(env(PI_STEP, params={"steps": "2"}), fake, "w1", L)
    for _, payload in fake.events:
        assert payload["task_id"] == "t1"
        assert payload["worker_id"] == "w1"
        assert payload["arch"] == platform.machine()


def test_result_meta_is_json_serializable(fake):
    executor.run_task(env(PI_STEP, params={"steps": "1"}), fake, "w1", L)
    json.dumps(fake.results[0][1])


# ------------------------------------------------- falha chega ao caminho duravel

def test_step_error_is_also_reported_durably(fake):
    """Evento `failed` nao basta: ele e transiente por desenho.

    Sem o relato duravel, o estado autoritativo da tarefa ficava `received`
    para sempre — `GetStatus` mentia e `watch()` nunca retornava.
    """
    code = "def step(state, data, api): raise ValueError('boom')\n"
    executor.run_task(env(code), fake, "w1", L)
    assert len(fake.failures) == 1
    task_id, error, meta = fake.failures[0]
    assert task_id == "t1" and "boom" in error
    assert meta["worker_id"] == "w1" and meta["arch"] == platform.machine()


def test_oversized_checkpoint_failure_is_reported_durably(fake):
    code = "def step(state, data, api): return {'blob': 'x' * 600000}, False, None\n"
    executor.run_task(env(code), fake, "w1", L)
    assert len(fake.failures) == 1
    assert "checkpoint" in fake.failures[0][1].lower()


def test_step_limit_failure_is_reported_durably(fake):
    code = "def step(state, data, api): return {'n': state.get('n', 0) + 1}, False, None\n"
    executor.run_task(env(code), fake, "w1", L, max_steps=3)
    assert len(fake.failures) == 1
    assert "passos" in fake.failures[0][1].lower()


def test_success_reports_no_failure(fake):
    executor.run_task(env(PI_STEP, params={"steps": "2"}), fake, "w1", L)
    assert fake.failures == []


def test_a_broken_failure_report_does_not_mask_the_task_failure(fake):
    """Se o relato duravel falhar, a tarefa ainda tem de voltar como `failed`."""
    def explode(task_id, error, meta):
        raise RuntimeError("fila de resultados fora do ar")
    fake.report_failure = explode
    code = "def step(state, data, api): raise ValueError('boom')\n"
    assert executor.run_task(env(code), fake, "w1", L) == "failed"
