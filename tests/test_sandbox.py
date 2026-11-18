"""Testes da VM de processo (item D, segundo tipo de virtualizacao).

O enunciado permite "um interpretador isolado que execute o codigo enviado". A
critica obvia e "isto e interpretador isolado ou so executar o codigo no proprio
processo?" — estes testes sao a resposta: o isolamento tem que ser demonstravel.
"""
from worker import sandbox

L = sandbox.Limits()

PI_STEP = (
    "def step(state, data, api):\n"
    "    n = state.get('n', 0) + 1\n"
    "    return {'n': n}, n >= 3, (b'done' if n >= 3 else None)\n"
)


def test_runs_one_step_and_returns_new_state():
    r = sandbox.run_step(PI_STEP, {}, b"", L)
    assert r.error is None
    assert r.state == {"n": 1}
    assert r.done is False


def test_signals_completion_and_result():
    r = sandbox.run_step(PI_STEP, {"n": 2}, b"", L)
    assert r.error is None
    assert r.done is True
    assert r.result == b"done"


def test_input_data_reaches_the_step():
    code = "def step(state, data, api): return {'n': len(data)}, True, data[::-1]\n"
    r = sandbox.run_step(code, {}, b"abc", L)
    assert r.error is None and r.state == {"n": 3} and r.result == b"cba"


def test_params_reach_the_step_through_the_api():
    code = "def step(state, data, api): return {}, True, api.params['k'].encode()\n"
    r = sandbox.run_step(code, {}, b"", L, params={"k": "v"})
    assert r.error is None and r.result == b"v"


# ------------------------------------------------------------- isolamento

def test_blocks_import_outside_the_allowlist():
    code = "import os\ndef step(state, data, api): return {}, True, b''\n"
    r = sandbox.run_step(code, {}, b"", L)
    assert r.error is not None and "os" in r.error


def test_blocks_import_inside_the_step_body_too():
    code = "def step(state, data, api):\n    import socket\n    return {}, True, b''\n"
    r = sandbox.run_step(code, {}, b"", L)
    assert r.error is not None and "socket" in r.error


def test_allows_import_inside_the_allowlist():
    code = ("import math\n"
            "def step(state, data, api): return {'v': math.floor(1.5)}, True, b''\n")
    r = sandbox.run_step(code, {}, b"", L)
    assert r.error is None and r.state == {"v": 1}


def test_builtins_are_reduced_to_an_allowlist():
    code = "def step(state, data, api): return {}, True, str(open).encode()\n"
    r = sandbox.run_step(code, {}, b"", L)
    assert r.error is not None and "open" in r.error


def test_cpu_limit_terminates_an_infinite_loop():
    code = "def step(state, data, api):\n    while True:\n        pass\n"
    r = sandbox.run_step(code, {}, b"", sandbox.Limits(cpu_seconds=1, wall_seconds=10))
    assert r.error is not None


def test_memory_limit_is_either_enforced_or_declared_as_not_enforced():
    """O limite de memoria pega, ou o sandbox diz em voz alta que nao pegou.

    No Linux — conteineres e runner do CI, que e onde a demonstracao corrigida
    roda — RLIMIT_AS mata a alocacao. O kernel do Darwin aceita o setrlimit e o
    ignora, e nao ha como descobrir isso pela propria chamada. O que nao pode
    acontecer e o sistema passar por isolado sem ser: por isso a alternativa
    nao e "passa calado", e "aparece em limits_skipped".
    """
    code = ("def step(state, data, api):\n"
            "    b = bytearray(10 ** 9)\n"
            "    return {'n': len(b)}, True, b''\n")
    r = sandbox.run_step(code, {}, b"", sandbox.Limits(address_space_mb=128))
    if r.error is None:
        assert any("RLIMIT_AS" in entry for entry in r.limits_skipped), (
            "alocacao de 1 GB passou sob limite de 128 MB e o sandbox nao "
            "declarou que o limite nao foi aplicado: " + repr(r.limits_skipped)
        )


def test_file_size_limit_is_enforced():
    """RLIMIT_FSIZE e aplicado em todo SO suportado, inclusive Darwin."""
    code = ("def step(state, data, api):\n"
            "    api.write_file('grande.bin', b'x' * (3 * 1024 * 1024))\n"
            "    return {}, True, b''\n")
    r = sandbox.run_step(code, {}, b"", sandbox.Limits(file_size_mb=1))
    assert r.error is not None


def test_each_step_gets_a_fresh_working_directory():
    """Recurso local nao vaza entre passos: o cwd e temporario e descartado."""
    code = ("def step(state, data, api):\n"
            "    seen = api.workdir_entries()\n"
            "    api.write_file('marca.txt', b'x')\n"
            "    return {'antes': seen}, True, b''\n")
    first = sandbox.run_step(code, {}, b"", L)
    second = sandbox.run_step(code, {}, b"", L)
    assert first.error is None and second.error is None
    assert first.state["antes"] == [] and second.state["antes"] == []


# ------------------------------------------------- erro de tarefa, nao do worker

def test_code_without_step_fails_as_task_error():
    r = sandbox.run_step("x = 1\n", {}, b"", L)
    assert r.error is not None and "step" in r.error


def test_exception_inside_step_fails_as_task_error():
    code = "def step(state, data, api): raise ValueError('boom')\n"
    r = sandbox.run_step(code, {}, b"", L)
    assert r.error is not None and "boom" in r.error


def test_syntax_error_fails_as_task_error():
    r = sandbox.run_step("def step(:\n", {}, b"", L)
    assert r.error is not None


def test_step_returning_garbage_fails_as_task_error():
    code = "def step(state, data, api): return 'nao e tupla'\n"
    r = sandbox.run_step(code, {}, b"", L)
    assert r.error is not None


def test_non_json_serializable_state_fails_as_task_error():
    code = "def step(state, data, api): return {'s': set([1])}, True, b''\n"
    r = sandbox.run_step(code, {}, b"", L)
    assert r.error is not None


def test_wall_clock_timeout_does_not_hang_the_caller():
    code = ("def step(state, data, api):\n"
            "    import time\n"
            "    time.sleep(30)\n")
    r = sandbox.run_step(code, {}, b"", sandbox.Limits(wall_seconds=2))
    assert r.error is not None


def test_run_legacy_is_absent_unless_a_runner_is_allowed():
    code = "def step(state, data, api): return {}, True, api.run_legacy('x', [])\n"
    r = sandbox.run_step(code, {}, b"", L, legacy_allowed=False)
    assert r.error is not None and "legacy" in r.error.lower()


def test_reports_the_architecture_it_ran_on():
    """Heterogeneidade (item D): o resultado diz em que ISA executou."""
    r = sandbox.run_step(PI_STEP, {}, b"", L)
    assert r.arch and r.arch == __import__("platform").machine()
