"""Garante que os modulos do worker do host nao dependem de pacote de terceiro.

Review Focus 1 do plano: se um desses modulos puxar `pika` ou `grpc` por acidente,
o worker do host quebra na maquina do avaliador, que nao instalou nada.

O ambiente e reproduzido com `-S -E`: `-S` nao carrega o modulo `site`, logo
nenhum site-packages (nem o do venv) entra no sys.path; `-E` ignora PYTHONPATH,
para que uma variavel de ambiente solta nao contrabandeie um pacote de terceiro.
O diretorio do projeto continua importavel, que e o caso real — o worker do host
sobe como `python3 -m host_worker.worker` a partir da raiz.

`-I` seria errado aqui: ele tambem remove o diretorio corrente do sys.path, e o
teste passaria a falhar por nao achar `minigrid`, nao por dependencia externa.
"""
import os
import subprocess
import sys

from conftest import PROJECT_ROOT

# Crescem conforme as tarefas: envelope/naming (T2), stomp (T3), sandbox (T4),
# executor (T5), legacy (T6), registry (T10), host_worker (T11).
STDLIB_ONLY = [
    "minigrid.envelope",
    "minigrid.naming",
    "minigrid.stomp",
    "worker.sandbox",
]


def _import_isolated(modules):
    env = {k: v for k, v in os.environ.items() if k != "PYTHONPATH"}
    return subprocess.run(
        [sys.executable, "-S", "-E", "-c", "import " + ",".join(modules)],
        cwd=str(PROJECT_ROOT),
        capture_output=True,
        text=True,
        env=env,
    )


def test_stdlib_only_modules_import_without_site_packages():
    r = _import_isolated(STDLIB_ONLY)
    assert r.returncode == 0, r.stderr


def test_the_guard_itself_detects_a_third_party_import():
    """Sem este teste, o de cima passaria mesmo com o guarda quebrado."""
    r = _import_isolated(["pika"])
    assert r.returncode != 0, "esperado que `pika` nao importe sem site-packages"
