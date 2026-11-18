"""VM de processo: um interpretador isolado que executa o codigo enviado.

E o segundo tipo de virtualizacao do item D e o ambiente do worker do host
(item G). O enunciado o permite explicitamente: "uma maquina virtual de processo
(como a JVM ou um interpretador isolado que execute o codigo enviado)".

Qual interface este ambiente imita, para o relatorio: a de *biblioteca e
linguagem*. O que o codigo da tarefa ve e a maquina virtual de bytecode do
CPython mais uma superficie restrita da biblioteca padrao — nao um conjunto de
instrucoes (isso e o worker emulado) nem chamadas de sistema (isso e o
conteiner). Tres niveis distintos, tres respostas distintas no relatorio.

Este modulo e o lado *pai*: monta o payload, cria e destroi o diretorio
temporario, dispara o filho com timeout e traduz tudo — inclusive morte por
sinal — em `StepResult`. O lado filho esta em `worker/_runner.py`.

Stdlib pura (ver worker/__init__.py).
"""
import base64
import json
import os
import platform
import shutil
import subprocess
import sys
import tempfile

_HERE = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(_HERE)
_RUNNER = os.path.join(_HERE, "_runner.py")


class Limits(object):
    """Limites de recurso aplicados ao passo.

    Espelham de proposito os limites de cgroup dos conteineres de worker
    (`cpus: 0.5`, `mem_limit: 256m`), para o relatorio poder comparar os dois
    ambientes na mesma unidade.
    """

    __slots__ = ("cpu_seconds", "address_space_mb", "file_size_mb", "wall_seconds")

    def __init__(self, cpu_seconds=10, address_space_mb=256, file_size_mb=16,
                 wall_seconds=60):
        self.cpu_seconds = cpu_seconds
        self.address_space_mb = address_space_mb
        self.file_size_mb = file_size_mb
        self.wall_seconds = wall_seconds

    def as_dict(self):
        return {
            "cpu_seconds": self.cpu_seconds,
            "address_space_mb": self.address_space_mb,
            "file_size_mb": self.file_size_mb,
            "wall_seconds": self.wall_seconds,
        }


class StepResult(object):
    """Resultado de um passo. `error` nao-nulo significa tarefa falhada.

    Falha da tarefa nunca e falha do worker: o laco do executor le `error` e
    publica `failed`, e o worker segue consumindo.
    """

    __slots__ = ("state", "done", "result", "error", "arch", "platform",
                 "limits_skipped")

    def __init__(self, state=None, done=False, result=None, error=None,
                 arch=None, platform_name=None, limits_skipped=None):
        self.state = state if state is not None else {}
        self.done = done
        self.result = result
        self.error = error
        self.arch = arch or platform.machine()
        self.platform = platform_name or sys.platform
        self.limits_skipped = limits_skipped or []

    def __repr__(self):
        return ("<StepResult done=" + str(self.done)
                + (" error=" + repr(self.error) if self.error else "")
                + " arch=" + str(self.arch) + ">")


def run_step(code, state, data, limits=None, legacy_allowed=False, params=None):
    """Executa um passo do codigo da tarefa na VM de processo.

    `code` e o codigo-fonte que veio no envelope — codigo movel, nao um modulo
    do projeto. `state` e o checkpoint (ou {} no primeiro passo). Devolve sempre
    um `StepResult`; nao levanta por causa do codigo da tarefa.
    """
    limits = limits or Limits()
    base = tempfile.mkdtemp(prefix="minigrid-step-")
    workdir = os.path.join(base, "work")
    os.mkdir(workdir)
    payload_path = os.path.join(base, "payload.json")
    result_path = os.path.join(base, "result.json")

    payload = {
        "code": code,
        "state": state if state is not None else {},
        "data_b64": base64.b64encode(data or b"").decode("ascii"),
        "params": dict(params or {}),
        "limits": limits.as_dict(),
        "legacy_allowed": bool(legacy_allowed),
        "workdir": workdir,
        "result_path": result_path,
        "project_root": PROJECT_ROOT if legacy_allowed else None,
    }

    try:
        with open(payload_path, "wb") as handle:
            handle.write(json.dumps(payload).encode("utf-8"))

        # -I -S: sem PYTHONPATH, sem site-packages, sem cwd no sys.path.
        # shell=False por construcao (lista de argumentos).
        completed = subprocess.run(
            [sys.executable, "-I", "-S", _RUNNER, payload_path],
            cwd=workdir,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=limits.wall_seconds,
        )
        return _read_result(result_path, completed)
    except subprocess.TimeoutExpired:
        return StepResult(
            state=state,
            error="TimeoutExpired: o passo excedeu o limite de "
                  + str(limits.wall_seconds) + "s de tempo de parede e foi morto",
        )
    except OSError as exc:
        return StepResult(state=state, error="OSError ao lancar a VM de processo: "
                                             + str(exc))
    finally:
        shutil.rmtree(base, ignore_errors=True)


def _read_result(result_path, completed):
    """Traduz a saida do filho em StepResult.

    Se o arquivo de resultado nao existe, o filho morreu antes de escrever —
    estouro de limite de CPU (SIGXCPU), de memoria, ou falha dura. O codigo de
    retorno negativo e o numero do sinal.
    """
    if not os.path.exists(result_path):
        detail = (completed.stderr or b"").decode("utf-8", "replace").strip()
        if completed.returncode and completed.returncode < 0:
            reason = "morto pelo sinal " + str(-completed.returncode)
        else:
            reason = "codigo de saida " + str(completed.returncode)
        return StepResult(
            error="a VM de processo terminou sem resultado (" + reason + ")"
                  + ((": " + detail[-800:]) if detail else "")
        )

    with open(result_path, "rb") as handle:
        raw = handle.read()
    try:
        body = json.loads(raw.decode("utf-8"))
    except (ValueError, UnicodeDecodeError) as exc:
        return StepResult(error="resultado ilegivel da VM de processo: " + str(exc))

    result = None
    if body.get("result_b64") is not None:
        result = base64.b64decode(body["result_b64"])

    return StepResult(
        state=body.get("state") or {},
        done=bool(body.get("done")),
        result=result,
        error=body.get("error"),
        arch=body.get("arch"),
        platform_name=body.get("platform"),
        limits_skipped=body.get("limits_skipped") or [],
    )
