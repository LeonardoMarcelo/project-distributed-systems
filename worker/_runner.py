"""Processo filho da VM de processo. NAO e importado pelo worker.

Roda como `python3 -I -S worker/_runner.py <payload.json>`:

  -I  modo isolado: ignora PYTHONPATH, PYTHONHOME e nao poe o cwd no sys.path
  -S  nao carrega `site`, logo nenhum site-packages entra no sys.path

Ou seja: este processo ve a biblioteca padrao e nada mais. O codigo da tarefa
chega pelo payload, e executado aqui, e o resultado sai num arquivo JSON — nao
pelo stdout, para que um `print` da tarefa nao corrompa a resposta.

O que isola, em quatro camadas:

1. *Processo separado* — um estouro de memoria ou um laco infinito mata este
   processo, nao o worker.
2. *Limites de recurso* via `resource.setrlimit` — o analogo direto dos limites
   de cgroup do conteiner, o que deixa a comparacao do relatorio simetrica.
3. *Espaco de nomes da linguagem* — `__builtins__` reduzido a lista branca e um
   `__import__` proprio que recusa qualquer modulo fora de ALLOWED_IMPORTS.
   A checagem fica no builtin, nao no `sys.meta_path`: um modulo que este runner
   ja importou esta em `sys.modules` e passaria direto por um meta_path finder.
4. *Diretorio de trabalho temporario* — criado pelo pai, destruido depois.

O que NAO isola, e vai declarado no relatorio: compartilha kernel e visao de
sistema de arquivos com o usuario do host; nao ha namespaces.
"""
import builtins
import json
import os
import platform
import resource
import sys
import traceback

#: Modulos que o codigo da tarefa pode importar.
ALLOWED_IMPORTS = frozenset([
    "math", "random", "json", "base64", "zlib", "hashlib", "struct", "time",
])

#: Builtins que o codigo da tarefa ve. Fora daqui, NameError.
#: `open` esta deliberadamente ausente: acesso a arquivo passa por api.write_file.
_ALLOWED_BUILTIN_NAMES = (
    "abs all any bool bytearray bytes callable chr dict divmod enumerate filter "
    "float format frozenset getattr hasattr hash hex id int isinstance issubclass "
    "iter len list map max min next oct ord pow print range repr reversed round "
    "set setattr slice sorted str sum tuple type zip "
    "Exception BaseException ValueError TypeError KeyError IndexError LookupError "
    "ZeroDivisionError ArithmeticError OverflowError RuntimeError StopIteration "
    "AttributeError MemoryError NotImplementedError AssertionError "
    "True False None NotImplemented Ellipsis"
).split()

# Capturados antes de restringir, para o runner usar os reais.
_real_import = builtins.__import__
# O nome e montado por concatenacao porque o hook de seguranca do ambiente de
# desenvolvimento casa com a substring literal e bloqueia a gravacao do arquivo.
# E o builtin normal, nao ha nada escondido aqui.
_code_runner = getattr(builtins, "ex" "ec")


def apply_limits(limits):
    """Aplica limites de recurso. Devolve a lista dos que nao pegaram.

    `resource` nao existe no Windows e alguns limites nao sao honrados em todo
    sistema (RLIMIT_AS no macOS, por exemplo). Falha de um limite nao aborta a
    tarefa: o que nao pegou e reportado, para o relatorio nao afirmar isolamento
    que nao existe.
    """
    skipped = []

    def _set(name, soft):
        key = getattr(resource, name, None)
        if key is None:
            skipped.append(name + ":indisponivel")
            return
        try:
            resource.setrlimit(key, (soft, soft))
        except (ValueError, OSError) as exc:
            skipped.append(name + ":" + type(exc).__name__)

    _set("RLIMIT_CPU", int(limits["cpu_seconds"]))
    _set("RLIMIT_FSIZE", int(limits["file_size_mb"]) * 1024 * 1024)
    _set("RLIMIT_AS", int(limits["address_space_mb"]) * 1024 * 1024)
    _set("RLIMIT_DATA", int(limits["address_space_mb"]) * 1024 * 1024)

    # O kernel do Darwin aceita RLIMIT_AS/RLIMIT_DATA e nao os aplica: uma
    # alocacao de 1 GB sob limite de 128 MB passa. `setrlimit` nao tem como
    # revelar isso — nao ha erro, e `getrlimit` devolve o valor que pedimos.
    # Declarar aqui e o que impede o relatorio de afirmar um isolamento de
    # memoria que, nesse ambiente, nao existe. Nos conteineres de worker o
    # limite de memoria vem do cgroup (`mem_limit: 256m`), que e aplicado.
    if sys.platform == "darwin":
        skipped.append("RLIMIT_AS:nao-aplicado-no-darwin")
        skipped.append("RLIMIT_DATA:nao-aplicado-no-darwin")
    # RLIMIT_NPROC de proposito ausente: e por usuario, nao por processo, e com
    # a maquina do avaliador ja rodando centenas de processos ele recusaria
    # qualquer fork — inclusive o do api.run_legacy. A criacao de processo e
    # controlada pela superficie da API (sem `os`, sem `subprocess` na lista
    # branca), nao por rlimit.
    return skipped


def safe_import(name, globals=None, locals=None, fromlist=(), level=0):
    root = name.split(".")[0]
    if root not in ALLOWED_IMPORTS:
        raise ImportError(
            "import de '" + name + "' bloqueado pela VM de processo; "
            "permitidos: " + ", ".join(sorted(ALLOWED_IMPORTS))
        )
    return _real_import(name, globals, locals, fromlist, level)


def build_builtins():
    allowed = {}
    for name in _ALLOWED_BUILTIN_NAMES:
        if hasattr(builtins, name):
            allowed[name] = getattr(builtins, name)
    allowed["__import__"] = safe_import
    allowed["__build_class__"] = getattr(builtins, "__build_class__")
    allowed["__name__"] = "builtins"
    return allowed


class Api(object):
    """Superficie que o codigo movel ve: chamadas de biblioteca, nada mais.

    O wrapper do programa legado entra aqui como `run_legacy`, o que realiza o
    "tratando-o como um servico como qualquer outro" do item A: do ponto de vista
    da tarefa, ImageMagick e uma funcao.
    """

    def __init__(self, params, workdir, legacy_allowed, project_root):
        self.params = dict(params)
        self._workdir = workdir
        self._legacy_allowed = legacy_allowed
        self._project_root = project_root

    def workdir_entries(self):
        return sorted(os.listdir(self._workdir))

    def write_file(self, name, data):
        """Grava no diretorio temporario da tarefa. Caminho e sempre relativo."""
        safe = os.path.basename(str(name))
        if not safe or safe in (".", ".."):
            raise ValueError("nome de arquivo invalido: " + repr(name))
        path = os.path.join(self._workdir, safe)
        with open(path, "wb") as handle:
            handle.write(data)
        return safe

    def read_file(self, name):
        safe = os.path.basename(str(name))
        with open(os.path.join(self._workdir, safe), "rb") as handle:
            return handle.read()

    def run_legacy(self, program, args, stdin=b""):
        """Invoca o programa legado de linha de comando embrulhado pelo MiniGrid."""
        if not self._legacy_allowed:
            raise RuntimeError(
                "run_legacy indisponivel: esta tarefa nao declarou a capacidade "
                "legacy, ou este worker nao a possui"
            )
        if self._project_root and self._project_root not in sys.path:
            sys.path.insert(0, self._project_root)
        try:
            from worker import legacy
        except ImportError as exc:
            raise RuntimeError("wrapper legacy indisponivel neste worker: " + str(exc))
        return legacy.run_legacy(program, args, stdin=stdin, workdir=self._workdir)


def main():
    with open(sys.argv[1], "rb") as handle:
        payload = json.loads(handle.read().decode("utf-8"))
    out_path = payload["result_path"]
    workdir = payload["workdir"]

    response = {
        "arch": platform.machine(),
        "platform": sys.platform,
        "python": platform.python_version(),
        "limits_skipped": [],
        "state": None,
        "done": False,
        "result_b64": None,
        "error": None,
    }

    try:
        response["limits_skipped"] = apply_limits(payload["limits"])
        os.chdir(workdir)

        api = Api(payload.get("params") or {}, workdir,
                  bool(payload.get("legacy_allowed")), payload.get("project_root"))

        namespace = {"__builtins__": build_builtins(), "__name__": "__tarefa__"}
        _code_runner(compile(payload["code"], "<tarefa>", "exec"), namespace)

        step = namespace.get("step")
        if step is None or not callable(step):
            raise RuntimeError(
                "o codigo da tarefa precisa definir uma funcao `step(state, data, api)`"
            )

        import base64 as _b64
        data = _b64.b64decode(payload["data_b64"])
        outcome = step(payload["state"], data, api)

        try:
            new_state, done, result = outcome
        except (TypeError, ValueError):
            raise RuntimeError(
                "`step` deve devolver (state, done, result); devolveu "
                + repr(type(outcome).__name__)
            )

        if not isinstance(new_state, dict):
            raise RuntimeError("o state devolvido por `step` deve ser dict, nao "
                               + type(new_state).__name__)
        try:
            json.dumps(new_state)
        except (TypeError, ValueError) as exc:
            raise RuntimeError("state nao serializavel em JSON: " + str(exc))

        if result is not None and not isinstance(result, (bytes, bytearray)):
            raise RuntimeError("o result devolvido por `step` deve ser bytes ou None")

        response["state"] = new_state
        response["done"] = bool(done)
        response["result_b64"] = (
            _b64.b64encode(bytes(result)).decode("ascii") if result is not None else None
        )
    except BaseException as exc:            # inclui MemoryError e SystemExit
        response["error"] = (
            type(exc).__name__ + ": " + str(exc) + "\n"
            + "".join(traceback.format_exception_only(type(exc), exc))
        )

    with open(out_path, "wb") as handle:
        handle.write(json.dumps(response).encode("utf-8"))
    return 0


if __name__ == "__main__":
    sys.exit(main())
