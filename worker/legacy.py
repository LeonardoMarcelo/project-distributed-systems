"""Wrapper que integra um programa legado de linha de comando ao MiniGrid (item A).

O enunciado pede "um wrapper que integre ao MiniGrid um programa legado de linha
de comando ja existente, tratando-o como um servico como qualquer outro". O
programa escolhido e o ImageMagick: existe antes do MiniGrid, nao sabe que o
MiniGrid existe, nao fala gRPC nem AMQP, e sua unica interface e argv mais
stdin/stdout mais codigo de saida.

O wrapper faz a traducao nas duas direcoes:

  tarefa  -> argv + stdin          (o que o programa entende)
  stdout  -> resultado da tarefa
  codigo de saida != 0 -> LegacyFailed, que o executor publica como `failed`

E ele chega ao codigo movel como `api.run_legacy(...)`, ou seja, como *chamada
de biblioteca*: do ponto de vista da receita, ImageMagick e uma funcao. E esse o
sentido de "um servico como qualquer outro".

`shell=False` sempre, por lista de argumentos: um parametro vindo do cliente
nunca e interpretado como sintaxe de shell.

Stdlib pura (ver worker/__init__.py).
"""
import shutil
import subprocess


class LegacyFailed(Exception):
    """O programa legado falhou, nao existe, ou estourou o tempo."""

    def __init__(self, message, returncode=None, stderr=""):
        Exception.__init__(self, message)
        self.returncode = returncode
        self.stderr = stderr or ""


def resolve_imagemagick():
    """Invocacao do ImageMagick disponivel nesta maquina.

    No ImageMagick 6 (Debian bookworm, que e a base do conteiner legado) o
    binario e `convert`. No 7 ele foi substituido por `magick`. Resolver em
    tempo de execucao evita fixar a imagem base numa distribuicao especifica.
    """
    if shutil.which("convert"):
        return ["convert"]
    if shutil.which("magick"):
        return ["magick"]
    raise LegacyFailed(
        "ImageMagick nao encontrado neste worker: nem `convert` (v6) nem "
        "`magick` (v7) estao no PATH"
    )


#: Nomes logicos de servico legado -> como invoca-los.
#: A receita pede "imagemagick"; quem sabe qual binario existe e o wrapper.
LEGACY_PROGRAMS = {
    "imagemagick": resolve_imagemagick,
}


def run_legacy(program, args, stdin=b"", workdir=None, timeout=60):
    """Invoca o programa legado e devolve o stdout em bytes.

    `program` pode ser um nome logico de LEGACY_PROGRAMS ou um binario direto.
    """
    resolver = LEGACY_PROGRAMS.get(program)
    argv = (resolver() if resolver else [program]) + [str(a) for a in args]

    try:
        completed = subprocess.run(
            argv,
            input=stdin or b"",
            cwd=workdir,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout,
            shell=False,
        )
    except FileNotFoundError:
        raise LegacyFailed("programa legado nao encontrado: " + str(program))
    except subprocess.TimeoutExpired:
        raise LegacyFailed(
            "programa legado " + str(program) + " excedeu " + str(timeout) + "s",
            returncode=None,
        )
    except OSError as exc:
        raise LegacyFailed("falha ao invocar " + str(program) + ": " + str(exc))

    if completed.returncode != 0:
        detail = (completed.stderr or b"").decode("utf-8", "replace").strip()
        raise LegacyFailed(
            "programa legado " + str(program) + " saiu com codigo "
            + str(completed.returncode) + (": " + detail[-500:] if detail else ""),
            returncode=completed.returncode,
            stderr=detail,
        )
    return completed.stdout
