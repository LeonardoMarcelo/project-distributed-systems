"""Envelope de tarefa: o que trafega do coordenador ate o worker.

Uma tarefa viaja como *codigo mais dados* — e a mobilidade fraca do item E do
enunciado. O worker recebe o codigo-fonte, instancia e executa no seu ambiente
virtual. Nada aqui depende de plataforma: o codigo e texto e os dados sao bytes
em base64, o que e exatamente o motivo de a migracao entre arm64, x86_64 e host
funcionar.

Stdlib pura (ver minigrid/__init__.py).
"""
import base64
import json
import time

#: Limite de payload por tarefa. Acima disso o desenho correto seria
#: armazenamento de objetos (MinIO/S3), declarado como nao-meta no design.
MAX_PAYLOAD = 8 * 1024 * 1024

#: Versao do formato, para o worker recusar um envelope que nao entende.
FORMAT_VERSION = 1


class PayloadTooLarge(Exception):
    """Dados de entrada acima de MAX_PAYLOAD."""


class MalformedEnvelope(Exception):
    """Envelope ilegivel ou de versao desconhecida."""


def encode(task_id, recipe, code, data, params, requires, submitted_at=None):
    """Serializa uma tarefa para trafegar na fila.

    `data` sao bytes arbitrarios; viajam em base64 porque JSON nao carrega bytes.
    """
    if len(data) > MAX_PAYLOAD:
        raise PayloadTooLarge(
            "payload de " + str(len(data)) + " bytes excede o limite de "
            + str(MAX_PAYLOAD) + " bytes"
        )
    body = {
        "v": FORMAT_VERSION,
        "task_id": task_id,
        "recipe": recipe,
        "code": code,
        "data": base64.b64encode(data).decode("ascii"),
        "params": dict(params or {}),
        "requires": list(requires or []),
        "submitted_at": submitted_at if submitted_at is not None else time.time(),
    }
    return json.dumps(body, separators=(",", ":")).encode("utf-8")


def decode(raw):
    """Desserializa um envelope. Devolve dict com `data` ja em bytes."""
    try:
        body = json.loads(raw.decode("utf-8"))
    except (ValueError, UnicodeDecodeError) as exc:
        raise MalformedEnvelope("envelope ilegivel: " + str(exc))

    if body.get("v") != FORMAT_VERSION:
        raise MalformedEnvelope("versao de envelope desconhecida: " + repr(body.get("v")))

    try:
        body["data"] = base64.b64decode(body["data"])
    except (KeyError, ValueError) as exc:
        raise MalformedEnvelope("campo data invalido: " + str(exc))
    return body
