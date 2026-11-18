#!/usr/bin/env python3
"""SPIKE DESCARTAVEL — removido na Task 3.

Unica pergunta: `NACK` com `requeue:true` por STOMP reentrega a mesma mensagem
numa fila duravel `/amq/queue/`? Se nao, o worker do host cai no plano B
(vendorizar o pika) antes de qualquer codigo ser escrito em cima do STOMP.

Stdlib pura, de proposito: e o mesmo ambiente do worker do host.
"""
import base64
import json
import socket
import sys
import urllib.request

MGMT = "http://localhost:15672/api"
AUTH = base64.b64encode(b"guest:guest").decode()
QUEUE = "spike.q"
HOST, PORT = "localhost", 61613


def mgmt(method, path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(MGMT + path, data=data, method=method)
    req.add_header("Authorization", "Basic " + AUTH)
    req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=10) as r:
        raw = r.read()
    return json.loads(raw) if raw else None


def frame(command, headers, body=b""):
    out = command.encode() + b"\n"
    for k, v in headers.items():
        out += str(k).encode() + b":" + str(v).encode() + b"\n"
    return out + b"\n" + body + b"\x00"


def read_frame(sock):
    """Le um frame respeitando content-length (corpo pode conter byte nulo)."""
    buf = b""
    while b"\n\n" not in buf:
        chunk = sock.recv(4096)
        if not chunk:
            return None
        buf += chunk
    head, rest = buf.split(b"\n\n", 1)
    lines = head.split(b"\n")
    command = lines[0].decode()
    headers = {}
    for line in lines[1:]:
        if b":" in line:
            k, v = line.split(b":", 1)
            headers[k.decode()] = v.decode()

    if "content-length" in headers:
        need = int(headers["content-length"])
        while len(rest) < need + 1:
            rest += sock.recv(4096)
        body = rest[:need]
    else:
        while b"\x00" not in rest:
            rest += sock.recv(4096)
        body = rest.split(b"\x00", 1)[0]
    return command, headers, body


def main():
    mgmt("PUT", "/queues/%2F/" + QUEUE, {"durable": True, "auto_delete": False})

    sock = socket.create_connection((HOST, PORT), timeout=10)
    sock.settimeout(10)
    sock.sendall(frame("CONNECT", {
        "accept-version": "1.2", "host": "/",
        "login": "guest", "passcode": "guest", "heart-beat": "0,0",
    }))
    cmd, _, _ = read_frame(sock)
    if cmd != "CONNECTED":
        print("REQUEUE FAIL: CONNECT devolveu", cmd)
        return 1

    payload = b"spike\x00com-nulo"
    sock.sendall(frame("SEND", {
        "destination": "/amq/queue/" + QUEUE,
        "content-length": str(len(payload)),
        "persistent": "true",
    }, payload))

    sock.sendall(frame("SUBSCRIBE", {
        "id": "0", "destination": "/amq/queue/" + QUEUE, "ack": "client",
    }))

    cmd, headers, body = read_frame(sock)
    print("1a entrega:", cmd, "corpo", body)
    if cmd != "MESSAGE" or body != payload:
        print("REQUEUE FAIL: primeira entrega errada")
        return 1

    sock.sendall(frame("NACK", {"id": headers["ack"], "requeue": "true"}))

    try:
        cmd2, headers2, body2 = read_frame(sock)
    except socket.timeout:
        print("REQUEUE FAIL: nada reentregue em 10s")
        return 1

    print("2a entrega:", cmd2, "corpo", body2)
    if cmd2 == "MESSAGE" and body2 == payload:
        sock.sendall(frame("ACK", {"id": headers2["ack"]}))
        sock.close()
        mgmt("DELETE", "/queues/%2F/" + QUEUE)
        print("REQUEUE OK")
        return 0

    print("REQUEUE FAIL: reentrega veio diferente")
    return 1


if __name__ == "__main__":
    sys.exit(main())
