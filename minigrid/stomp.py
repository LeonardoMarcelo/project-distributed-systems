"""Cliente STOMP 1.2 escrito a mao sobre socket TCP.

Por que existe, em vez de usar o pika como o resto do sistema: o worker do host
(item G) roda fora do Docker, na maquina do avaliador, e nao pode instalar nada.
AMQP 0-9-1 e binario e exigiria biblioteca; STOMP e um protocolo de texto que
cabe em `socket` da biblioteca padrao.

O ganho nao e so de dependencia. O item A exige o sistema organizado em camadas,
incluindo a camada de transporte — e em todo o resto do projeto o transporte fica
escondido dentro do gRPC e do pika. Aqui o socket e aberto, o frame e montado e a
camada aparece.

Formato do frame:

    COMANDO\\n
    cabecalho:valor\\n
    cabecalho:valor\\n
    \\n
    corpo\\x00

Assinando `/amq/queue/<nome>`, este cliente consome da *mesma* fila duravel que
os workers em conteiner consomem por AMQP. `NACK` com `requeue:true` devolve a
mensagem para a fila, que e o mecanismo de migracao de tarefa do item E.

Stdlib pura (ver minigrid/__init__.py).
"""
import socket


class StompError(Exception):
    """Frame ERROR recebido do broker, ou protocolo violado."""


class Frame(object):
    """Um frame STOMP recebido."""

    __slots__ = ("command", "headers", "body")

    def __init__(self, command, headers, body):
        self.command = command
        self.headers = headers
        self.body = body

    @property
    def ack_id(self):
        """Valor do cabecalho `ack`, exigido por ACK/NACK no STOMP 1.2."""
        return self.headers.get("ack")

    def __repr__(self):
        return "<Frame " + self.command + " " + str(len(self.body)) + "B>"


def build_frame(command, headers, body=b""):
    """Serializa um frame. Usada tambem pelos testes do protocolo."""
    out = command.encode("utf-8") + b"\n"
    for key, value in headers.items():
        out += str(key).encode("utf-8") + b":" + str(value).encode("utf-8") + b"\n"
    return out + b"\n" + body + b"\x00"


class StompClient(object):
    def __init__(self, host="localhost", port=61613, login="guest", passcode="guest",
                 vhost="/"):
        self.host = host
        self.port = port
        self.login = login
        self.passcode = passcode
        self.vhost = vhost
        self._sock = None
        self._buf = b""
        self._sub_id = 0

    # ---------------------------------------------------------------- sessao

    def connect(self, timeout=15):
        self._sock = socket.create_connection((self.host, self.port), timeout=timeout)
        self._sock.settimeout(timeout)
        self._send_frame("CONNECT", {
            "accept-version": "1.2",
            "host": self.vhost,
            "login": self.login,
            "passcode": self.passcode,
            # Sem heartbeat: evita precisar de thread de keepalive no worker do
            # host. O TCP e o health check do Consul ja detectam queda.
            "heart-beat": "0,0",
        })
        frame = self._read_frame(timeout)
        if frame is None or frame.command != "CONNECTED":
            raise StompError("CONNECT recusado: " + repr(frame))
        return frame

    def close(self):
        if self._sock is None:
            return
        try:
            self._send_frame("DISCONNECT", {})
        except OSError:
            pass
        try:
            self._sock.close()
        finally:
            self._sock = None
            self._buf = b""

    # ------------------------------------------------------------- operacoes

    def send(self, destination, body, persistent=True, headers=None):
        """Publica no destino.

        `/amq/queue/<nome>` entrega numa fila AMQP existente;
        `/exchange/<nome>/<routing-key>` publica num exchange.

        `content-length` vai sempre: sem ele, o broker trata o corpo como texto
        terminado em nulo e um byte nulo no payload corta a mensagem.
        """
        frame_headers = {
            "destination": destination,
            "content-length": str(len(body)),
        }
        if persistent:
            frame_headers["persistent"] = "true"
        if headers:
            frame_headers.update(headers)
        self._send_frame("SEND", frame_headers, body)

    def subscribe(self, destination, ack="client", prefetch=1):
        """Assina o destino.

        `ack=client` e obrigatorio para que ACK/NACK sejam nossos — e o que
        permite devolver a tarefa a fila na migracao. `prefetch` limita quantas
        mensagens o broker adianta; 1 por padrao, igual ao worker AMQP.
        """
        self._sub_id += 1
        sub_id = str(self._sub_id)
        self._send_frame("SUBSCRIBE", {
            "id": sub_id,
            "destination": destination,
            "ack": ack,
            "prefetch-count": str(prefetch),
        })
        return sub_id

    def receive(self, timeout=5):
        """Proximo frame MESSAGE, ou None se nada chegou dentro do timeout.

        Timeout nao fecha a conexao: o laco do worker alterna entre esperar
        tarefa e bater heartbeat no Consul.
        """
        frame = self._read_frame(timeout)
        if frame is None:
            return None
        if frame.command == "ERROR":
            raise StompError(frame.headers.get("message", "") + ": "
                             + frame.body.decode("utf-8", "replace"))
        if frame.command != "MESSAGE":
            raise StompError("frame inesperado: " + frame.command)
        return frame

    def ack(self, frame):
        """Confirma: a mensagem sai da fila."""
        if frame.ack_id is None:
            raise StompError("frame sem cabecalho `ack`; assine com ack=client")
        self._send_frame("ACK", {"id": frame.ack_id})

    def nack(self, frame, requeue=True):
        """Recusa. Com `requeue=True`, a mensagem volta para a fila.

        E este caminho que entrega a migracao do item E: o worker que recebe
        SIGTERM no meio da tarefa devolve a mensagem, e o broker a reentrega a
        outro worker, que retoma do ultimo checkpoint.
        """
        if frame.ack_id is None:
            raise StompError("frame sem cabecalho `ack`; assine com ack=client")
        self._send_frame("NACK", {
            "id": frame.ack_id,
            "requeue": "true" if requeue else "false",
        })

    # ---------------------------------------------------------------- frames

    def _send_frame(self, command, headers, body=b""):
        if self._sock is None:
            raise StompError("socket fechado; chame connect() antes")
        self._sock.sendall(build_frame(command, headers, body))

    def _read_frame(self, timeout):
        """Le um frame do buffer, puxando do socket so quando falta dado.

        O buffer e de instancia porque dois frames podem chegar no mesmo `recv`;
        o excedente fica guardado para a proxima chamada em vez de ser perdido.
        """
        if self._sock is None:
            raise StompError("socket fechado; chame connect() antes")
        self._sock.settimeout(timeout)

        while True:
            parsed = self._try_parse()
            if parsed is not None:
                return parsed
            try:
                chunk = self._sock.recv(65536)
            except socket.timeout:
                return None
            if not chunk:
                raise StompError("conexao fechada pelo broker")
            self._buf += chunk

    def _try_parse(self):
        """Extrai um frame completo do buffer, ou None se ainda falta dado."""
        # Frames de heartbeat/keepalive sao linhas vazias; descarta.
        while self._buf.startswith(b"\n"):
            self._buf = self._buf[1:]
        if self._buf.startswith(b"\r\n"):
            self._buf = self._buf[2:]

        if b"\n\n" not in self._buf:
            return None

        head, rest = self._buf.split(b"\n\n", 1)
        lines = head.split(b"\n")
        command = lines[0].decode("utf-8").strip()
        headers = {}
        for line in lines[1:]:
            if b":" in line:
                key, value = line.split(b":", 1)
                headers[key.decode("utf-8")] = value.decode("utf-8")

        if "content-length" in headers:
            need = int(headers["content-length"])
            if len(rest) < need + 1:          # +1 pelo nulo terminador
                return None
            body = rest[:need]
            remainder = rest[need + 1:]
        else:
            if b"\x00" not in rest:
                return None
            body, remainder = rest.split(b"\x00", 1)

        self._buf = remainder
        return Frame(command, headers, body)
