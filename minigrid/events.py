"""Publicacao e assinatura do pub/sub de eventos (item C).

O enunciado pede que as mudancas de estado das tarefas sejam "divulgadas no
modelo de publicacao e assinatura, de modo que varios clientes e um painel de
monitoramento possam acompanha-las". O mecanismo e um exchange `topic` do
RabbitMQ: cada assinante cria a *propria* fila exclusiva e temporaria ligada ao
exchange, entao todos recebem todos os eventos — ao contrario de uma fila
compartilhada, em que um assinante roubaria o evento do outro.

Classificacao destes fluxos, para a tabela do relatorio: assincronos,
**transientes** e de granularidade *fluxo*. Transientes de proposito: um evento
de estado perdido nao prejudica a tarefa, porque o estado autoritativo esta no
Consul KV. O que nao pode se perder e a tarefa, e essa trafega por fila
duravel com mensagem persistente — ver coordinator/dispatch.py.

Usa `pika`, logo roda so em conteiner ou no runner do CI. O worker do host
publica os mesmos eventos pelo cliente STOMP escrito a mao (minigrid/stomp.py).
"""
import json
import threading
import time

import pika

DEFAULT_EXCHANGE = "minigrid.events"


def routing_key_for(kind):
    """`running` -> `task.running`; `rpc.trace` fica como esta.

    Isso deixa o assinante filtrar por `task.#` (so tarefas), `rpc.#` (so
    rastreio de chamadas) ou `#` (tudo, que e o que o painel faz).
    """
    return kind if "." in kind else "task." + kind


class Publisher(object):
    """Publicador com reconexao e serializado por lock.

    O lock nao e zelo excessivo: `pika.BlockingConnection` nao e segura entre
    threads, e aqui publicam o interceptador do gRPC (uma thread por chamada),
    o coletor de resultados e o reaper.
    """

    def __init__(self, amqp_url, exchange=DEFAULT_EXCHANGE):
        self.amqp_url = amqp_url
        self.exchange = exchange
        self._lock = threading.Lock()
        self._connection = None
        self._channel = None

    def _ensure(self):
        if self._channel is not None and self._channel.is_open:
            return self._channel
        self._connection = pika.BlockingConnection(pika.URLParameters(self.amqp_url))
        self._channel = self._connection.channel()
        self._channel.exchange_declare(
            exchange=self.exchange, exchange_type="topic", durable=True
        )
        return self._channel

    def publish(self, kind, payload):
        """Publica um evento. Devolve True se saiu, False se nao deu.

        Nao levanta: quem publica evento esta sempre no meio de outra coisa mais
        importante (atender um RPC, executar uma tarefa), e um broker de eventos
        indisponivel nao pode derrubar essas coisas.
        """
        body = dict(payload)
        body.setdefault("at", time.time())
        body.setdefault("kind", kind)
        encoded = json.dumps(body, default=str).encode("utf-8")

        with self._lock:
            for tentativa in (1, 2):
                try:
                    channel = self._ensure()
                    channel.basic_publish(
                        exchange=self.exchange,
                        routing_key=routing_key_for(kind),
                        body=encoded,
                        properties=pika.BasicProperties(
                            content_type="application/json",
                            delivery_mode=1,     # transiente, por desenho
                        ),
                    )
                    return True
                except Exception:
                    self._discard()
                    if tentativa == 2:
                        return False
        return False

    def _discard(self):
        for obj in (self._channel, self._connection):
            try:
                if obj is not None and obj.is_open:
                    obj.close()
            except Exception:
                pass
        self._channel = None
        self._connection = None

    def close(self):
        with self._lock:
            self._discard()


class Subscriber(object):
    """Assinante com fila propria, exclusiva e temporaria.

    Cada instancia recebe *todos* os eventos que casam com o padrao. E o que
    permite dois clientes acompanharem a mesma tarefa e o painel ver tudo ao
    mesmo tempo.
    """

    def __init__(self, amqp_url, exchange=DEFAULT_EXCHANGE, pattern="#"):
        self.amqp_url = amqp_url
        self.exchange = exchange
        self.pattern = pattern
        self._connection = None
        self._channel = None
        self._queue = None
        self._closed = False

    def open(self):
        self._connection = pika.BlockingConnection(pika.URLParameters(self.amqp_url))
        self._channel = self._connection.channel()
        self._channel.exchange_declare(
            exchange=self.exchange, exchange_type="topic", durable=True
        )
        declared = self._channel.queue_declare(
            queue="", exclusive=True, auto_delete=True
        )
        self._queue = declared.method.queue
        self._channel.queue_bind(
            exchange=self.exchange, queue=self._queue, routing_key=self.pattern
        )
        return self

    def listen(self, inactivity_timeout=1.0):
        """Gera (kind, payload) conforme os eventos chegam.

        Emite `(None, None)` quando passa `inactivity_timeout` sem evento. Isso
        parece estranho mas e necessario: quem consome precisa de uma chance de
        checar se deve parar (um `Watch` cancelado pelo cliente, um worker
        recebendo SIGTERM) sem ficar preso num bloqueio sem fim.
        """
        if self._channel is None:
            self.open()
        for method, properties, body in self._channel.consume(
            self._queue, auto_ack=True, inactivity_timeout=inactivity_timeout
        ):
            if self._closed:
                return
            if method is None:
                yield None, None
                continue
            try:
                payload = json.loads(body.decode("utf-8"))
            except (ValueError, UnicodeDecodeError):
                continue
            yield payload.get("kind") or method.routing_key, payload

    def close(self):
        self._closed = True
        for obj in (self._channel, self._connection):
            try:
                if obj is not None and obj.is_open:
                    obj.close()
            except Exception:
                pass
        self._channel = None
        self._connection = None
