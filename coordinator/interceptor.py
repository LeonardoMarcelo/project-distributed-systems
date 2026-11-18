"""Interceptador de servidor gRPC (item A do enunciado).

O enunciado pede "pelo menos um interceptador, que capture todas as chamadas
para registrar tempo de resposta e origem". Isso e middleware adaptavel no
sentido do Tanenbaum: codigo que se interpoe no caminho da invocacao sem que
nem o cliente nem o servidor saibam que ele existe.

No gRPC o conceito tem suporte nativo, e foi um dos motivos de escolher gRPC
para o item B: `grpc.ServerInterceptor` entra na cadeia antes do servicer, ve
todas as chamadas de todos os metodos, e nao exige uma linha de codigo dentro
dos handlers.

O que e capturado por chamada:

    method      qual RPC
    peer        a *origem*: endereco e porta de quem chamou (context.peer())
    client_id   identidade logica, vinda do metadado x-minigrid-client-id que o
                interceptador *cliente* injeta (ver minigrid/client.py)
    elapsed_ms  o *tempo de resposta*
    code        status final, inclusive quando o handler aborta
    streamed    quantas mensagens sairam, nas chamadas de stream

A funcao de publicacao e injetada, nao importada. Duas razoes: o interceptador
fica testavel sem broker nenhum, e a dependencia aponta na direcao certa — o
interceptador nao precisa saber que existe RabbitMQ.
"""
import time

import grpc


class TraceInterceptor(grpc.ServerInterceptor):
    def __init__(self, publish, kind="rpc.trace"):
        """`publish(kind, payload)` — tipicamente events.Publisher.publish."""
        self._publish = publish
        self._kind = kind

    # ------------------------------------------------------------- interno

    def _emit(self, payload):
        """Publica o rastreio.

        Falha de publicacao e engolida de proposito. Os eventos sao transientes
        por desenho (design §7.9); derrubar uma submissao de tarefa porque o
        broker de eventos piscou seria trocar um problema pequeno por um grande.
        """
        try:
            self._publish(self._kind, payload)
        except Exception:
            pass

    @staticmethod
    def _client_id(handler_call_details):
        for key, value in (handler_call_details.invocation_metadata or ()):
            if key == "x-minigrid-client-id":
                return value
        return ""

    @staticmethod
    def _code_name(context, error):
        """Status final da chamada.

        `context.code()` e None num retorno normal. Num `context.abort()` o gRPC
        levanta de dentro do handler e o codigo fica registrado no contexto — e
        por isso que o rastreio e emitido no `finally`, nao no caminho felizes.
        """
        code = None
        try:
            code = context.code()
        except Exception:
            code = None
        if code is not None:
            return getattr(code, "name", str(code))
        if error is not None:
            return "UNKNOWN"
        return "OK"

    def _trace(self, method, context, handler_call_details, started, error,
               streamed=None):
        payload = {
            "method": method,
            "peer": context.peer() if hasattr(context, "peer") else "",
            "client_id": self._client_id(handler_call_details),
            "elapsed_ms": round((time.monotonic() - started) * 1000.0, 3),
            "code": self._code_name(context, error),
            "at": time.time(),
        }
        if streamed is not None:
            payload["streamed"] = streamed
        if error is not None:
            payload["error"] = type(error).__name__ + ": " + str(error)
        self._emit(payload)

    # -------------------------------------------------------------- publico

    def intercept_service(self, continuation, handler_call_details):
        handler = continuation(handler_call_details)
        if handler is None:
            return None
        method = handler_call_details.method

        if handler.unary_unary is not None:
            return grpc.unary_unary_rpc_method_handler(
                self._wrap_unary(handler.unary_unary, method, handler_call_details),
                request_deserializer=handler.request_deserializer,
                response_serializer=handler.response_serializer,
            )
        if handler.unary_stream is not None:
            return grpc.unary_stream_rpc_method_handler(
                self._wrap_stream(handler.unary_stream, method, handler_call_details),
                request_deserializer=handler.request_deserializer,
                response_serializer=handler.response_serializer,
            )
        # Os quatro RPCs deste servico sao unary-unary ou unary-stream. Um
        # metodo client-stream futuro passa sem rastreio em vez de quebrar.
        return handler

    def _wrap_unary(self, behavior, method, details):
        def wrapper(request, context):
            started = time.monotonic()
            error = None
            try:
                return behavior(request, context)
            except BaseException as exc:
                error = exc
                raise
            finally:
                self._trace(method, context, details, started, error)
        return wrapper

    def _wrap_stream(self, behavior, method, details):
        def wrapper(request, context):
            started = time.monotonic()
            error = None
            sent = 0
            try:
                for message in behavior(request, context):
                    sent += 1
                    yield message
            except BaseException as exc:
                error = exc
                raise
            finally:
                # No stream o tempo de resposta e o da chamada inteira, do
                # primeiro byte ao fim do stream — e o que interessa no `Watch`,
                # que fica aberto do `Submit` ate a tarefa concluir.
                self._trace(method, context, details, started, error, streamed=sent)
        return wrapper
