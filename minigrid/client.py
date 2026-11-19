"""A biblioteca do middleware MiniGrid, do lado da aplicacao cliente (item A).

Esta e a camada que o enunciado pede: "o middleware deve expor ao cliente uma
interface simples e uniforme, com operacoes como submeter tarefa, consultar
estado e obter resultado, sem que o cliente conheca o endereco ou a quantidade
de trabalhadores".

Quatro transparencias, e onde cada uma e implementada aqui:

*Acesso* — `submit()` e a mesma chamada para um calculo numerico, para codigo
arbitrario enviado pelo proprio cliente e para o servico legado embrulhado. A
unica diferenca e o nome da receita.

*Localizacao* — o endereco do coordenador nao esta em nenhuma configuracao do
cliente. Ele e lido do servico de nomes (`_resolve_leader`), e qual worker vai
executar a tarefa e decidido pelo broker. O cliente nem sabe quantos existem.

*Migracao* — o `task_id` e o `watch()` continuam validos depois de a tarefa
trocar de worker. Para quem chamou, a migracao e invisivel; o que aparece, se
alguem olhar, e um evento `migrated` no fluxo.

*Falhas* — queda do lider aparece como `UNAVAILABLE` ou `FAILED_PRECONDITION`.
`_call` captura os dois, reconsulta o servico de nomes, reabre o canal na outra
replica e repete a chamada. A aplicacao nao ve erro.

O interceptador de cliente mede o tempo ponta a ponta e injeta
`x-minigrid-client-id`, que o interceptador do servidor le para registrar a
origem logica da chamada — o par dos dois e o que fecha o item A.
"""
import json
import os
import time
import uuid

import grpc

from minigrid import envelope, naming, recipes
from minigrid._pb2 import minigrid_pb2 as pb
from minigrid._pb2 import minigrid_pb2_grpc as pb_grpc

LEADER_KEY = "minigrid/coordinator/leader"

STATE_NAMES = {
    pb.STATE_UNSPECIFIED: "unspecified",
    pb.RECEIVED: "received",
    pb.RUNNING: "running",
    pb.CHECKPOINT: "checkpoint",
    pb.MIGRATED: "migrated",
    pb.DONE: "done",
    pb.FAILED: "failed",
}


class NoCoordinator(Exception):
    """Nenhum coordenador lider registrado no servico de nomes."""


class TaskFailed(Exception):
    """A tarefa terminou em falha."""


class _ClientInterceptor(grpc.UnaryUnaryClientInterceptor,
                         grpc.UnaryStreamClientInterceptor):
    """Interceptador de cliente: injeta identidade e mede o tempo ponta a ponta.

    O do servidor mede o tempo *dentro* do coordenador; este mede o que o
    usuario sente, incluindo rede e serializacao. A diferenca entre os dois e o
    custo do transporte, que e justamente o que o relatorio precisa comentar.
    """

    def __init__(self, client_id, sink):
        self.client_id = client_id
        self._sink = sink

    def _with_metadata(self, details):
        metadata = list(details.metadata or [])
        metadata.append(("x-minigrid-client-id", self.client_id))
        return details._replace(metadata=metadata)

    def _record(self, details, started, error=None):
        self._sink({
            "method": details.method,
            "client_id": self.client_id,
            "elapsed_ms": round((time.monotonic() - started) * 1000.0, 3),
            "error": str(error) if error else "",
            "at": time.time(),
        })

    def intercept_unary_unary(self, continuation, details, request):
        started = time.monotonic()
        try:
            response = continuation(self._with_metadata(details), request)
            self._record(details, started)
            return response
        except Exception as exc:
            self._record(details, started, exc)
            raise

    def intercept_unary_stream(self, continuation, details, request):
        started = time.monotonic()
        response = continuation(self._with_metadata(details), request)
        self._record(details, started)
        return response


class MiniGridClient(object):
    def __init__(self, consul_url=None, client_id=None, max_retries=5,
                 leader_key=LEADER_KEY, prefer=None):
        self.consul = naming.Consul(
            consul_url or os.environ.get("MINIGRID_CONSUL", "http://localhost:8500")
        )
        self.client_id = client_id or ("cli-" + uuid.uuid4().hex[:8])
        self.max_retries = int(os.environ.get("MINIGRID_MAX_RETRIES", max_retries))
        self.leader_key = leader_key
        # Dentro de um conteiner usa-se o endereco interno; do host, o externo.
        # A chave guarda os dois justamente porque ha consumidores dos dois
        # lados da fronteira do Docker.
        self.prefer = prefer or (
            "internal" if os.environ.get("MINIGRID_INSIDE_DOCKER") else "external"
        )
        self.last_call = None
        self._channel = None
        self._stub = None
        self._address = None

    # --------------------------------------------------- servico de nomes

    def _resolve_leader(self):
        raw = self.consul.kv_get(self.leader_key)
        if not raw:
            raise NoCoordinator(
                "nenhum coordenador lider registrado em " + self.leader_key
                + "; o compose subiu? (`docker compose up -d`)"
            )
        try:
            addresses = json.loads(raw.decode("utf-8"))
        except ValueError:
            raise NoCoordinator("registro de lider ilegivel no servico de nomes")
        address = addresses.get(self.prefer) or addresses.get("internal")
        if not address:
            raise NoCoordinator("registro de lider sem endereco utilizavel")
        return address

    def _connect(self, force=False):
        address = self._resolve_leader()
        if force or self._stub is None or address != self._address:
            self.close()
            channel = grpc.insecure_channel(
                address, options=list(envelope.GRPC_CHANNEL_OPTIONS)
            )
            self._channel = grpc.intercept_channel(
                channel, _ClientInterceptor(self.client_id, self._sink)
            )
            self._stub = pb_grpc.MiniGridStub(self._channel)
            self._address = address
        return self._stub

    def _sink(self, record):
        self.last_call = record

    def close(self):
        if self._channel is not None:
            try:
                self._channel.close()
            except Exception:
                pass
        self._channel = None
        self._stub = None
        self._address = None

    # --------------------------------------------------- chamada com repeticao

    _RETRYABLE = (
        grpc.StatusCode.UNAVAILABLE,          # replica caiu
        grpc.StatusCode.FAILED_PRECONDITION,  # replica deixou de ser lider
        grpc.StatusCode.DEADLINE_EXCEEDED,
    )

    def _call(self, method_name, request, timeout=30):
        """Chama o lider, reconsultando o servico de nomes quando preciso.

        E aqui que mora a transparencia de falhas: matar o lider no meio de uma
        sessao nao produz erro para quem chamou, produz uma pausa.
        """
        ultimo = None
        for tentativa in range(self.max_retries):
            try:
                stub = self._connect(force=tentativa > 0)
                return getattr(stub, method_name)(request, timeout=timeout)
            except NoCoordinator as exc:
                ultimo = exc
            except grpc.RpcError as exc:
                if exc.code() not in self._RETRYABLE:
                    raise
                ultimo = exc
                self.close()
            # Recuo exponencial com teto: a eleicao por expiracao de TTL leva
            # ~20 s, entao insistir rapido so gasta tentativa.
            time.sleep(min(0.5 * (2 ** tentativa), 8.0))
        raise NoCoordinator(
            "nenhum coordenador respondeu em " + str(self.max_retries)
            + " tentativas; ultimo erro: " + str(ultimo)
        )

    # ------------------------------------------------------------- interface

    def submit(self, recipe, data=b"", params=None, code=None, requires=None):
        """Submete uma tarefa e devolve o identificador, sem esperar a execucao.

        `code=None` carrega o fonte do catalogo de receitas; passar `code`
        explicitamente envia codigo arbitrario — as duas coisas sao a mesma
        operacao do ponto de vista do coordenador, que e o que torna a mobilidade
        de codigo do item E uniforme.
        """
        if code is None:
            code = recipes.load(recipe)
            params = recipes.params_for(recipe, params)
            if requires is None:
                requires = recipes.requires_for(recipe)
        request = pb.SubmitRequest(
            recipe=recipe, code=code, data=data or b"",
            params={str(k): str(v) for k, v in (params or {}).items()},
            requires=list(requires or []),
            client_id=self.client_id,
        )
        return self._call("Submit", request).task_id

    def status(self, task_id):
        reply = self._call("GetStatus", pb.TaskId(task_id=task_id))
        return {
            "task_id": reply.task_id,
            "state": STATE_NAMES.get(reply.state, "unspecified"),
            "recipe": reply.recipe,
            "worker_id": reply.worker_id,
            "arch": reply.arch,
            "virt": reply.virt,
            "step": reply.step,
            "resumed_from_step": reply.resumed_from_step,
            "migrations": reply.migrations,
            "error": reply.error,
        }

    def result(self, task_id):
        reply = self._call("GetResult", pb.TaskId(task_id=task_id))
        if reply.state == pb.FAILED:
            raise TaskFailed("tarefa " + task_id + " falhou")
        return reply.result

    def watch(self, task_id, timeout=600):
        """Itera as transicoes de estado da tarefa, por push.

        E a operacao assincrona do item B: nao ha polling. O coordenador empurra
        cada transicao por um stream de servidor, e o laco termina quando a
        tarefa conclui ou falha.
        """
        stream = self._call("Watch", pb.TaskId(task_id=task_id), timeout=timeout)
        for event in stream:
            yield {
                "task_id": event.task_id,
                "state": STATE_NAMES.get(event.state, "unspecified"),
                "kind": event.kind,
                "worker_id": event.worker_id,
                "arch": event.arch,
                "virt": event.virt,
                "step": event.step,
                "reason": event.reason,
                "error": event.error,
            }

    def wait(self, task_id, timeout=600):
        """Conveniencia: acompanha ate terminar e devolve o resultado."""
        for event in self.watch(task_id, timeout=timeout):
            if event["kind"] == "failed":
                raise TaskFailed("tarefa " + task_id + " falhou: " + event["error"])
            if event["kind"] == "done":
                break
        return self.result(task_id)

    def workers(self):
        reply = self._call("ListWorkers", pb.Empty())
        return {
            "leader": reply.leader,
            "workers": [{
                "id": w.id, "arch": w.arch, "virt": w.virt, "status": w.status,
                "capabilities": list(w.capabilities), "tags": list(w.tags),
            } for w in reply.workers],
        }
