"""Laco de execucao com checkpoint e retomada — o coracao do item E.

Mobilidade fraca: a tarefa chega como codigo-fonte mais dados, e este modulo a
instancia na VM de processo (`worker.sandbox`) um passo por vez, persistindo o
estado entre passos. Se o worker cair, for desligado ou ficar sobrecarregado, a
tarefa volta para a fila e *outro* worker a retoma do ultimo checkpoint.

Por que a migracao entre ambientes heterogeneos funciona: nada do que migra
depende de plataforma. O codigo e texto, o checkpoint e JSON, os dados sao
bytes. Um checkpoint gravado por um worker `aarch64` e retomado sem traducao por
um worker `x86_64` emulado ou pela VM de processo no macOS do host.

Recursos locais que o destino reestabelece: o diretorio de trabalho temporario e
recriado a cada passo pelo sandbox, e os dados de entrada vem do envelope da
mensagem, nao do sistema de arquivos. Nao existe conexao de rede por tarefa — o
acesso externo passa pela API do sandbox. E por isso que nao ha nada a
"reconectar" no destino.

O executor nao conhece AMQP, STOMP nem Consul: ele recebe um `Transport`
injetado. E o que permite o mesmo codigo rodar no worker em conteiner (AMQP +
pika) e no worker do host (STOMP escrito a mao, stdlib pura).

Stdlib pura (ver worker/__init__.py).
"""
import json
import platform
import sys

from worker import sandbox

#: Limite de valor do Consul KV. Um checkpoint acima disso nao cabe no servico
#: de nomes, e a tarefa falha com mensagem em vez de corromper silenciosamente.
MAX_CHECKPOINT = 512 * 1024

#: Teto de passos por tarefa. Guarda contra receita que nunca devolve done.
DEFAULT_MAX_STEPS = 10000


class Transport(object):
    """Interface que o executor espera. Implementada por:

    - `worker.main.AmqpTransport`   — worker em conteiner, pika
    - `host_worker.worker.StompTransport` — worker do host, stdlib pura
    - `tests.test_executor.FakeTransport` — em memoria

    Documentada como classe para quem implementa ter a assinatura exata; o
    executor so faz duck typing.
    """

    def publish_event(self, kind, payload):
        raise NotImplementedError

    def save_checkpoint(self, task_id, state):
        raise NotImplementedError

    def load_checkpoint(self, task_id):
        raise NotImplementedError

    def clear_checkpoint(self, task_id):
        raise NotImplementedError

    def send_result(self, task_id, result, meta):
        raise NotImplementedError

    def report_failure(self, task_id, error, meta):
        """Relata falha pelo *mesmo caminho duravel* do resultado.

        Publicar so o evento `failed` nao basta, e isso custou um defeito real:
        os eventos sao transientes por desenho, entao o estado autoritativo no
        servico de nomes continuava `received` para sempre. As consequencias
        eram todas visiveis para quem usa — `GetStatus` mentia, `watch()`
        esperava um evento terminal que ja tinha passado e nunca retornava, e o
        reaper via a tarefa como viva e podia republica-la em laco.
        """
        raise NotImplementedError


def _never_stop():
    return False


def run_task(envelope, transport, worker_id, limits=None, checkpoint_every=1,
             should_stop=None, stop_reason="shutdown", virt="container",
             max_steps=DEFAULT_MAX_STEPS):
    """Executa a tarefa do envelope ate acabar, falhar ou ser cedida.

    `stop_reason` pode ser uma string ou um invocavel sem argumentos. O
    invocavel existe porque quem decide o motivo e o proprio `should_stop`, que
    roda *depois* desta funcao ter sido chamada: o worker so sabe que a causa foi
    sobrecarga no instante em que mede a carga. Passar o motivo como valor fixo
    congelava o padrao "shutdown", e o evento `migrated` mentia sobre a causa da
    migracao — exatamente o dado que o item E pede para explicar.

    Devolve:
      "done"     — concluida; resultado enviado e checkpoint limpo
      "failed"   — erro da tarefa; o worker segue vivo
      "yielded"  — `should_stop` pediu parada; checkpoint persistido e evento
                   `migrated` publicado. Quem chama deve devolver a mensagem a
                   fila (nack com requeue), para outro worker retomar.
    """
    limits = limits or sandbox.Limits()
    should_stop = should_stop or _never_stop
    task_id = envelope["task_id"]
    legacy_allowed = any(str(r).startswith("legacy:") for r in envelope.get("requires") or [])

    identity = {
        "task_id": task_id,
        "worker_id": worker_id,
        "arch": platform.machine(),
        "platform": sys.platform,
        "virt": virt,
        "recipe": envelope.get("recipe"),
    }

    def emit(kind, extra=None):
        payload = dict(identity)
        if extra:
            payload.update(extra)
        payload["kind"] = kind
        transport.publish_event(kind, payload)

    def fail(error, step):
        """Falha a tarefa: publica o evento E grava pelo caminho duravel.

        As duas coisas, sempre. O evento e para quem esta assistindo agora; o
        relato duravel e o que torna a falha verdade para quem consultar depois.
        """
        emit("failed", {"step": step, "error": error})
        meta = dict(identity)
        meta.update({"steps": step, "resumed_from_step": resumed_from})
        try:
            transport.report_failure(task_id, error, meta)
        except Exception:
            pass

    state = transport.load_checkpoint(task_id) or {}
    resumed_from = _step_of(state)
    steps_run = resumed_from

    emit("running", {"resumed_from_step": resumed_from, "step": resumed_from})

    while True:
        if steps_run >= max_steps:
            fail("a tarefa excedeu o teto de " + str(max_steps)
                 + " passos sem concluir; receita possivelmente sem condicao de parada",
                 steps_run)
            return "failed"

        outcome = sandbox.run_step(
            envelope["code"], state, envelope.get("data") or b"",
            limits=limits, legacy_allowed=legacy_allowed,
            params=envelope.get("params") or {},
        )

        if outcome.error:
            fail(outcome.error, steps_run)
            return "failed"

        state = outcome.state
        steps_run += 1
        state.setdefault("n", steps_run)

        if outcome.done:
            persisted = _persist(transport, task_id, state, emit, fail, steps_run)
            if persisted is False:
                return "failed"
            meta = dict(identity)
            meta.update({
                "steps": steps_run,
                "resumed_from_step": resumed_from,
                "limits_skipped": outcome.limits_skipped,
            })
            transport.send_result(task_id, outcome.result or b"", meta)
            transport.clear_checkpoint(task_id)
            emit("done", {"step": steps_run, "bytes": len(outcome.result or b"")})
            return "done"

        if steps_run % checkpoint_every == 0:
            if _persist(transport, task_id, state, emit, fail, steps_run) is False:
                return "failed"

        if should_stop():
            # Forca o checkpoint fora do intervalo: sem isso, a migracao perde
            # todo o progresso feito desde a ultima gravacao.
            if _persist(transport, task_id, state, emit, fail, steps_run,
                        announce=False) is False:
                return "failed"
            emit("migrated", {"step": steps_run,
                              "reason": _resolve_reason(stop_reason)})
            return "yielded"


def _resolve_reason(stop_reason):
    """Resolve o motivo da parada, que pode ser string ou invocavel."""
    if callable(stop_reason):
        try:
            return str(stop_reason())
        except Exception:
            return "shutdown"
    return str(stop_reason)


def _step_of(state):
    """Numero do passo representado pelo checkpoint."""
    try:
        return int(state.get("n", 0))
    except (TypeError, ValueError):
        return 0


def _persist(transport, task_id, state, emit, fail, steps_run, announce=True):
    """Grava o checkpoint. Devolve False se a tarefa deve falhar.

    O tamanho e verificado *antes* de gravar: estourar o limite do Consul KV no
    meio da escrita deixaria a tarefa num estado em que ela nem concluiu nem
    tem checkpoint valido para retomar.
    """
    try:
        size = len(json.dumps(state).encode("utf-8"))
    except (TypeError, ValueError) as exc:
        fail("checkpoint nao serializavel em JSON: " + str(exc), steps_run)
        return False

    if size > MAX_CHECKPOINT:
        fail("checkpoint de " + str(size) + " bytes excede o limite de "
             + str(MAX_CHECKPOINT) + " bytes do Consul KV; a receita precisa manter "
             "o estado pequeno e deixar dados volumosos no resultado", steps_run)
        return False

    transport.save_checkpoint(task_id, state)
    if announce:
        emit("checkpoint", {"step": steps_run, "bytes": size})
    return True
