"""Testes do coordenador: RPC, despacho, coleta de resultado e reaper.

O coordenador e a fronteira entre o item B (chamadas remotas do cliente) e o
item C (distribuicao por fila). Ele roda aqui em processo, contra o Consul e o
RabbitMQ de verdade do compose — nao contra mocks. Cada teste usa um prefixo de
KV e nomes de fila proprios, para nao colidir com um sistema em execucao.
"""
import json
import threading
import time
import uuid

import grpc
import pytest

from conftest import mgmt, needs_infra, queue_depth, wait_until
from coordinator import server as server_mod
from minigrid import envelope
from minigrid._pb2 import minigrid_pb2 as pb
from minigrid._pb2 import minigrid_pb2_grpc as pb_grpc

pytestmark = needs_infra

PI_STEP = (
    "def step(state, data, api):\n"
    "    return {'n': 1}, True, b'ok'\n"
)


class Stack(object):
    """Uma ou duas replicas de coordenador, em processo, com topologia isolada."""

    def __init__(self, consul, replicas=1, blob_dir=None):
        self.token = uuid.uuid4().hex[:10]
        self.config = server_mod.Config(
            kv_prefix="minigrid/test/" + self.token + "/",
            tasks_queue="test." + self.token + ".tasks",
            legacy_queue="test." + self.token + ".tasks.legacy",
            results_queue="test." + self.token + ".results",
            events_exchange="test." + self.token + ".events",
            election_key="minigrid/test/" + self.token + "/leader",
        )
        self.blob_dir = str(blob_dir) if blob_dir else None
        self.nodes = []
        for index in range(replicas):
            node = server_mod.MiniGridServer(
                consul=consul,
                config=self.config,
                name="coord-" + str(index + 1),
                internal_addr="127.0.0.1:0",
                external_addr="127.0.0.1:0",
                blob_dir=self.blob_dir,
                port=0,
            )
            node.start()
            self.nodes.append(node)
        assert wait_until(lambda: any(n.is_leader() for n in self.nodes), timeout=10), \
            "nenhuma replica assumiu a lideranca"
        self._channels = []

    @property
    def leader(self):
        return [n for n in self.nodes if n.is_leader()][0]

    @property
    def follower(self):
        return [n for n in self.nodes if not n.is_leader()][0]

    def stub_for(self, node):
        channel = grpc.insecure_channel(
            "127.0.0.1:" + str(node.port),
            options=list(envelope.GRPC_CHANNEL_OPTIONS),
        )
        self._channels.append(channel)
        return pb_grpc.MiniGridStub(channel)

    @property
    def stub(self):
        return self.stub_for(self.leader)

    def kill_leader(self):
        self.leader.stop()

    def stop(self):
        for channel in self._channels:
            channel.close()
        for node in self.nodes:
            try:
                node.stop()
            except Exception:
                pass
        for queue in (self.config.tasks_queue, self.config.legacy_queue,
                      self.config.results_queue):
            try:
                mgmt("DELETE", "/queues/%2F/" + queue, expect_json=False)
            except Exception:
                pass
        try:
            mgmt("DELETE", "/exchanges/%2F/" + self.config.events_exchange,
                 expect_json=False)
        except Exception:
            pass


@pytest.fixture
def stack(consul, tmp_path):
    s = Stack(consul, replicas=1, blob_dir=tmp_path / "blobs")
    yield s
    s.stop()


@pytest.fixture
def pair(consul, tmp_path):
    s = Stack(consul, replicas=2, blob_dir=tmp_path / "blobs")
    yield s
    s.stop()


def submit(stack, recipe="pi", code=PI_STEP, data=b"", params=None, requires=None):
    reply = stack.stub.Submit(pb.SubmitRequest(
        recipe=recipe, code=code, data=data,
        params=params or {}, requires=requires or [],
    ))
    return reply.task_id


# -------------------------------------------------------------------- Submit

def test_submit_returns_a_task_id_quickly(stack):
    """Item B: a submissao e assincrona — devolve o id sem esperar a execucao."""
    started = time.monotonic()
    task_id = submit(stack, params={"steps": "3"})
    assert task_id
    assert (time.monotonic() - started) < 1.0


def test_submit_rejects_payload_over_the_limit(stack):
    """Review Focus 2: payload grande recusado na borda, com erro claro.

    INVALID_ARGUMENT, nao RESOURCE_EXHAUSTED: o limite tem de ser o do MiniGrid,
    anunciado e explicado, nao o teto de 4 MB que o gRPC aplica por padrao com
    uma mensagem de transporte opaca.
    """
    with pytest.raises(grpc.RpcError) as exc:
        stack.stub.Submit(pb.SubmitRequest(
            recipe="compress", code=PI_STEP,
            data=b"x" * (envelope.MAX_PAYLOAD + 1),
        ))
    assert exc.value.code() == grpc.StatusCode.INVALID_ARGUMENT
    assert str(envelope.MAX_PAYLOAD) in exc.value.details()


def test_submit_accepts_a_payload_just_under_the_limit(stack):
    """Guarda o outro lado: sem isto, baixar o limite demais passaria calado."""
    task_id = submit(stack, recipe="compress",
                     data=b"x" * (envelope.MAX_PAYLOAD - 4096))
    assert task_id
    assert stack.stub.GetStatus(pb.TaskId(task_id=task_id)).state == pb.RECEIVED


def test_result_larger_than_the_grpc_default_can_be_fetched(stack):
    """O mesmo teto vale na volta: um resultado de 6 MB tem de passar."""
    task_id = submit(stack)
    grande = b"R" * (6 * 1024 * 1024)
    _finish(stack, task_id, grande)
    assert wait_until(
        lambda: len(stack.stub.GetResult(pb.TaskId(task_id=task_id)).result) == len(grande),
        timeout=25)


def test_submit_rejects_empty_code(stack):
    with pytest.raises(grpc.RpcError) as exc:
        stack.stub.Submit(pb.SubmitRequest(recipe="pi", code=""))
    assert exc.value.code() == grpc.StatusCode.INVALID_ARGUMENT


def test_submitted_task_lands_on_the_generic_queue(stack):
    submit(stack)
    assert wait_until(lambda: queue_depth(stack.config.tasks_queue) == 1, timeout=10)
    assert queue_depth(stack.config.legacy_queue) == 0


def test_task_requiring_legacy_lands_on_the_legacy_queue(stack):
    submit(stack, recipe="img.filter", requires=["legacy:imagemagick"])
    assert wait_until(lambda: queue_depth(stack.config.legacy_queue) == 1, timeout=10)
    assert queue_depth(stack.config.tasks_queue) == 0


def test_the_queued_envelope_carries_code_and_data(stack):
    """Mobilidade fraca (item E): o que viaja e codigo mais dados."""
    submit(stack, data=b"entrada", params={"k": "v"})
    assert wait_until(lambda: queue_depth(stack.config.tasks_queue) == 1, timeout=10)
    got = mgmt("POST", "/queues/%2F/" + stack.config.tasks_queue + "/get",
               {"count": 1, "ackmode": "ack_requeue_true", "encoding": "base64"})
    import base64
    raw = base64.b64decode(got[0]["payload"])
    body = envelope.decode(raw)
    assert body["code"] == PI_STEP
    assert body["data"] == b"entrada"
    assert body["params"]["k"] == "v"


def test_queued_message_is_persistent(stack):
    """Item C: delivery_mode=2, senao um restart do broker perde a tarefa."""
    submit(stack)
    assert wait_until(lambda: queue_depth(stack.config.tasks_queue) == 1, timeout=10)
    got = mgmt("POST", "/queues/%2F/" + stack.config.tasks_queue + "/get",
               {"count": 1, "ackmode": "ack_requeue_true", "encoding": "auto"})
    assert got[0]["properties"]["delivery_mode"] == 2


# ------------------------------------------------------------ estado e erro

def test_get_status_of_an_unknown_task_is_not_found(stack):
    with pytest.raises(grpc.RpcError) as exc:
        stack.stub.GetStatus(pb.TaskId(task_id="nao-existe"))
    assert exc.value.code() == grpc.StatusCode.NOT_FOUND


def test_get_status_right_after_submit_reports_received(stack):
    task_id = submit(stack)
    status = stack.stub.GetStatus(pb.TaskId(task_id=task_id))
    assert status.state == pb.RECEIVED
    assert status.recipe == "pi"


def test_get_result_of_an_unknown_task_is_not_found(stack):
    with pytest.raises(grpc.RpcError) as exc:
        stack.stub.GetResult(pb.TaskId(task_id="nao-existe"))
    assert exc.value.code() == grpc.StatusCode.NOT_FOUND


def test_list_workers_reads_the_name_service(stack, consul):
    consul.register_service("w-test", "minigrid-worker",
                            ["arch:test", "virt:container", "cap:generic"], 10)
    consul.pass_check("w-test")
    listed = stack.stub.ListWorkers(pb.Empty())
    ids = [w.id for w in listed.workers]
    assert "w-test" in ids
    entry = [w for w in listed.workers if w.id == "w-test"][0]
    assert entry.arch == "test" and entry.virt == "container"
    assert entry.status == "passing"


# --------------------------------------------------------------------- Watch

def test_watch_of_an_unknown_task_ends_instead_of_hanging(stack):
    """Review Focus 5: nao pode bloquear para sempre."""
    with pytest.raises(grpc.RpcError) as exc:
        list(stack.stub.Watch(pb.TaskId(task_id="nao-existe")))
    assert exc.value.code() == grpc.StatusCode.NOT_FOUND


def test_watch_emits_the_current_state_first(stack):
    task_id = submit(stack)
    stream = stack.stub.Watch(pb.TaskId(task_id=task_id))
    first = next(iter(stream))
    assert first.task_id == task_id and first.kind == "received"
    stream.cancel()


def test_watch_of_an_already_finished_task_yields_current_state_and_ends(stack):
    """Review Focus 5: tarefa concluida antes do Watch nao pode travar o cliente."""
    task_id = submit(stack)
    _finish(stack, task_id, b"RESULTADO")
    assert wait_until(
        lambda: stack.stub.GetStatus(pb.TaskId(task_id=task_id)).state == pb.DONE,
        timeout=15)
    events = list(stack.stub.Watch(pb.TaskId(task_id=task_id)))
    assert events[-1].state == pb.DONE


def test_two_clients_watching_the_same_task_both_receive_the_transitions(stack):
    """Review Focus 5: pub/sub, nao fila concorrente — ninguem rouba evento."""
    task_id = submit(stack)
    recebidos = {}

    def observar(rotulo):
        kinds = []
        for event in stack.stub_for(stack.leader).Watch(pb.TaskId(task_id=task_id)):
            kinds.append(event.kind)
            if event.state in (pb.DONE, pb.FAILED):
                break
        recebidos[rotulo] = kinds

    threads = [threading.Thread(target=observar, args=(r,), daemon=True)
               for r in ("a", "b")]
    for t in threads:
        t.start()
    time.sleep(1.5)                      # deixa os dois assinarem
    _finish(stack, task_id, b"RESULTADO")
    for t in threads:
        t.join(timeout=25)

    assert set(recebidos) == {"a", "b"}, recebidos
    assert "done" in recebidos["a"] and "done" in recebidos["b"]


# ------------------------------------------------------------- lider e replica

def test_only_the_leader_accepts_submissions(pair):
    with pytest.raises(grpc.RpcError) as exc:
        pair.stub_for(pair.follower).Submit(pb.SubmitRequest(recipe="pi", code=PI_STEP))
    assert exc.value.code() == grpc.StatusCode.FAILED_PRECONDITION


def test_follower_still_answers_reads(pair):
    """Leitura na replica e util: o painel nao precisa saber quem e o lider."""
    task_id = submit(pair)
    status = pair.stub_for(pair.follower).GetStatus(pb.TaskId(task_id=task_id))
    assert status.task_id == task_id


def test_follower_becomes_leader_and_accepts_submissions(pair):
    antigo = pair.leader
    seguidor = pair.follower
    antigo.stop()
    assert wait_until(seguidor.is_leader, timeout=25)
    reply = pair.stub_for(seguidor).Submit(pb.SubmitRequest(recipe="pi", code=PI_STEP))
    assert reply.task_id


# --------------------------------------------------- resultado e reaper

def test_result_collector_stores_the_blob_and_marks_the_task_done(stack):
    task_id = submit(stack)
    _finish(stack, task_id, b"OUT")
    assert wait_until(
        lambda: stack.stub.GetResult(pb.TaskId(task_id=task_id)).result == b"OUT",
        timeout=20)
    status = stack.stub.GetStatus(pb.TaskId(task_id=task_id))
    assert status.state == pb.DONE
    assert status.worker_id == "w-fake" and status.arch == "x86_64"


def test_reaper_republishes_a_task_whose_owner_vanished(stack, consul):
    """Terceiro gatilho de migracao (item E): queda abrupta do worker.

    SIGTERM e sobrecarga sao tratados pelo proprio worker. Este cobre o caso em
    que o worker morre sem conseguir devolver a mensagem — o TTL do health check
    expira e o coordenador republica a tarefa a partir do checkpoint.
    """
    task_id = submit(stack)
    # Simula um worker que pegou a tarefa, gravou checkpoint e morreu.
    _drain(stack.config.tasks_queue)
    consul.kv_put(stack.config.kv_prefix + "tasks/" + task_id + "/owner", b"ghost")
    consul.kv_put(stack.config.kv_prefix + "tasks/" + task_id + "/checkpoint",
                  json.dumps({"n": 4}).encode())
    stack.leader.set_task_state(task_id, "running", worker_id="ghost")

    republicadas = wait_until(lambda: stack.leader.reaper.tick(), timeout=15)
    assert task_id in (republicadas or [])
    assert wait_until(lambda: queue_depth(stack.config.tasks_queue) == 1, timeout=10)


def test_reaper_leaves_tasks_with_a_live_owner_alone(stack, consul):
    task_id = submit(stack)
    _drain(stack.config.tasks_queue)
    consul.register_service("w1", "minigrid-worker", ["arch:test"], 10)
    consul.pass_check("w1")
    consul.kv_put(stack.config.kv_prefix + "tasks/" + task_id + "/owner", b"w1")
    stack.leader.set_task_state(task_id, "running", worker_id="w1")
    assert stack.leader.reaper.tick() == []


def test_reaper_does_not_republish_a_finished_task(stack, consul):
    task_id = submit(stack)
    _drain(stack.config.tasks_queue)
    _finish(stack, task_id, b"OUT")
    assert wait_until(
        lambda: stack.stub.GetStatus(pb.TaskId(task_id=task_id)).state == pb.DONE,
        timeout=20)
    consul.kv_put(stack.config.kv_prefix + "tasks/" + task_id + "/owner", b"ghost")
    assert stack.leader.reaper.tick() == []


# ------------------------------------------------------------------ auxiliares

def _finish(stack, task_id, result):
    """Faz o papel de um worker que concluiu: publica o resultado na fila."""
    stack.leader.publish_result(task_id, result, {
        "worker_id": "w-fake", "arch": "x86_64", "virt": "container",
        "steps": "1", "resumed_from_step": "0",
    })


def _drain(queue):
    mgmt("POST", "/queues/%2F/" + queue + "/get",
         {"count": 100, "ackmode": "ack_requeue_false", "encoding": "auto"})
