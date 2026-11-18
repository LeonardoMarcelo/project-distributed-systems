"""Testes do interceptador de servidor (item A).

O enunciado pede "pelo menos um interceptador, que capture todas as chamadas
para registrar tempo de resposta e origem". No gRPC isso e um recurso nativo —
o termo do Tanenbaum e o nome da feature — e e por isso que o gRPC foi escolhido
para o item B.

O interceptador recebe a funcao de publicacao injetada, em vez de falar com o
RabbitMQ direto. E o que torna estes testes possiveis sem broker nenhum.
"""
import concurrent.futures
import threading

import grpc
import pytest

from coordinator.interceptor import TraceInterceptor
from minigrid._pb2 import minigrid_pb2 as pb
from minigrid._pb2 import minigrid_pb2_grpc as pb_grpc


class EchoServicer(pb_grpc.MiniGridServicer):
    """Servicer minimo: so o suficiente para o interceptador ter o que medir."""

    def Submit(self, request, context):
        return pb.SubmitReply(task_id="t-" + request.recipe)

    def GetStatus(self, request, context):
        return pb.TaskStatus(task_id=request.task_id, state=pb.RUNNING)

    def GetResult(self, request, context):
        context.abort(grpc.StatusCode.NOT_FOUND, "tarefa inexistente")

    def Watch(self, request, context):
        for kind in ("running", "done"):
            yield pb.TaskEvent(task_id=request.task_id, kind=kind)

    def ListWorkers(self, request, context):
        raise RuntimeError("explosao nao tratada")


class Harness(object):
    def __init__(self):
        self.traces = []
        self._lock = threading.Lock()
        self.server = grpc.server(
            concurrent.futures.ThreadPoolExecutor(max_workers=4),
            interceptors=[TraceInterceptor(self._record)],
        )
        pb_grpc.add_MiniGridServicer_to_server(EchoServicer(), self.server)
        self.port = self.server.add_insecure_port("127.0.0.1:0")
        self.server.start()
        self.channel = grpc.insecure_channel("127.0.0.1:" + str(self.port))
        self.stub = pb_grpc.MiniGridStub(self.channel)

    def _record(self, kind, payload):
        with self._lock:
            self.traces.append((kind, payload))

    def stop(self):
        self.channel.close()
        self.server.stop(0).wait()


@pytest.fixture
def harness():
    h = Harness()
    yield h
    h.stop()


def test_interceptor_records_method_peer_elapsed_and_code(harness):
    harness.stub.GetStatus(pb.TaskId(task_id="x"))
    assert len(harness.traces) == 1
    kind, payload = harness.traces[0]
    assert kind == "rpc.trace"
    assert payload["method"].endswith("GetStatus")
    assert payload["peer"].startswith("ipv")
    assert payload["elapsed_ms"] >= 0
    assert payload["code"] == "OK"


def test_interceptor_records_the_code_of_a_failing_call(harness):
    with pytest.raises(grpc.RpcError):
        harness.stub.GetResult(pb.TaskId(task_id="missing"))
    assert harness.traces[-1][1]["code"] == "NOT_FOUND"


def test_interceptor_records_an_unhandled_exception_as_unknown(harness):
    with pytest.raises(grpc.RpcError):
        harness.stub.ListWorkers(pb.Empty())
    trace = harness.traces[-1][1]
    assert trace["code"] in ("UNKNOWN", "INTERNAL")
    assert trace["method"].endswith("ListWorkers")


def test_interceptor_covers_streaming_calls_too(harness):
    events = list(harness.stub.Watch(pb.TaskId(task_id="x")))
    assert [e.kind for e in events] == ["running", "done"]
    trace = harness.traces[-1][1]
    assert trace["method"].endswith("Watch")
    assert trace["streamed"] == 2


def test_interceptor_captures_every_call_not_just_the_first(harness):
    harness.stub.GetStatus(pb.TaskId(task_id="a"))
    harness.stub.GetStatus(pb.TaskId(task_id="b"))
    harness.stub.Submit(pb.SubmitRequest(recipe="pi"))
    assert len(harness.traces) == 3
    assert [t[1]["method"].rsplit("/", 1)[-1] for t in harness.traces] == [
        "GetStatus", "GetStatus", "Submit",
    ]


def test_interceptor_reads_the_client_id_from_metadata(harness):
    harness.stub.GetStatus(pb.TaskId(task_id="a"),
                           metadata=(("x-minigrid-client-id", "cli-42"),))
    assert harness.traces[-1][1]["client_id"] == "cli-42"


def test_trace_payload_is_json_serializable(harness):
    import json

    harness.stub.GetStatus(pb.TaskId(task_id="a"))
    json.dumps(harness.traces[-1][1])


def test_publish_failure_does_not_break_the_rpc():
    """Evento perdido e transiente por desenho (§7.9); a chamada nao pode cair.

    O inverso seria pior que inutil: uma queda do broker de eventos derrubaria
    a submissao de tarefas, que e o caminho que *nao* pode falhar.
    """
    def explode(kind, payload):
        raise RuntimeError("broker de eventos fora do ar")

    server = grpc.server(
        concurrent.futures.ThreadPoolExecutor(max_workers=2),
        interceptors=[TraceInterceptor(explode)],
    )
    pb_grpc.add_MiniGridServicer_to_server(EchoServicer(), server)
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    channel = grpc.insecure_channel("127.0.0.1:" + str(port))
    try:
        reply = pb_grpc.MiniGridStub(channel).Submit(pb.SubmitRequest(recipe="pi"))
        assert reply.task_id == "t-pi"
    finally:
        channel.close()
        server.stop(0).wait()
