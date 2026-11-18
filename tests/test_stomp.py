import uuid

import pytest

from conftest import STOMP_HOST, STOMP_PORT, mgmt, needs_infra

pytestmark = needs_infra


@pytest.fixture
def amqp_queue():
    """Fila duravel de nome aleatorio, removida no teardown."""
    name = "test." + uuid.uuid4().hex[:10]
    mgmt("PUT", "/queues/%2F/" + name, {"durable": True, "auto_delete": False})
    yield name
    mgmt("DELETE", "/queues/%2F/" + name, expect_json=False)


@pytest.fixture
def stomp():
    from minigrid import stomp as stomp_mod

    client = stomp_mod.StompClient(STOMP_HOST, STOMP_PORT, "guest", "guest")
    client.connect()
    yield client
    client.close()


def test_send_and_receive_then_ack(stomp, amqp_queue):
    stomp.send("/amq/queue/" + amqp_queue, b"hello")
    stomp.subscribe("/amq/queue/" + amqp_queue)
    frame = stomp.receive(timeout=5)
    assert frame is not None and frame.body == b"hello"
    stomp.ack(frame)
    assert stomp.receive(timeout=1) is None


def test_nack_with_requeue_redelivers_the_same_message(stomp, amqp_queue):
    stomp.send("/amq/queue/" + amqp_queue, b"again")
    stomp.subscribe("/amq/queue/" + amqp_queue)
    first = stomp.receive(timeout=5)
    assert first is not None
    stomp.nack(first, requeue=True)
    second = stomp.receive(timeout=5)
    assert second is not None and second.body == b"again"
    stomp.ack(second)


def test_body_with_null_byte_survives_the_frame(stomp, amqp_queue):
    payload = b"a\x00b\x00c"
    stomp.send("/amq/queue/" + amqp_queue, payload)
    stomp.subscribe("/amq/queue/" + amqp_queue)
    frame = stomp.receive(timeout=5)
    assert frame is not None and frame.body == payload
    stomp.ack(frame)


def test_receive_returns_none_on_timeout_without_closing(stomp, amqp_queue):
    stomp.subscribe("/amq/queue/" + amqp_queue)
    assert stomp.receive(timeout=1) is None
    stomp.send("/amq/queue/" + amqp_queue, b"depois")
    assert stomp.receive(timeout=5).body == b"depois"


def test_persistent_send_survives_with_no_consumer(stomp, amqp_queue):
    """Desacoplamento no tempo (item C): mensagem fica na fila sem consumidor."""
    from conftest import queue_depth, wait_until

    stomp.send("/amq/queue/" + amqp_queue, b"guardada", persistent=True)
    assert wait_until(lambda: queue_depth(amqp_queue) == 1, timeout=5)


def test_send_to_exchange_destination_reaches_a_bound_queue(stomp, amqp_queue):
    """Pub/sub (item C): SEND para /exchange/<nome>/<routing-key>."""
    from conftest import queue_depth, wait_until

    mgmt("PUT", "/exchanges/%2F/test.events", {"type": "topic", "durable": True})
    mgmt("POST", "/bindings/%2F/e/test.events/q/" + amqp_queue,
         {"routing_key": "task.#"}, expect_json=False)
    stomp.send("/exchange/test.events/task.done", b"evento")
    assert wait_until(lambda: queue_depth(amqp_queue) == 1, timeout=5)
    mgmt("DELETE", "/exchanges/%2F/test.events", expect_json=False)


def test_multiple_frames_in_one_tcp_read_are_both_delivered(stomp, amqp_queue):
    """Dois MESSAGE podem chegar no mesmo recv; o buffer nao pode perder o segundo."""
    stomp.send("/amq/queue/" + amqp_queue, b"um")
    stomp.send("/amq/queue/" + amqp_queue, b"dois")
    stomp.subscribe("/amq/queue/" + amqp_queue, prefetch=10)
    bodies = []
    for _ in range(2):
        frame = stomp.receive(timeout=5)
        assert frame is not None
        bodies.append(frame.body)
        stomp.ack(frame)
    assert sorted(bodies) == [b"dois", b"um"]
