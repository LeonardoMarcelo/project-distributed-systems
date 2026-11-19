"""Testes da eleicao de lider (item F).

O enunciado exige "pelo menos duas replicas, com eleicao automatica de um novo
lider em caso de falha". A eleicao e feita com sessao mais lock de chave no
Consul: quem consegue adquirir `minigrid/coordinator/leader` e o lider, e quem
nao consegue fica observando a chave com consulta bloqueante.
"""
import json
import time

import pytest

from conftest import needs_infra, wait_until
from coordinator import election

pytestmark = needs_infra

KEY = "minigrid/test/leader"


@pytest.fixture
def elections(consul):
    """Fabrica de eleicoes que garante parada de todas no teardown."""
    criadas = []

    def make(name, internal=None, external=None, **kwargs):
        opcoes = {"key": KEY, "ttl": 10, "renew_every": 5}
        opcoes.update(kwargs)
        e = election.LeaderElection(
            consul, name,
            addresses={
                "internal": internal or (name + ":50051"),
                "external": external or "localhost:50051",
            },
            **opcoes
        )
        criadas.append(e)
        return e

    yield make

    for e in criadas:
        try:
            e.stop()
        except Exception:
            pass


def test_only_one_of_two_replicas_becomes_leader(elections):
    a, b = elections("coord-1"), elections("coord-2")
    a.start()
    b.start()
    assert wait_until(lambda: a.is_leader() or b.is_leader(), timeout=5)
    assert [a.is_leader(), b.is_leader()].count(True) == 1


def test_follower_takes_over_when_leader_stops_gracefully(elections):
    a, b = elections("coord-1"), elections("coord-2")
    a.start()
    b.start()
    assert wait_until(lambda: a.is_leader() or b.is_leader(), timeout=5)
    leader, follower = (a, b) if a.is_leader() else (b, a)
    leader.stop()
    assert wait_until(follower.is_leader, timeout=15), (
        "o seguidor nao assumiu depois da parada graciosa do lider"
    )


def test_leader_publishes_both_addresses(elections, consul):
    a = elections("coord-1", internal="coord-1:50051", external="localhost:50051")
    a.start()
    assert wait_until(a.is_leader, timeout=5)
    stored = json.loads(consul.kv_get(KEY).decode())
    assert stored["internal"] == "coord-1:50051"
    assert stored["external"] == "localhost:50051"
    assert a.current_leader()["external"] == "localhost:50051"


def test_on_change_fires_when_leadership_is_acquired(elections):
    seen = []
    a = elections("coord-1")
    a.on_change(seen.append)
    a.start()
    assert wait_until(lambda: seen == [True], timeout=5)


def test_on_change_fires_on_loss_when_the_session_is_destroyed(elections, consul):
    seen = []
    a = elections("coord-1")
    a.on_change(seen.append)
    a.start()
    assert wait_until(a.is_leader, timeout=5)
    consul.session_destroy(a.session_id)      # simula expiracao da sessao
    assert wait_until(lambda: seen[-1] is False, timeout=15), seen


def test_session_uses_zero_lock_delay(elections, consul):
    """O padrao do Consul e 15 s de LockDelay, somados ao tempo de failover."""
    a = elections("coord-1")
    a.start()
    assert wait_until(lambda: a.session_id is not None, timeout=5)
    assert consul.session_info(a.session_id)["LockDelay"] == 0


def test_session_behavior_is_delete_so_the_key_is_released(elections, consul):
    a = elections("coord-1")
    a.start()
    assert wait_until(a.is_leader, timeout=5)
    info = consul.session_info(a.session_id)
    assert info["Behavior"] == "delete"
    assert info["TTL"] == "10s"


def test_stopping_releases_the_key_immediately(elections, consul):
    a = elections("coord-1")
    a.start()
    assert wait_until(a.is_leader, timeout=5)
    a.stop()
    assert wait_until(lambda: consul.kv_get(KEY) is None, timeout=10)


def test_follower_reports_who_the_leader_is(elections):
    a, b = elections("coord-1"), elections("coord-2")
    a.start()
    b.start()
    assert wait_until(lambda: a.is_leader() or b.is_leader(), timeout=5)
    leader, follower = (a, b) if a.is_leader() else (b, a)
    assert wait_until(lambda: follower.current_leader() is not None, timeout=10)
    assert follower.current_leader()["internal"] == leader.addresses["internal"]


def test_three_replicas_still_elect_exactly_one(elections):
    nodes = [elections("coord-" + str(i)) for i in (1, 2, 3)]
    for n in nodes:
        n.start()
    assert wait_until(lambda: sum(n.is_leader() for n in nodes) == 1, timeout=8)
    assert sum(n.is_leader() for n in nodes) == 1


def test_renewal_keeps_leadership_past_the_ttl(elections):
    """Sem renovacao a sessao expiraria em 10 s e a lideranca cairia sozinha."""
    a = elections("coord-1", ttl=10, renew_every=2)
    a.start()
    assert wait_until(a.is_leader, timeout=5)
    time.sleep(12)
    assert a.is_leader(), "a lideranca caiu: a thread de renovacao nao esta funcionando"
