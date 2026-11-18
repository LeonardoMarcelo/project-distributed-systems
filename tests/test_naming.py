from conftest import TEST_KV_PREFIX, needs_infra

pytestmark = needs_infra


def test_register_and_list_service(consul):
    consul.register_service("w-test", "minigrid-worker", ["arch:test", "virt:test"], 10)
    ids = [s["ID"] for s in consul.list_services("minigrid-worker")]
    assert "w-test" in ids


def test_pass_check_keeps_the_service_passing(consul):
    consul.register_service("w-test", "minigrid-worker", ["arch:test"], 10)
    consul.pass_check("w-test")
    entry = [s for s in consul.list_services("minigrid-worker") if s["ID"] == "w-test"][0]
    assert entry["Status"] == "passing"


def test_deregister_removes_the_service(consul):
    consul.register_service("w-test", "minigrid-worker", [], 10)
    consul.deregister_service("w-test")
    assert "w-test" not in [s["ID"] for s in consul.list_services("minigrid-worker")]


def test_kv_put_get_delete(consul):
    consul.kv_put(TEST_KV_PREFIX + "k", b"v")
    assert consul.kv_get(TEST_KV_PREFIX + "k") == b"v"
    consul.kv_delete(TEST_KV_PREFIX + "k")
    assert consul.kv_get(TEST_KV_PREFIX + "k") is None


def test_kv_get_of_missing_key_is_none(consul):
    assert consul.kv_get(TEST_KV_PREFIX + "ausente") is None


def test_kv_value_survives_arbitrary_bytes(consul):
    payload = b"\x00\xff\x01binario"
    consul.kv_put(TEST_KV_PREFIX + "bin", payload)
    assert consul.kv_get(TEST_KV_PREFIX + "bin") == payload


def test_kv_keys_lists_by_prefix(consul):
    consul.kv_put(TEST_KV_PREFIX + "a/owner", b"w1")
    consul.kv_put(TEST_KV_PREFIX + "b/owner", b"w2")
    keys = consul.kv_keys(TEST_KV_PREFIX)
    assert TEST_KV_PREFIX + "a/owner" in keys
    assert TEST_KV_PREFIX + "b/owner" in keys


def test_kv_keys_of_empty_prefix_is_empty_list(consul):
    assert consul.kv_keys(TEST_KV_PREFIX + "nada-aqui/") == []


def test_only_one_session_acquires_the_same_key(consul):
    s1 = consul.session_create("a", 10, "delete", "0s")
    s2 = consul.session_create("b", 10, "delete", "0s")
    assert consul.kv_put(TEST_KV_PREFIX + "lock", b"1", acquire=s1) is True
    assert consul.kv_put(TEST_KV_PREFIX + "lock", b"2", acquire=s2) is False


def test_destroying_session_releases_the_key(consul):
    s1 = consul.session_create("a", 10, "delete", "0s")
    consul.kv_put(TEST_KV_PREFIX + "lock2", b"1", acquire=s1)
    consul.session_destroy(s1)
    s2 = consul.session_create("b", 10, "delete", "0s")
    assert consul.kv_put(TEST_KV_PREFIX + "lock2", b"2", acquire=s2) is True


def test_session_info_reports_zero_lock_delay(consul):
    s1 = consul.session_create("a", 10, "delete", "0s")
    assert consul.session_info(s1)["LockDelay"] == 0


def test_session_renew_keeps_the_session_alive(consul):
    s1 = consul.session_create("a", 10, "delete", "0s")
    consul.session_renew(s1)
    assert consul.session_info(s1) is not None


def test_kv_watch_returns_the_current_value_and_an_index(consul):
    consul.kv_put(TEST_KV_PREFIX + "w", b"v1")
    index, value = consul.kv_watch(TEST_KV_PREFIX + "w", 0, 1)
    assert value == b"v1" and index > 0


def test_kv_watch_blocks_until_timeout_when_nothing_changes(consul):
    import time

    consul.kv_put(TEST_KV_PREFIX + "w2", b"v1")
    index, _ = consul.kv_watch(TEST_KV_PREFIX + "w2", 0, 1)
    t0 = time.monotonic()
    consul.kv_watch(TEST_KV_PREFIX + "w2", index, 1)
    assert time.monotonic() - t0 >= 0.9
