"""Guarda a sincronia entre proto/minigrid.proto e os stubs versionados.

Os stubs sao gerados num conteiner (scripts/gen_proto.sh) e commitados, porque a
suite roda fora dos conteineres e precisa deles sem recompilar. O preco dessa
duplicacao e o apodrecimento: alguem edita o .proto, esquece de regerar, e o
sistema passa a mentir sobre a propria interface. Este teste e o antidoto.
"""
import pathlib
import shutil
import subprocess
import sys

import pytest

from conftest import PROJECT_ROOT

PB2_DIR = pathlib.Path(PROJECT_ROOT) / "minigrid" / "_pb2"
GENERATED = ("minigrid_pb2.py", "minigrid_pb2_grpc.py")

def _grpc_tools_available():
    try:
        import grpc_tools.protoc  # noqa: F401
        return True
    except ImportError:
        return False


def _docker_available():
    return shutil.which("docker") is not None


def test_versioned_stubs_exist_and_import():
    for name in GENERATED:
        assert (PB2_DIR / name).exists(), name
    from minigrid._pb2 import minigrid_pb2, minigrid_pb2_grpc  # noqa: F401


def test_grpc_stub_import_was_rewritten_for_the_package():
    """O protoc gera um import absoluto de topo que nao resolve no pacote."""
    text = (PB2_DIR / "minigrid_pb2_grpc.py").read_text()
    assert "from minigrid._pb2 import minigrid_pb2 as" in text
    assert "\nimport minigrid_pb2 as" not in text


def test_proto_defines_the_five_rpcs_the_design_promises():
    proto = (pathlib.Path(PROJECT_ROOT) / "proto" / "minigrid.proto").read_text()
    for rpc in ("Submit", "GetStatus", "GetResult", "Watch", "ListWorkers"):
        assert "rpc " + rpc + " " in proto


def test_watch_is_a_server_stream():
    """A operacao assincrona do item B depende de `Watch` empurrar, nao responder."""
    proto = (pathlib.Path(PROJECT_ROOT) / "proto" / "minigrid.proto").read_text()
    assert "returns (stream TaskEvent)" in proto


@pytest.mark.skipif(not _grpc_tools_available(),
                    reason="grpc_tools ausente no host; roda no CI e via scripts/gen_proto.sh")
def test_versioned_stubs_match_the_proto(tmp_path):
    subprocess.run(
        [sys.executable, "-m", "grpc_tools.protoc",
         "--proto_path=proto",
         "--python_out=" + str(tmp_path),
         "--grpc_python_out=" + str(tmp_path),
         "proto/minigrid.proto"],
        cwd=PROJECT_ROOT, check=True, capture_output=True,
    )
    for name in GENERATED:
        fresh = (tmp_path / name).read_text()
        if name == "minigrid_pb2_grpc.py":
            fresh = fresh.replace(
                "\nimport minigrid_pb2 as",
                "\nfrom minigrid._pb2 import minigrid_pb2 as",
            )
        assert fresh == (PB2_DIR / name).read_text(), (
            name + " dessincronizado do .proto: rode scripts/gen_proto.sh"
        )
