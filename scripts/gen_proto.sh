#!/usr/bin/env bash
# Gera os stubs do protobuf a partir de proto/minigrid.proto.
#
# Roda `grpc_tools.protoc` dentro de um conteiner descartavel, de proposito: a
# meta de zero instalacao na maquina vale tambem para as ferramentas de build.
# Nada de `pip install grpcio-tools` no host.
#
# A saida vai para minigrid/_pb2/ e e *versionada*. Motivo: a suite de testes
# roda fora dos conteineres (no runner do CI e na maquina de quem desenvolve) e
# precisa dos stubs sem recompilar o .proto. O teste tests/test_proto_sync.py
# falha se a copia versionada divergir do .proto.
set -euo pipefail
cd "$(dirname "$0")/.."

# Casado com requirements/: stubs gerados por uma versao mais nova que o runtime
# instalado levantam erro de versao em tempo de import.
GRPC_VERSION="${MINIGRID_GRPC_VERSION:-1.83.1}"

mkdir -p minigrid/_pb2

docker run --rm \
  -v "$PWD":/app -w /app \
  -e GRPC_VERSION="$GRPC_VERSION" \
  python:3.12-slim bash -eu -c '
    pip install --quiet "grpcio-tools==${GRPC_VERSION}"
    python -m grpc_tools.protoc \
      --proto_path=proto \
      --python_out=minigrid/_pb2 \
      --pyi_out=minigrid/_pb2 \
      --grpc_python_out=minigrid/_pb2 \
      proto/minigrid.proto

    # O protoc gera `import minigrid_pb2 as ...`, que e um import absoluto de
    # topo e nao resolve de dentro do pacote minigrid._pb2. Reescreve para o
    # caminho do pacote.
    sed -i "s/^import minigrid_pb2 as /from minigrid._pb2 import minigrid_pb2 as /" \
      minigrid/_pb2/minigrid_pb2_grpc.py

    chmod -R a+rw minigrid/_pb2
  '

cat > minigrid/_pb2/__init__.py <<'PY'
"""Stubs gerados por scripts/gen_proto.sh. NAO editar a mao.

Versionados de proposito: a suite de testes roda fora dos conteineres e precisa
deles sem recompilar o .proto. tests/test_proto_sync.py guarda a sincronia.
"""
PY

echo "stubs gerados em minigrid/_pb2/ com grpcio-tools==${GRPC_VERSION}"
ls -1 minigrid/_pb2/
