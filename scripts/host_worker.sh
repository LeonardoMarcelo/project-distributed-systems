#!/usr/bin/env bash
# Sobe o worker que roda FORA do Docker (item G do enunciado).
#
# NAO INSTALA NADA NA SUA MAQUINA. Sem pip, sem venv, sem brew, sem sudo.
# O unico pre-requisito e um `python3` qualquer (3.9 ou mais novo), porque o
# worker usa somente a biblioteca padrao: `socket` para falar STOMP com o
# RabbitMQ e `urllib` para falar HTTP com o Consul.
#
# O sistema funciona antes deste worker existir. Quando ele sobe, se registra no
# servico de nomes e passa a receber tarefas — e isso que o item G pede que seja
# demonstrado.
set -euo pipefail
cd "$(dirname "$0")/.."

PYTHON="${MINIGRID_PYTHON:-python3}"

if ! command -v "$PYTHON" >/dev/null 2>&1; then
  cat >&2 <<EOF
ERRO: nao encontrei '$PYTHON' no PATH.

Este e o unico pre-requisito deste script. Praticamente todo macOS e toda
distribuicao Linux ja vem com python3; se o seu nao vier, instale-o pelo
gerenciador de pacotes do sistema, ou aponte para outro interpretador:

    MINIGRID_PYTHON=/caminho/para/python3 scripts/host_worker.sh
EOF
  exit 1
fi

VERSAO="$("$PYTHON" -c 'import sys; print("%d.%d" % sys.version_info[:2])')"
"$PYTHON" - <<'PY' || { echo "ERRO: Python 3.9 ou mais novo e necessario (achei $VERSAO)" >&2; exit 1; }
import sys
raise SystemExit(0 if sys.version_info >= (3, 9) else 1)
PY

echo "usando $PYTHON $VERSAO (biblioteca padrao apenas; nada sera instalado)"

CONSUL_URL="${MINIGRID_CONSUL:-http://localhost:8500}"
STOMP_HOST="${MINIGRID_STOMP_HOST:-localhost}"
STOMP_PORT="${MINIGRID_STOMP_PORT:-61613}"

falta=0
if ! "$PYTHON" - "$CONSUL_URL" <<'PY'
import sys, urllib.request
try:
    urllib.request.urlopen(sys.argv[1] + "/v1/status/leader", timeout=3).read()
except Exception as exc:
    print("  Consul inalcancavel em " + sys.argv[1] + ": " + str(exc))
    raise SystemExit(1)
PY
then falta=1; fi

if ! "$PYTHON" - "$STOMP_HOST" "$STOMP_PORT" <<'PY'
import socket, sys
try:
    socket.create_connection((sys.argv[1], int(sys.argv[2])), timeout=3).close()
except Exception as exc:
    print("  STOMP inalcancavel em " + sys.argv[1] + ":" + sys.argv[2] + ": " + str(exc))
    raise SystemExit(1)
PY
then falta=1; fi

if [ "$falta" -ne 0 ]; then
  cat >&2 <<'EOF'

ERRO: a infraestrutura do MiniGrid nao esta de pe.

Suba primeiro os componentes conteinerizados, da raiz do projeto:

    docker compose up -d

e entao rode este script de novo. (O contrario tambem funciona: o sistema opera
sem este worker e o absorve quando ele aparece — mas ele precisa de um Consul e
de um RabbitMQ para se registrar e consumir.)
EOF
  exit 1
fi

echo "Consul e STOMP alcancaveis. Subindo o worker na VM de processo..."
echo "   (Ctrl+C para parar; a tarefa em andamento e devolvida a fila e migra)"
echo

export MINIGRID_CONSUL="$CONSUL_URL"
export MINIGRID_STOMP_HOST="$STOMP_HOST"
export MINIGRID_STOMP_PORT="$STOMP_PORT"

exec "$PYTHON" -m host_worker.worker
