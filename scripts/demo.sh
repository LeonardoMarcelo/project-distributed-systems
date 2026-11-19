#!/usr/bin/env bash
# Roteiro da demonstracao, na ordem que o enunciado pede para o video.
#
# Existe para a gravacao nao depender de digitacao ao vivo nem de ordem
# decorada. Cada etapa anuncia o que vai provar, executa, e espera voce apertar
# ENTER antes da proxima — entao da tempo de narrar e de mostrar as telas.
#
#     scripts/demo.sh              # pausa entre etapas (para gravar)
#     scripts/demo.sh --auto       # sem pausas (para conferir que tudo passa)
#
# Deixe estas tres telas abertas antes de comecar:
#
#     painel do MiniGrid   http://localhost:8080   (ou a porta do seu .env)
#     servico de nomes     http://localhost:8500/ui
#     filas                http://localhost:15672
set -euo pipefail
cd "$(dirname "$0")/.."

AUTO=0
[ "${1:-}" = "--auto" ] && AUTO=1

# O compose le `.env` sozinho, mas o shell nao. Sem isto o roteiro anunciaria a
# porta padrao enquanto o painel estaria noutra, e a gravacao mostraria um link
# que nao abre.
if [ -f .env ]; then
  set -a
  . ./.env
  set +a
fi

PAINEL_PORTA="${MINIGRID_DASHBOARD_PORT:-8080}"
PYTHON="${MINIGRID_PYTHON:-python3}"
HOST_WORKER_PID=""

trap 'limpar' EXIT

limpar() {
  if [ -n "$HOST_WORKER_PID" ] && kill -0 "$HOST_WORKER_PID" 2>/dev/null; then
    kill "$HOST_WORKER_PID" 2>/dev/null || true
  fi
  docker compose start coord-1 coord-2 worker-a worker-b worker-legacy >/dev/null 2>&1 || true
}

etapa() {
  echo
  echo "────────────────────────────────────────────────────────────────────"
  echo "  $1"
  echo "────────────────────────────────────────────────────────────────────"
  shift
  for linha in "$@"; do echo "  $linha"; done
  echo
}

pausa() {
  if [ "$AUTO" -eq 0 ]; then
    printf "  [ENTER para seguir] "
    read -r _ </dev/tty || true
    echo
  else
    sleep 1
  fi
}

espera_estado() {
  # espera_estado <task_id> <estado...>
  local id="$1"; shift
  for _ in $(seq 1 240); do
    local atual
    atual="$(./grid status "$id" 2>/dev/null | "$PYTHON" -c \
      'import json,sys; print(json.load(sys.stdin)["state"])' 2>/dev/null || echo "?")"
    for alvo in "$@"; do
      [ "$atual" = "$alvo" ] && { echo "$atual"; return 0; }
    done
    sleep 2
  done
  echo "timeout"
  return 1
}

lider() {
  curl -s "http://localhost:8500/v1/kv/minigrid/coordinator/leader?raw" \
    | "$PYTHON" -c 'import json,sys; print(json.load(sys.stdin)["internal"])' 2>/dev/null \
    || echo "nenhum"
}

# ════════════════════════════════════════════════════════════════════ 1
etapa "1. Subida dos componentes e registro no servico de nomes" \
      "O enunciado pede um unico 'docker compose up'. Nenhum passo antes." \
      "Repare que nada aqui informa endereco de worker a ninguem."
docker compose up -d
scripts/wait_healthy.sh 420
docker compose ps --format '{{.Service}}\t{{.Status}}'
echo
echo "  Quem se registrou, e em que ambiente:"
./grid workers
echo
echo "  Abra agora: http://localhost:$PAINEL_PORTA  —  http://localhost:8500/ui"
pausa

# ════════════════════════════════════════════════════════════════════ 2
etapa "2. Submissao por RPC e passagem pela fila" \
      "O cliente chama gRPC e recebe um identificador na hora (assincrono)." \
      "Com os workers parados, a tarefa FICA na fila: desacoplamento no tempo."
docker compose stop worker-a worker-b >/dev/null
echo "  workers genericos parados. Submetendo 5 tarefas..."
IDS=""
for _ in 1 2 3 4 5; do IDS="$IDS $(./grid submit compress --param level=6)"; done
echo "  identificadores:$IDS"
echo
echo "  profundidade da fila (olhe tambem em http://localhost:15672):"
curl -s -u guest:guest "http://localhost:15672/api/queues/%2F/minigrid.tasks" \
  | "$PYTHON" -c 'import json,sys; print("   minigrid.tasks ->", json.load(sys.stdin)["messages_ready"], "mensagens esperando")'
pausa

etapa "2b. Subindo um worker: a fila drena, nada se perdeu"
docker compose start worker-b >/dev/null
for id in $IDS; do printf "   %s -> %s\n" "${id:0:8}" "$(espera_estado "$id" done failed)"; done
pausa

# ════════════════════════════════════════════════════════════════════ 3
etapa "3. Execucao em dois tipos de ambiente, incluindo arquitetura diferente" \
      "Mesma receita de imagem, duas plataformas fixadas no compose." \
      "Em qualquer host, exatamente um dos dois esta emulado."
docker compose start worker-a >/dev/null
sleep 6
./grid bench --count 16 --steps 1
pausa

etapa "3b. O terceiro ambiente: VM de processo, FORA do Docker" \
      "Nao instala nada na maquina: so a biblioteca padrao do Python." \
      "O sistema ja estava funcionando sem ele; agora e absorvido."
scripts/host_worker.sh > /tmp/minigrid-host-worker.log 2>&1 &
HOST_WORKER_PID=$!
for _ in $(seq 1 30); do
  ./grid workers 2>/dev/null | grep -q "process-vm" && break
  sleep 2
done
./grid workers
pausa

etapa "3c. O programa legado embrulhado como servico" \
      "ImageMagick, que nao sabe que o MiniGrid existe, chamado pelo codigo movel."
"$PYTHON" -c "
import base64, pathlib
pathlib.Path('demo-entrada.png').write_bytes(base64.b64decode(
 'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8DwHwAFBQIAX8jx0gAAAABJRU5ErkJggg=='))"
./grid submit img.filter --input demo-entrada.png --output demo-saida.png \
  --param resize=96x96 --wait
"$PYTHON" -c "
import pathlib
p = pathlib.Path('demo-saida.png')
print('  demo-saida.png:', p.stat().st_size, 'bytes | PNG valido:',
      p.read_bytes()[:8] == b'\x89PNG\r\n\x1a\n')"
pausa

# ════════════════════════════════════════════════════════════════════ 4
etapa "4. Migracao de tarefa em andamento apos desligar o no" \
      "Tarefa longa, checkpoint a cada passo. Desligamos o no que a executa." \
      "Ela volta para a fila e OUTRO no retoma do ultimo ponto de verificacao."
TAREFA="$(./grid submit pi --steps 16 --param per_step=40000)"
echo "  tarefa: $TAREFA"
for _ in $(seq 1 90); do
  ATUAL="$(curl -s "http://localhost:8500/v1/kv/minigrid/tasks/$TAREFA/checkpoint?raw" \
           | "$PYTHON" -c 'import json,sys; print(json.load(sys.stdin)["n"])' 2>/dev/null || echo 0)"
  [ "${ATUAL:-0}" -ge 3 ] 2>/dev/null && break
  sleep 1
done
PRIMEIRO="$(./grid status "$TAREFA" | "$PYTHON" -c 'import json,sys; print(json.load(sys.stdin)["worker_id"])')"
echo "  executando em: $PRIMEIRO  (checkpoint no passo $ATUAL)"
echo
case "$PRIMEIRO" in
  *worker-a*)   SERVICO=worker-a ;;
  *worker-b*)   SERVICO=worker-b ;;
  *) SERVICO="" ;;
esac
if [ -n "$SERVICO" ]; then
  echo "  desligando $SERVICO..."
  docker compose stop "$SERVICO" >/dev/null
else
  echo "  a tarefa esta no no de VM de processo; parando-o..."
  kill "$HOST_WORKER_PID" 2>/dev/null || true
  HOST_WORKER_PID=""
fi
echo "  aguardando a retomada em outro no..."
espera_estado "$TAREFA" done failed >/dev/null
./grid status "$TAREFA" | "$PYTHON" -c "
import json,sys
s = json.load(sys.stdin)
print('  concluida por:   ', s['worker_id'], '(' + s['arch'] + '/' + s['virt'] + ')')
print('  retomou do passo:', s['resumed_from_step'], 'de', s['step'], 'totais')
print('  migracoes:       ', s['migrations'])"
pausa

# ════════════════════════════════════════════════════════════════════ 5
etapa "5. Eleicao de novo coordenador apos a falha da replica principal" \
      "'docker kill', nao 'stop': queda abrupta e o caso real." \
      "O failover fica limitado pelo TTL da sessao (~20s), e isso e esperado."
ANTES="$(lider)"
echo "  lider atual: $ANTES"
case "$ANTES" in
  coord-1*) VITIMA=minigrid-coord-1 ;;
  *)        VITIMA=minigrid-coord-2 ;;
esac
echo "  matando $VITIMA..."
docker kill "$VITIMA" >/dev/null
INICIO="$($PYTHON -c 'import time; print(time.monotonic())')"
for _ in $(seq 1 60); do
  DEPOIS="$(lider)"
  [ "$DEPOIS" != "$ANTES" ] && [ "$DEPOIS" != "nenhum" ] && break
  sleep 1
done
"$PYTHON" -c "import time; print('  novo lider: $DEPOIS  em %.1fs' % (time.monotonic() - $INICIO))"
echo
echo "  e o cliente nem percebeu — submetendo sem tratar erro:"
./grid submit pi --steps 2 --wait | tail -3
pausa

# ════════════════════════════════════════════════════════════════════ fim
etapa "Fim do roteiro" \
      "Todos os itens do video foram demonstrados contra o sistema real." \
      "Para zerar a maquina depois: scripts/cleanup.sh"
rm -f demo-entrada.png demo-saida.png
docker compose start coord-1 coord-2 worker-a worker-b worker-legacy >/dev/null 2>&1 || true
echo "  replicas e workers restaurados."
