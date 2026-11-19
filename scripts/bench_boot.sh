#!/usr/bin/env bash
# Mede o custo de inicializacao de cada ambiente de virtualizacao (item D).
#
# O enunciado pede para "comparar seu custo de inicializacao e isolamento". Esta
# tabela do relatorio vem daqui, medida, nao estimada. Saida em CSV para colar
# direto no documento.
#
#     scripts/bench_boot.sh            # 5 repeticoes
#     scripts/bench_boot.sh 10         # 10 repeticoes
#
# Tres ambientes, tres interfaces imitadas:
#
#   conteiner nativo   chamadas de sistema (kernel compartilhado, namespaces)
#   conteiner emulado  idem + conjunto de instrucoes (QEMU/Rosetta)
#   VM de processo     biblioteca e linguagem (VM de bytecode + stdlib restrita)
set -euo pipefail
cd "$(dirname "$0")/.."

REPS="${1:-5}"
IMAGEM="python:3.12-slim"
PYTHON="${MINIGRID_PYTHON:-python3}"

# Arquitetura nativa do host, para rotular corretamente qual dos dois esta
# emulado — o que inverte entre o Apple Silicon e o runner x86_64 do CI.
NATIVA="$(docker info --format '{{.Architecture}}' 2>/dev/null || uname -m)"
case "$NATIVA" in
  aarch64|arm64) PLAT_NATIVA="linux/arm64"; PLAT_EMULADA="linux/amd64" ;;
  *)             PLAT_NATIVA="linux/amd64"; PLAT_EMULADA="linux/arm64" ;;
esac

echo "# arquitetura nativa deste host: $NATIVA"
echo "# nativa=$PLAT_NATIVA  emulada=$PLAT_EMULADA  repeticoes=$REPS"
echo "ambiente,interface_imitada,repeticao,segundos"

medir_conteiner() {
  local rotulo="$1" interface="$2" plataforma="$3"
  docker pull -q --platform "$plataforma" "$IMAGEM" >/dev/null 2>&1 || true
  for i in $(seq 1 "$REPS"); do
    local t0 t1
    t0=$("$PYTHON" -c 'import time; print(time.monotonic())')
    docker run --rm --platform "$plataforma" "$IMAGEM" python -c 'pass' >/dev/null 2>&1
    t1=$("$PYTHON" -c 'import time; print(time.monotonic())')
    "$PYTHON" -c "print('$rotulo,$interface,$i,%.4f' % ($t1 - $t0))"
  done
}

medir_conteiner "conteiner-nativo"  "chamadas-de-sistema" "$PLAT_NATIVA"
medir_conteiner "conteiner-emulado" "chamadas-de-sistema+ISA" "$PLAT_EMULADA"

# VM de processo: o custo e o do subprocesso `python3 -I -S` mais a aplicacao
# dos limites de recurso e a montagem do espaco de nomes restrito. Mede-se um
# passo trivial, para o numero ser inicializacao e nao computacao.
"$PYTHON" - "$REPS" <<'PY'
import sys
import time

from worker import sandbox

CODE = "def step(state, data, api): return {}, True, b''\n"
reps = int(sys.argv[1])
limites = sandbox.Limits(cpu_seconds=10, wall_seconds=60)

# Uma execucao de aquecimento, descartada: a primeira paga importacao de modulo
# do processo pai, que nao e custo do ambiente.
sandbox.run_step(CODE, {}, b"", limites)

for i in range(1, reps + 1):
    t0 = time.monotonic()
    r = sandbox.run_step(CODE, {}, b"", limites)
    dt = time.monotonic() - t0
    if r.error:
        print("vm-de-processo,biblioteca-e-linguagem,%d,ERRO %s" % (i, r.error[:60]))
    else:
        print("vm-de-processo,biblioteca-e-linguagem,%d,%.4f" % (i, dt))
PY

echo "# fim. Medias por ambiente:"
echo "#   use: scripts/bench_boot.sh | awk -F, 'NR>3 && \$4 ~ /^[0-9]/ {s[\$1]+=\$4; n[\$1]++} END {for (k in s) printf \"#   %-20s %.4fs\\n\", k, s[k]/n[k]}'"
