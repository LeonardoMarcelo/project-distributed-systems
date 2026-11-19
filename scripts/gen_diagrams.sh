#!/usr/bin/env bash
# Exporta os diagramas .mmd para PNG.
#
# Roda o mermaid-cli num conteiner descartavel, pelo mesmo motivo de
# scripts/gen_proto.sh: a meta de zero instalacao na maquina vale tambem para as
# ferramentas de autoria. Nada de `npm install -g`.
#
# As fontes `.mmd` sao o artefato versionado; o PNG e derivado. O GitHub
# renderiza `.mmd` nativamente, entao o PNG existe para a entrega em `.zip` e
# para colar no relatorio.
set -euo pipefail
cd "$(dirname "$0")/.."

# Tag FIXADA, e nao `latest`, por um motivo concreto: nas versoes 11.x e
# `latest` a imagem nao traz o Chrome que o mermaid-cli precisa, e o export
# falha com "cache path is incorrectly configured". A 10.9.1 funciona.
IMAGEM="minlag/mermaid-cli:10.9.1"
# `--scale` e o fator do Puppeteer: 3 da um PNG com resolucao suficiente para
# colar no relatorio sem serrilhado. Nao existe `--width` nesta versao do CLI.
ESCALA="${MINIGRID_DIAGRAM_SCALE:-3}"
TEMA="${MINIGRID_DIAGRAM_THEME:-dark}"

if ! docker image inspect "$IMAGEM" >/dev/null 2>&1; then
  echo "baixando $IMAGEM (uma vez)..."
  docker pull -q "$IMAGEM" >/dev/null
fi

falhou=0
for fonte in docs/*.mmd; do
  nome="$(basename "$fonte" .mmd)"
  antes=0
  [ -f "docs/$nome.png" ] && antes=$(date -r "docs/$nome.png" +%s 2>/dev/null || echo 0)
  echo "exportando $fonte -> docs/$nome.png"
  # Sem -u: o Chrome do mermaid-cli precisa de um HOME gravavel, e rodar com o
  # uid do host deixa o crashpad sem onde escrever. O dono dos PNGs e corrigido
  # depois do laco.
  docker run --rm \
    -v "$PWD/docs":/data \
    "$IMAGEM" \
    --input "/data/$nome.mmd" \
    --output "/data/$nome.png" \
    --backgroundColor "#212735" \
    --theme "$TEMA" \
    --scale "$ESCALA" \
    --puppeteerConfigFile /data/.puppeteer.json \
    --quiet || true

  # O mermaid-cli escreve o erro em stderr e sai sem gerar nada. Sem esta
  # checagem o script "termina bem" e o PNG velho segue para o commit — foi
  # exatamente o que aconteceu uma vez.
  depois=0
  [ -f "docs/$nome.png" ] && depois=$(date -r "docs/$nome.png" +%s 2>/dev/null || echo 0)
  if [ "$depois" -le "$antes" ]; then
    echo "  ERRO: docs/$nome.png nao foi regenerado" >&2
    falhou=1
  fi
done

# Os PNGs saem do conteiner como root; devolve ao usuario do host.
if command -v id >/dev/null 2>&1; then
  docker run --rm -v "$PWD/docs":/data --entrypoint sh "$IMAGEM" \
    -c "chown $(id -u):$(id -g) /data/*.png 2>/dev/null || true" >/dev/null 2>&1 || true
fi

if [ "$falhou" -ne 0 ]; then
  echo >&2
  echo "ERRO: um ou mais diagramas nao foram gerados. NAO commite os PNGs." >&2
  exit 1
fi

echo
echo "pronto:"
ls -lh docs/*.png 2>/dev/null | awk '{printf "  %-34s %s\n", $9, $5}'
