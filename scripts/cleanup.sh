#!/usr/bin/env bash
# Zera a maquina depois da apresentacao.
#
# Remove conteineres, redes, volumes e as imagens construidas localmente. O que
# o MiniGrid escreveu vive em tres lugares, e todos saem daqui:
#
#   imagens e volumes   dentro da VM do Docker/OrbStack, nao no seu sistema
#   codigo e relatorio  so a pasta do projeto (apague a pasta se quiser)
#   worker do host      nada em disco; so diretorios temporarios por tarefa,
#                       em $TMPDIR, apagados pelo proprio sandbox
#
# Nenhum `brew install`, nenhum `pip install`, nenhum daemon no launchd, nada em
# /usr/local, nada em ~/Library. Nao ha mais nada para limpar.
set -euo pipefail
cd "$(dirname "$0")/.."

echo "derrubando conteineres, rede e volumes..."
docker compose --profile cli down -v --remove-orphans

echo "removendo as imagens construidas para este projeto..."
docker compose --profile cli down --rmi local 2>/dev/null || true
for tag in minigrid-coordinator:test minigrid-coordinator:amd64 \
           minigrid-worker:arm64 minigrid-worker:amd64 minigrid-worker:legacy; do
  docker image rm "$tag" >/dev/null 2>&1 || true
done

echo
echo "pronto. O que sobrou na sua maquina:"
echo "  - a pasta deste projeto (apague-a se quiser remover tudo)"
echo "  - as imagens base do Docker Hub (python:3.12-slim, rabbitmq, consul),"
echo "    que sao cache reaproveitavel; remova com \`docker image prune -a\`"
