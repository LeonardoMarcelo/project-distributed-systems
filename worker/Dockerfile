# Imagem do worker generico (item D: conteineres, o tipo obrigatorio).
#
# Esta mesma imagem sobe duas vezes no compose, com `platform:` diferente:
# `linux/amd64` e `linux/arm64`. Em qualquer host, exatamente um dos dois esta
# emulado — no Apple Silicon e o amd64, no runner x86_64 do CI e o arm64. E
# assim que a exigencia de "ao menos um trabalhador em arquitetura de processador
# diferente" e atendida sem depender da maquina de quem corrige.
#
# Qual interface este ambiente imita, para o relatorio: *chamadas de sistema*. O
# conteiner compartilha o kernel do host e e isolado por namespaces (PID, rede,
# montagem) e limitado por cgroups. O worker emulado acrescenta um nivel: ali o
# *conjunto de instrucoes* tambem e imitado, por QEMU/Rosetta.
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

COPY requirements/worker.txt /app/requirements/worker.txt
RUN pip install --no-cache-dir -r /app/requirements/worker.txt

COPY minigrid/__init__.py minigrid/envelope.py minigrid/naming.py \
     minigrid/stomp.py minigrid/events.py /app/minigrid/
COPY worker/ /app/worker/

# Falha o build, nao o runtime, se algum modulo do worker nao importar.
RUN python -c "from worker import main, executor, sandbox, registry, legacy; \
print('worker ok')"

CMD ["python", "-m", "worker.main"]
