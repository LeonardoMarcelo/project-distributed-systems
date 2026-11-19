# Imagem do coordenador.
#
# Receita declarativa de conteiner (item D): o ambiente inteiro — distribuicao
# base, versao do interpretador, dependencias — esta descrito aqui, nao montado
# a mao. Isso e o que faz o `docker compose up` do item G funcionar numa maquina
# limpa.
#
# A ordem das camadas e deliberada: requirements antes do codigo, para que
# editar o codigo nao invalide a camada de dependencias no rebuild. E o sistema
# de arquivos em camadas trabalhando a favor.
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

COPY requirements/coordinator.txt /app/requirements/coordinator.txt
RUN pip install --no-cache-dir -r /app/requirements/coordinator.txt

COPY minigrid/ /app/minigrid/
COPY coordinator/ /app/coordinator/
COPY proto/ /app/proto/

# Falha o BUILD, nao o runtime, se o protobuf instalado for mais antigo que o
# gencode dos stubs versionados. Sem esta linha a imagem subia normalmente e
# morria no primeiro import, dentro do compose, com uma VersionError que parece
# erro de codigo e e de pin de dependencia.
RUN python -c "from minigrid._pb2 import minigrid_pb2, minigrid_pb2_grpc; \
from coordinator import server; print('stubs e modulos ok')"

# Volume compartilhado pelas duas replicas: envelopes submetidos e blobs de
# resultado. Nao vao para o Consul KV porque o limite dele e 512 KB por valor e
# o payload do MiniGrid vai a 8 MB.
VOLUME ["/blobs"]

EXPOSE 50051

CMD ["python", "-m", "coordinator.server"]
