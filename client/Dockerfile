# Imagem da aplicacao cliente.
#
# O cliente e conteinerizado porque precisa de `grpcio`, que tem extensao
# compilada, e a meta de zero instalacao na maquina vale para ele tambem. O
# atalho `./minigrid` embrulha `docker compose run --rm client`, entao quem usa
# digita `./minigrid submit pi --wait` e nao percebe o conteiner.
#
# O enunciado nao pede o cliente fora de conteiner; pede que ele nao conheca
# endereco nem quantidade de workers, o que continua verdade.
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

COPY requirements/coordinator.txt /app/requirements/client.txt
RUN pip install --no-cache-dir -r /app/requirements/client.txt

COPY minigrid/ /app/minigrid/
COPY proto/ /app/proto/

RUN python -c "from minigrid import client, __main__; print('cliente ok')"

ENTRYPOINT ["python", "-m", "minigrid"]
CMD ["--help"]
