"""Despacho de tarefas para os workers (item C).

O coordenador nunca fala com um worker. Ele publica numa fila duravel e
esquece — quem consome, quando consome e em que arquitetura executa nao e
problema dele. E dessa indirecao que sai a transparencia de localizacao do item
A: nao existe, em nenhum ponto do caminho, um endereco de worker.

Roteamento por capacidade, nao por endereco: o envelope declara o que precisa
(`requires: ["legacy:imagemagick"]`) e isso escolhe a fila. Tarefa generica vai
para a fila que os tres workers genericos consomem; tarefa que precisa do
programa legado vai para a fila que so o worker legado consome. Trocar qual
worker atende qual capacidade nao exige mudar uma linha do cliente.
"""
import pika


def route(requires, config):
    """Fila de destino, decidida pelas capacidades exigidas."""
    for requirement in requires or []:
        if str(requirement).startswith("legacy:"):
            return config.legacy_queue
    return config.tasks_queue


def submit(channel, raw_envelope, queue, task_id=None):
    """Publica o envelope na fila, de forma persistente.

    `delivery_mode=2` nao e detalhe: sem isso a mensagem vive so na memoria do
    broker, e um restart do RabbitMQ perderia tarefas submetidas. Fila duravel
    sem mensagem persistente sobrevive ao restart vazia — os dois precisam
    andar juntos, e e um erro classico fazer so metade.
    """
    channel.basic_publish(
        exchange="",                      # exchange padrao: roteia pelo nome da fila
        routing_key=queue,
        body=raw_envelope,
        properties=pika.BasicProperties(
            content_type="application/json",
            delivery_mode=2,              # persistente
            message_id=task_id or "",
        ),
    )
    return queue
