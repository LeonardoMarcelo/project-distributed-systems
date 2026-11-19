"""Declaracao da topologia de filas e exchanges (item C).

Quem declara e o coordenador lider, no boot. Os workers nao declaram nada: eles
assinam o que ja existe, e tentam em laco com recuo ate existir. Essa escolha e
o que faz o item G funcionar — "o sistema deve funcionar normalmente mesmo antes
de esse trabalhador se registrar" — porque a ordem de subida deixa de importar.

Duas naturezas distintas de canal, e a diferenca e proposital:

*Tarefas e resultados* vao em **filas duraveis com mensagens persistentes**.
Sobrevivem a restart do broker e ficam esperando se nenhum worker estiver de pe.
E o desacoplamento no tempo que o enunciado pede: quem envia nao precisa que
quem executa esteja vivo.

*Eventos de estado* vao num **exchange topic com mensagens transientes**. Evento
perdido nao machuca ninguem, porque o estado autoritativo da tarefa esta no
Consul KV; o exchange existe para notificar, nao para guardar.
"""


def declare(channel, config):
    """Cria exchange e filas. Idempotente — pode ser chamada em cada eleicao."""
    channel.exchange_declare(
        exchange=config.events_exchange, exchange_type="topic", durable=True
    )
    for queue in (config.tasks_queue, config.legacy_queue, config.results_queue):
        channel.queue_declare(queue=queue, durable=True)
    return True
