"""Projeção do estado vivo da tarefa no serviço de nomes.

Por que isto existe. O enunciado nomeia tres operacoes do middleware:
"submeter tarefa, consultar estado e obter resultado". As duas pontas
funcionavam, mas *consultar estado* nao: os eventos de ciclo de vida iam para o
exchange de pub/sub, que e transiente por desenho, e nada os gravava. O
resultado pratico era que `GetStatus` saltava de `received` direto para `done` —
com `worker_id` vazio e `step` em zero durante toda a execucao. Quem perguntasse
"onde minha tarefa esta rodando?" nao tinha resposta, e o painel parecia saber
mais que a propria API.

A correcao e projetar os eventos no Consul KV. Quem escreve e o worker que
detem o lock de posse da tarefa (worker/registry.py), ou seja, o unico
autorizado a executa-la — nao ha dois escritores concorrentes. O coletor de
resultados do coordenador escreve o estado terminal depois, e faz merge em vez
de substituir.

Escrita por passo e um custo aceitavel: o valor tem dezenas de bytes e o Consul
e local. O que nao e aceitavel e a API mentir sobre onde a tarefa esta.

Stdlib pura (ver worker/__init__.py).
"""
import json

#: Eventos que valem uma escrita no servico de nomes. `done` e `failed` ficam
#: de fora: quem grava o estado terminal e o coletor de resultados, pelo caminho
#: duravel, e sobrescrever aqui criaria uma corrida entre os dois.
LIFECYCLE = ("running", "checkpoint", "migrated")


def project(consul, kv_prefix, kind, payload):
    """Grava o progresso da tarefa no KV. Nunca levanta.

    Falha de projecao nao pode derrubar a tarefa: o estado terminal ainda chega
    pelo caminho duravel, e perder uma atualizacao de progresso e um problema
    pequeno comparado a perder a execucao.
    """
    if kind not in LIFECYCLE:
        return False
    task_id = payload.get("task_id")
    if not task_id:
        return False

    key = kv_prefix + "tasks/" + task_id + "/meta"
    try:
        raw = consul.kv_get(key)
        record = json.loads(raw.decode("utf-8")) if raw else {"task_id": task_id}
    except Exception:
        record = {"task_id": task_id}

    record["state"] = kind
    for campo in ("worker_id", "arch", "virt", "recipe", "platform"):
        if payload.get(campo):
            record[campo] = payload[campo]
    if payload.get("step") is not None:
        record["step"] = int(payload.get("step") or 0)
    if payload.get("resumed_from_step") is not None:
        record["resumed_from_step"] = int(payload.get("resumed_from_step") or 0)
    if kind == "migrated":
        record["migrations"] = int(record.get("migrations", 0)) + 1
        record["reason"] = payload.get("reason") or ""
    record["updated_at"] = payload.get("at") or 0

    try:
        consul.kv_put(key, json.dumps(record, default=str).encode("utf-8"))
        return True
    except Exception:
        return False
