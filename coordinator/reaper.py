"""Reaper: o terceiro gatilho de migracao (item E).

O enunciado pede que o sistema migre uma tarefa em andamento "quando o no
original for sobrecarregado ou desligado". Dois desses casos o proprio worker
resolve, porque ele esta vivo para isso:

  desligamento  SIGTERM -> devolve a mensagem a fila (nack com requeue)
  sobrecarga    carga acima do limiar -> devolve a mensagem a fila

Falta o caso em que o worker nao tem chance de devolver nada: `docker kill`,
queda de energia, kernel matando o processo. A mensagem fica em estado
*unacked* presa aquele consumidor, a tarefa congela, e ninguem percebe.

E para esse caso que o reaper existe. Ele cruza duas fontes:

  quem detem a posse de cada tarefa   (Consul KV, `tasks/<id>/owner`)
  quem esta vivo no servico de nomes  (Consul, health check por TTL)

Uma tarefa `running` cujo dono desapareceu do servico de nomes e republicada a
partir do ultimo checkpoint. E tambem o mecanismo que cobre o worker do host,
cujo consumo por STOMP tem semantica de ack ligeiramente diferente.

Nao e redundante com o requeue do broker: o broker so reentrega quando a
*conexao* cai, e uma conexao TCP pode ficar aberta e muda por muito tempo. O
TTL do Consul e o relogio independente que resolve isso.
"""
import json


class Reaper(object):
    def __init__(self, consul, config, republish, publish_event=None,
                 worker_service="minigrid-worker"):
        """`republish(task_id, meta, checkpoint)` devolve a tarefa para a fila."""
        self.consul = consul
        self.config = config
        self.republish = republish
        self.publish_event = publish_event or (lambda kind, payload: None)
        self.worker_service = worker_service

    def _live_workers(self):
        try:
            services = self.consul.list_services(self.worker_service)
        except Exception:
            # Consul indisponivel: nao da para distinguir worker morto de
            # servico de nomes fora do ar. Tratar todos como vivos evita
            # republicar o sistema inteiro por causa de um blip.
            return None
        return {s["ID"] for s in services if s.get("Status") == "passing"}

    def tick(self):
        """Uma varredura. Devolve a lista de task_ids republicados."""
        live = self._live_workers()
        if live is None:
            return []

        prefix = self.config.kv_prefix + "tasks/"
        try:
            keys = self.consul.kv_keys(prefix)
        except Exception:
            return []

        republicados = []
        for key in keys:
            if not key.endswith("/owner"):
                continue
            task_id = key[len(prefix):-len("/owner")]
            owner_raw = self.consul.kv_get(key)
            if not owner_raw:
                continue
            owner = owner_raw.decode("utf-8", "replace")
            if owner in live:
                continue

            meta = self._meta(task_id)
            if meta is None:
                continue
            # Tarefa que ja acabou nao volta para a fila, mesmo com posse orfa.
            if meta.get("state") in ("done", "failed"):
                self.consul.kv_delete(key)
                continue
            if meta.get("state") not in ("running", "checkpoint", "received",
                                         "migrated"):
                continue

            checkpoint = self._checkpoint(task_id)
            self.consul.kv_delete(key)
            try:
                self.republish(task_id, meta, checkpoint)
            except Exception:
                continue

            meta["state"] = "migrated"
            meta["migrations"] = int(meta.get("migrations", 0)) + 1
            meta["error"] = ""
            self._write_meta(task_id, meta)
            self.publish_event("migrated", {
                "task_id": task_id,
                "worker_id": owner,
                "arch": meta.get("arch", ""),
                "virt": meta.get("virt", ""),
                "step": int(meta.get("step", 0) or 0),
                "reason": "worker-perdido",
            })
            republicados.append(task_id)
        return republicados

    # ----------------------------------------------------------------- KV

    def _meta(self, task_id):
        raw = self.consul.kv_get(self.config.kv_prefix + "tasks/" + task_id + "/meta")
        if not raw:
            return None
        try:
            return json.loads(raw.decode("utf-8"))
        except ValueError:
            return None

    def _write_meta(self, task_id, meta):
        self.consul.kv_put(
            self.config.kv_prefix + "tasks/" + task_id + "/meta",
            json.dumps(meta, default=str).encode("utf-8"),
        )

    def _checkpoint(self, task_id):
        raw = self.consul.kv_get(
            self.config.kv_prefix + "tasks/" + task_id + "/checkpoint"
        )
        if not raw:
            return None
        try:
            return json.loads(raw.decode("utf-8"))
        except ValueError:
            return None
