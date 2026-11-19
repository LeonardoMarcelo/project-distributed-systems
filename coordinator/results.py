"""Coleta dos resultados devolvidos pelos workers.

O resultado volta por fila duravel, nao por chamada direta ao coordenador. Duas
razoes: o worker nao precisa saber qual replica e lider no instante em que
termina, e um resultado produzido durante uma troca de lider nao se perde — fica
na fila ate o novo lider consumir.

Onde o blob fica: num volume compartilhado pelas duas replicas, nao no Consul
KV. O KV tem limite de 512 KB por valor e o payload do MiniGrid vai a 8 MB;
tentar guardar resultado no servico de nomes seria usar a ferramenta errada. O
KV guarda o *metadado* — estado, quem executou, em que arquitetura — que e
pequeno e precisa ser consultavel por qualquer replica.

Roda so no lider (ligado e desligado pelo callback da eleicao).
"""
import json
import os
import threading

import pika


class ResultCollector(object):
    def __init__(self, amqp_url, consul, config, blob_dir, publish_event=None):
        self.amqp_url = amqp_url
        self.consul = consul
        self.config = config
        self.blob_dir = blob_dir
        self.publish_event = publish_event or (lambda kind, payload: None)
        self._stop = threading.Event()
        self._thread = None

    # ------------------------------------------------------------ ciclo de vida

    def start(self):
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="coletor-resultado",
                                        daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5)
            self._thread = None

    # ------------------------------------------------------------------- laco

    def _run(self):
        while not self._stop.is_set():
            try:
                self._consume_once()
            except Exception:
                # Broker caiu ou a conexao morreu: espera e reabre. Nao
                # propaga, senao a thread morre e o lider para de coletar sem
                # ninguem notar.
                self._stop.wait(2)

    def _consume_once(self):
        connection = pika.BlockingConnection(pika.URLParameters(self.amqp_url))
        try:
            channel = connection.channel()
            channel.queue_declare(queue=self.config.results_queue, durable=True)
            channel.basic_qos(prefetch_count=1)
            for method, properties, body in channel.consume(
                self.config.results_queue, auto_ack=False, inactivity_timeout=1.0
            ):
                if self._stop.is_set():
                    if method is not None:
                        channel.basic_nack(method.delivery_tag, requeue=True)
                    break
                if method is None:
                    continue
                try:
                    self._handle(body)
                except Exception:
                    # Resultado malformado nao volta para a fila: reentregar
                    # para sempre entupiria o coletor.
                    channel.basic_ack(method.delivery_tag)
                else:
                    channel.basic_ack(method.delivery_tag)
        finally:
            try:
                connection.close()
            except Exception:
                pass

    # --------------------------------------------------------------- aplicacao

    def _handle(self, body):
        message = json.loads(body.decode("utf-8"))
        task_id = message["task_id"]
        meta = message.get("meta") or {}
        state = message.get("state") or "done"

        import base64
        blob = base64.b64decode(message.get("result_b64") or "")
        if self.blob_dir:
            out_dir = os.path.join(self.blob_dir, "out")
            if not os.path.isdir(out_dir):
                os.makedirs(out_dir)
            with open(os.path.join(out_dir, task_id), "wb") as handle:
                handle.write(blob)

        key = self.config.kv_prefix + "tasks/" + task_id + "/meta"
        raw = self.consul.kv_get(key)
        record = json.loads(raw.decode("utf-8")) if raw else {"task_id": task_id}
        record.update({
            "state": state,
            "worker_id": meta.get("worker_id", ""),
            "arch": meta.get("arch", ""),
            "virt": meta.get("virt", ""),
            "step": int(meta.get("steps", meta.get("step", 0)) or 0),
            "resumed_from_step": int(meta.get("resumed_from_step", 0) or 0),
            "result_bytes": len(blob),
            "error": message.get("error", "") or "",
        })
        record["meta"] = {str(k): str(v) for k, v in meta.items()}
        self.consul.kv_put(key, json.dumps(record, default=str).encode("utf-8"))

        # A posse termina com a tarefa; deixa-la para tras faria o reaper
        # enxergar uma tarefa concluida como orfa.
        self.consul.kv_delete(self.config.kv_prefix + "tasks/" + task_id + "/owner")
        self.consul.kv_delete(
            self.config.kv_prefix + "tasks/" + task_id + "/checkpoint"
        )
        return record
