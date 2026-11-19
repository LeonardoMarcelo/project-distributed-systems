"""Registro do worker no servico de nomes e posse de tarefa (itens F e E).

O enunciado e explicito: "os trabalhadores nao devem ter enderecos fixos: ao
iniciar, cada um se registra em um servico de nomes". Nada no sistema contem o
endereco de um worker — nem o cliente, nem o coordenador. O worker se anuncia,
diz o que sabe fazer (`cap:`), em que arquitetura roda (`arch:`) e em que tipo
de ambiente virtual vive (`virt:`), e bate um heartbeat enquanto estiver vivo.

Duas responsabilidades, e a segunda e menos obvia:

1. *Presenca* — health check por TTL. Parar de bater o heartbeat faz o Consul
   marcar o servico como critico, e e assim que o reaper do coordenador descobre
   que um worker morreu sem devolver a tarefa que estava executando.

2. *Posse de tarefa* — lock por sessao na chave `tasks/<id>/owner`. Isto resolve
   um problema real: depois de um `nack` com requeue, o worker antigo pode ainda
   estar vivo por alguns instantes. Se os dois gravassem checkpoint e resultado
   da mesma tarefa, o resultado seria corrompido por intercalacao. So o detentor
   do lock escreve.

Uma sutileza que vale explicar, porque e o tipo de coisa que quebra em silencio:
a sessao do worker usa `Behavior=release`, nao `delete`. Com `delete`, a morte do
worker apagaria a chave `owner` junto com o lock — e o reaper, que procura
justamente por tarefas cujo dono desapareceu, nao encontraria nada para
republicar. Com `release`, o lock e liberado mas o valor fica, o reaper ve
`owner=<worker morto>`, cruza com a lista de vivos e republica. A sessao do
*coordenador* usa `delete`, que e o oposto, e por um motivo oposto: lá a chave
precisa desaparecer para o seguidor conseguir adquiri-la.

Stdlib pura (ver worker/__init__.py).
"""
import platform
import threading

WORKER_SERVICE = "minigrid-worker"


class WorkerRegistration(object):
    def __init__(self, consul, worker_id, virt="container", capabilities=None,
                 arch=None, ttl=10, heartbeat_every=4, kv_prefix="minigrid/",
                 service=WORKER_SERVICE, deregister_after="30s", extra_tags=None):
        self.consul = consul
        self.worker_id = worker_id
        self.virt = virt
        self.capabilities = list(capabilities or ["generic"])
        self.arch = arch or platform.machine()
        self.ttl = ttl
        self.heartbeat_every = heartbeat_every
        self.kv_prefix = kv_prefix
        self.service = service
        self.deregister_after = deregister_after
        self.extra_tags = list(extra_tags or [])

        self.session_id = None
        self._stop = threading.Event()
        self._thread = None

    # ---------------------------------------------------------------- tags

    def tags(self):
        tags = ["arch:" + self.arch, "virt:" + self.virt]
        tags += ["cap:" + cap for cap in self.capabilities]
        tags += self.extra_tags
        return tags

    # ------------------------------------------------------------ ciclo de vida

    def start(self):
        self.session_id = self.consul.session_create(
            "worker-" + self.worker_id, self.ttl,
            behavior="release",    # ver a nota no topo do modulo
            lock_delay="0s",
        )
        self.consul.register_service(
            self.worker_id, self.service, self.tags(), self.ttl,
            deregister_after=self.deregister_after,
            meta={"arch": self.arch, "virt": self.virt,
                  "platform": platform.system()},
        )
        self.consul.pass_check(self.worker_id)

        self._stop.clear()
        self._thread = threading.Thread(target=self._heartbeat_loop,
                                        name="heartbeat", daemon=True)
        self._thread.start()
        return self

    def stop(self):
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=3)
            self._thread = None
        try:
            self.consul.deregister_service(self.worker_id)
        except Exception:
            pass
        if self.session_id:
            try:
                self.consul.session_destroy(self.session_id)
            except Exception:
                pass
            self.session_id = None

    def _heartbeat_loop(self):
        while not self._stop.wait(self.heartbeat_every):
            try:
                self.consul.pass_check(self.worker_id)
            except Exception:
                # Consul piscou: tenta de novo no proximo tique. Se a
                # indisponibilidade durar mais que o TTL, o coordenador vai
                # tratar este worker como morto e republicar a tarefa — o que e
                # o comportamento correto, nao um bug.
                pass
            if self.session_id:
                try:
                    self.consul.session_renew(self.session_id)
                except Exception:
                    try:
                        self.session_id = self.consul.session_create(
                            "worker-" + self.worker_id, self.ttl,
                            behavior="release", lock_delay="0s",
                        )
                    except Exception:
                        self.session_id = None

    # ------------------------------------------------------------ posse

    def _owner_key(self, task_id):
        return self.kv_prefix + "tasks/" + task_id + "/owner"

    def acquire_task(self, task_id):
        """Tenta tomar a posse. False significa "outro worker e o dono"."""
        if not self.session_id:
            return False
        return self.consul.kv_put(
            self._owner_key(task_id),
            self.worker_id.encode("utf-8"),
            acquire=self.session_id,
        )

    def release_task(self, task_id):
        """Devolve a posse e apaga a chave.

        Apagar importa: uma chave `owner` orfa faria o reaper enxergar uma
        tarefa viva como abandonada e republica-la sem necessidade.
        """
        try:
            self.consul.kv_delete(self._owner_key(task_id))
        except Exception:
            pass
