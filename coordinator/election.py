"""Eleicao de lider entre as replicas do coordenador (item F).

O enunciado exige "pelo menos duas replicas, com eleicao automatica de um novo
lider em caso de falha", e permite usar uma implementacao pronta. A escolhida e
o Consul, com o padrao de *sessao mais lock de chave*:

1. Cada replica cria uma sessao com TTL e a renova periodicamente.
2. Todas tentam adquirir a mesma chave (`minigrid/coordinator/leader`) usando a
   propria sessao. O Consul garante que exatamente uma consegue.
3. Quem conseguiu grava nela os proprios enderecos e passa a aceitar escritas.
4. Quem nao conseguiu observa a chave com *consulta bloqueante* e tenta
   adquirir no instante em que ela e liberada.

Tres configuracoes importam, e vao explicadas no relatorio:

`LockDelay = 0s` — o padrao do Consul e **15 s**: depois de uma sessao morrer,
ele recusa novas aquisicoes da chave por esse periodo, para evitar que dois
donos se atropelem num particionamento de rede. Somado ao TTL, isso dobraria o
tempo de failover. Zeramos porque o custo que o LockDelay evita (dois lideres
por alguns segundos numa particao) e menor, aqui, que o custo de demorar: o
lider so despacha tarefas para uma fila duravel, e tarefa duplicada e absorvida
pelo lock de posse por tarefa (worker/registry.py).

`Behavior = delete` — quando a sessao morre, o Consul *apaga* a chave travada em
vez de so destravar. E o que permite ao seguidor detectar a mudanca pela
consulta bloqueante e assumir sozinho, sem ninguem avisar.

`TTL = 10s` — numa parada graciosa (SIGTERM) a replica destroi a propria sessao
e o failover e imediato. Numa queda abrupta (`docker kill`), ele e limitado pelo
TTL, e o Consul recolhe sessoes expiradas entre TTL e 2xTTL. Dai a asseracao
honesta de <=30 s na rubrica, em vez de <=10 s.
"""
import json
import threading
import time

DEFAULT_KEY = "minigrid/coordinator/leader"


class LeaderElection(object):
    def __init__(self, consul, name, addresses, key=DEFAULT_KEY, ttl=10,
                 renew_every=5, retry_every=2, wait_seconds=10):
        self.consul = consul
        self.name = name
        self.addresses = dict(addresses)
        self.key = key
        self.ttl = ttl
        self.renew_every = renew_every
        self.retry_every = retry_every
        self.wait_seconds = wait_seconds

        self.session_id = None
        self._leader = False
        self._callbacks = []
        self._stop = threading.Event()
        self._threads = []
        self._lock = threading.Lock()
        self._last_index = 0

    # ---------------------------------------------------------------- estado

    def is_leader(self):
        with self._lock:
            return self._leader

    def current_leader(self):
        """Enderecos do lider atual, lidos do servico de nomes. None se nenhum.

        E daqui que o cliente e o painel descobrem o coordenador — nao de
        configuracao. E a transparencia de localizacao do item A.
        """
        raw = self.consul.kv_get(self.key)
        if not raw:
            return None
        try:
            return json.loads(raw.decode("utf-8"))
        except ValueError:
            return None

    def on_change(self, callback):
        """Registra `callback(is_leader: bool)`, chamado a cada transicao.

        O coordenador usa isso para ligar e desligar o coletor de resultados e o
        reaper, que so devem rodar no lider.
        """
        self._callbacks.append(callback)

    # ------------------------------------------------------------ ciclo de vida

    def start(self):
        self._stop.clear()
        self.session_id = self._create_session()
        self._threads = [
            threading.Thread(target=self._renew_loop, name="eleicao-renova",
                             daemon=True),
            threading.Thread(target=self._campaign_loop, name="eleicao-campanha",
                             daemon=True),
        ]
        for thread in self._threads:
            thread.start()

    def stop(self):
        """Parada graciosa: destroi a sessao, o que libera a chave na hora.

        E a diferenca entre failover imediato e failover limitado pelo TTL.
        """
        self._stop.set()
        session = self.session_id
        self.session_id = None
        if session:
            try:
                self.consul.session_destroy(session)
            except Exception:
                pass
        self._set_leader(False)
        for thread in self._threads:
            thread.join(timeout=2)
        self._threads = []

    # ------------------------------------------------------------------ interno

    def _create_session(self):
        return self.consul.session_create(
            self.name, self.ttl, behavior="delete", lock_delay="0s"
        )

    def _set_leader(self, value):
        with self._lock:
            changed = self._leader != value
            self._leader = value
        if changed:
            for callback in list(self._callbacks):
                try:
                    callback(value)
                except Exception:
                    pass

    def _renew_loop(self):
        """Renova a sessao. Se ela morreu, cria outra e perde a lideranca.

        Perder a sessao e perder a chave (Behavior=delete), entao continuar se
        achando lider seria a pior falha possivel aqui: duas replicas
        despachando como lider.
        """
        while not self._stop.wait(self.renew_every):
            session = self.session_id
            if not session:
                continue
            try:
                self.consul.session_renew(session)
            except Exception:
                self._set_leader(False)
                self._last_index = 0
                try:
                    self.session_id = self._create_session()
                except Exception:
                    self.session_id = None

    def _campaign_loop(self):
        while not self._stop.is_set():
            try:
                self._try_acquire_or_watch()
            except Exception:
                # Consul indisponivel: nao da para saber se somos lider, e
                # agir como se fossemos e pior do que esperar.
                self._set_leader(False)
                self._stop.wait(self.retry_every)

    def _try_acquire_or_watch(self):
        session = self.session_id
        if not session:
            self._stop.wait(self.retry_every)
            return

        body = json.dumps(self.addresses, sort_keys=True).encode("utf-8")
        acquired = self.consul.kv_put(self.key, body, acquire=session)

        if acquired:
            self._set_leader(True)
            # Ja somos lider: fica na consulta bloqueante so para reagir caso a
            # chave desapareca (sessao expirada por falha de renovacao).
            index, value = self.consul.kv_watch(
                self.key, self._last_index, self.wait_seconds
            )
            self._last_index = index
            if value is None:
                self._set_leader(False)
            return

        self._set_leader(False)
        # Nao conseguimos: espera a chave mudar em vez de ficar em laco apertado.
        index, value = self.consul.kv_watch(
            self.key, self._last_index, self.wait_seconds
        )
        self._last_index = index
        if value is None:
            # Chave liberada; tenta adquirir na proxima volta imediatamente.
            return
        self._stop.wait(min(self.retry_every, 1))
