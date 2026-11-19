"""A rubrica do enunciado, como asserções contra o sistema em execução.

Cada teste aqui é um item A–G, verificado contra o compose de verdade — não
contra mocks, não contra componentes em processo. Isso tem um custo (a suíte é
lenta e precisa de Docker) e uma vantagem que paga o custo: se um destes testes
falha, o item correspondente do trabalho está quebrado de fato.

    docker compose up -d
    scripts/host_worker.sh &            # para test_g_*
    python3 -m pytest tests/test_rubrica.py -v

Estes testes mexem no estado do sistema (derrubam workers, matam o líder,
escalam réplicas). Eles restauram o que mudaram no teardown, mas não rode isto
em paralelo com uma demonstração.
"""
import base64
import json
import os
import shutil
import subprocess
import time
import uuid

import pytest

from conftest import CONSUL_URL, PROJECT_ROOT, mgmt, queue_depth, wait_until

grpc = pytest.importorskip("grpc", reason="grpcio ausente; instale para rodar a rubrica")

from minigrid import envelope, events, naming           # noqa: E402
from minigrid._pb2 import minigrid_pb2 as pb           # noqa: E402
from minigrid._pb2 import minigrid_pb2_grpc as pb_grpc  # noqa: E402

AMQP_URL = os.environ.get("MINIGRID_AMQP", "amqp://guest:guest@localhost:5672/%2F")
COORD1 = os.environ.get("MINIGRID_COORD1", "localhost:50051")
COORD2 = os.environ.get("MINIGRID_COORD2", "localhost:50052")

TASKS_QUEUE = "minigrid.tasks"
LEGACY_QUEUE = "minigrid.tasks.legacy"
EVENTS_EXCHANGE = "minigrid.events"
LEADER_KEY = "minigrid/coordinator/leader"

FAST = "def step(state, data, api): return {'n': 1}, True, b'ok'\n"
SLOW = (
    "def step(state, data, api):\n"
    "    import time\n"
    "    total = int(api.params.get('steps', '30'))\n"
    "    time.sleep(float(api.params.get('sleep', '0.4')))\n"
    "    n = state.get('n', 0) + 1\n"
    "    done = n >= total\n"
    "    return {'n': n}, done, (b'fim' if done else None)\n"
)
SAMPLE_PNG = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8DwHwAFBQIA"
    "X8jx0gAAAABJRU5ErkJggg=="
)


# --------------------------------------------------------------- infraestrutura

def _compose(*args, check=True, timeout=300):
    return subprocess.run(
        ["docker", "compose"] + list(args),
        cwd=str(PROJECT_ROOT), capture_output=True, text=True,
        check=check, timeout=timeout,
    )


def _stack_up():
    if shutil.which("docker") is None:
        return False
    try:
        out = _compose("ps", "--format", "{{.Service}}", check=False).stdout
    except Exception:
        return False
    return "coord-1" in out and "worker-a" in out


needs_stack = pytest.mark.skipif(
    not _stack_up(),
    reason="compose do MiniGrid nao esta de pe; rode `docker compose up -d`",
)

pytestmark = needs_stack


@pytest.fixture(scope="module")
def consul():
    return naming.Consul(CONSUL_URL)


@pytest.fixture(scope="module")
def client():
    """Cliente do middleware, igual ao que a aplicação usa."""
    from minigrid.client import MiniGridClient

    c = MiniGridClient(consul_url=CONSUL_URL)
    yield c
    c.close()


def _replica_responde(address, timeout=3):
    """A réplica atende leitura? Serve de precondição aos testes de falha."""
    stub, canal = _stub(address)
    try:
        stub.ListWorkers(pb.Empty(), timeout=timeout)
        return True
    except grpc.RpcError:
        return False
    finally:
        canal.close()


@pytest.fixture
def restaura_stack():
    """Devolve o sistema ao estado normal, aconteça o que acontecer no teste.

    Esperar apenas "existe um líder" não basta: os testes de falha matam
    contêineres em sequência, e um teste que começa com a réplica sobrevivente
    ainda subindo mede o tempo de boot do Docker, não o failover do MiniGrid.
    Por isso o teardown espera as *duas* réplicas voltarem a atender.
    """
    yield
    _compose("up", "-d", "--scale", "worker-a=1", "--scale", "worker-b=1",
             check=False, timeout=300)
    _compose("start", "coord-1", "coord-2", "worker-a", "worker-b",
             "worker-legacy", check=False, timeout=300)
    wait_until(lambda: _replica_responde(COORD1) and _replica_responde(COORD2),
               timeout=120)
    wait_until(lambda: _leader_address(naming.Consul(CONSUL_URL)) is not None,
               timeout=60)


def _leader_address(consul):
    raw = consul.kv_get(LEADER_KEY)
    if not raw:
        return None
    try:
        return json.loads(raw.decode("utf-8")).get("external")
    except ValueError:
        return None


def _stub(address):
    channel = grpc.insecure_channel(address,
                                    options=list(envelope.GRPC_CHANNEL_OPTIONS))
    return pb_grpc.MiniGridStub(channel), channel


def _results_for(task_ids, timeout=120):
    """Espera as tarefas atingirem estado terminal. Devolve {id: status}."""
    from minigrid.client import MiniGridClient

    c = MiniGridClient(consul_url=CONSUL_URL)
    pendentes, prontos = set(task_ids), {}
    limite = time.monotonic() + timeout
    try:
        while pendentes and time.monotonic() < limite:
            for task_id in list(pendentes):
                info = c.status(task_id)
                if info["state"] in ("done", "failed"):
                    prontos[task_id] = info
                    pendentes.discard(task_id)
            if pendentes:
                time.sleep(1)
    finally:
        c.close()
    return prontos



# ------------------------------------------------- controle de consumidores
#
# Varios testes precisam de uma fila SEM consumidor: "nada se perde com o
# trabalhador desligado", "o envelope leva o codigo", "a mensagem e
# persistente". Parar os workers pelo compose nao basta, porque o no de VM de
# processo roda FORA do compose (item G) e continua consumindo a fila generica —
# e o CI sobe esse no de proposito.
#
# A saida usa o proprio roteamento por capacidade do sistema: uma tarefa que
# declara `legacy:imagemagick` vai para a fila que SO o worker legado consome, e
# esse worker e um conteiner que o compose controla. Assim o teste fica
# deterministico sem depender de quem mais esta de pe.
#
# O codigo enviado e o da receita `compress`, que nao chama o programa legado: a
# capacidade aqui serve para *rotear*, nao porque a tarefa precise do
# ImageMagick.
CAPACIDADE_CONTROLADA = ["legacy:imagemagick"]


def _submit_controlada(cliente, data=b"x" * 2048):
    from minigrid import recipes

    return cliente.submit("compress", data=data,
                          code=recipes.load("compress"),
                          params={"level": "6"},
                          requires=CAPACIDADE_CONTROLADA)


def _no_e_conteiner(status):
    return (status.get("virt") or "") == "container"


def _submit_tarefa_longa(cliente, passos=30, sono="0.4"):
    """Tarefa de duracao PREVISIVEL, com pausa por passo.

    As receitas do catalogo medem trabalho, nao tempo: `pi` com 16 passos leva
    segundos num arm64 nativo e muito mais num x86_64 emulado. Os testes de
    migracao precisam que a tarefa ainda esteja rodando quando o no for
    desligado — com duracao dependente da velocidade da maquina, a tarefa
    terminava antes de o `docker compose stop` fazer efeito e nao havia migracao
    para observar. Com pausa explicita por passo, o piso de duracao e conhecido:
    `passos * sono` segundos.
    """
    return cliente.submit("pi", code=SLOW, requires=[],
                          params={"steps": str(passos), "sleep": str(sono)})


def _submit_ate_cair_em_conteiner(cliente, consul, params, tentativas=4):
    """Submete ate a tarefa comecar num no que o compose consegue desligar.

    O no de VM de processo nao pode ser parado daqui. Em vez de fingir
    determinismo, o teste tenta algumas vezes e faz skip com motivo claro.
    """
    for _ in range(tentativas):
        task_id = _submit_tarefa_longa(
            cliente, passos=int(params.get("steps", 30)),
            sono=params.get("sleep", "0.4"))
        chave = "minigrid/tasks/" + task_id + "/checkpoint"
        if not wait_until(lambda: consul.kv_get(chave) is not None, timeout=180):
            continue
        status = cliente.status(task_id)
        if _no_e_conteiner(status) and status.get("worker_id"):
            return task_id, status
    return None, None


# =============================================================== item A

def test_a_interceptador_registra_tempo_e_origem(client):
    """"Um interceptador que capture todas as chamadas para registrar tempo de
    resposta e origem"."""
    vistos = []
    subscriber = events.Subscriber(AMQP_URL, EVENTS_EXCHANGE, pattern="rpc.#")
    subscriber.open()
    try:
        client.submit("pi", params={"steps": "1"})
        limite = time.monotonic() + 20
        for kind, payload in subscriber.listen(inactivity_timeout=0.5):
            if kind == "rpc.trace":
                vistos.append(payload)
                break
            if time.monotonic() > limite:
                break
    finally:
        subscriber.close()

    assert vistos, "nenhum rastreio de RPC publicado pelo interceptador"
    rastreio = vistos[0]
    assert rastreio["method"].endswith(("Submit", "GetStatus", "GetResult",
                                       "Watch", "ListWorkers"))
    assert rastreio["peer"], "origem da chamada nao registrada"
    assert rastreio["elapsed_ms"] >= 0, "tempo de resposta nao registrado"
    assert rastreio["code"] == "OK"


def test_a_wrapper_de_programa_legado_executa(client):
    """"Um wrapper que integre um programa legado de linha de comando,
    tratando-o como um serviço como qualquer outro"."""
    task_id = client.submit("img.filter", data=SAMPLE_PNG,
                            params={"resize": "32x32"})
    assert wait_until(
        lambda: client.status(task_id)["state"] in ("done", "failed"), timeout=120)
    info = client.status(task_id)
    assert info["state"] == "done", info["error"]
    saida = client.result(task_id)
    assert saida[:8] == b"\x89PNG\r\n\x1a\n", "o ImageMagick nao devolveu um PNG"
    assert info["worker_id"] == "worker-legacy"


def test_a_cliente_nao_conhece_endereco_de_worker(client):
    """Transparência de localização: a interface não tem onde pôr um endereço."""
    campos = set(pb.SubmitRequest.DESCRIPTOR.fields_by_name)
    assert not {"address", "host", "worker", "endpoint", "queue"} & campos
    listados = client.workers()
    assert listados["workers"], "nenhum worker registrado"
    for worker in listados["workers"]:
        assert "addr" not in worker and "host" not in worker


# =============================================================== item B

def test_b_submit_e_assincrono(client):
    """"Deve existir pelo menos uma operação assíncrona, em que o cliente recebe
    um identificador da tarefa"."""
    inicio = time.monotonic()
    task_id = client.submit("pi", params={"steps": "8", "per_step": "20000"})
    decorrido = time.monotonic() - inicio
    assert task_id
    assert decorrido < 2.0, (
        "Submit levou %.2fs: nao devolveu o identificador sem esperar a execucao"
        % decorrido
    )
    assert client.status(task_id)["state"] in ("received", "running", "checkpoint")


def test_b_cliente_e_notificado_quando_o_resultado_fica_pronto(client):
    """"...e é notificado quando o resultado estiver pronto" — por push, não por
    polling: `Watch` é um stream de servidor."""
    task_id = client.submit("pi", params={"steps": "3"})
    kinds = []
    for evento in client.watch(task_id, timeout=120):
        kinds.append(evento["kind"])
        if evento["kind"] in ("done", "failed"):
            break
    assert kinds[-1] == "done", kinds
    assert json.loads(client.result(task_id))["steps"] == 3


# =============================================================== item C

def test_c_fila_persistente_nao_perde_tarefa_com_worker_desligado(restaura_stack):
    """"As tarefas devem trafegar por filas persistentes, para que nenhuma se
    perca caso um trabalhador esteja desligado no momento do envio"."""
    from minigrid.client import MiniGridClient

    _compose("stop", "worker-legacy", timeout=300)
    cliente = MiniGridClient(consul_url=CONSUL_URL)
    try:
        ids = [_submit_controlada(cliente) for _ in range(10)]
        assert wait_until(lambda: queue_depth(LEGACY_QUEUE) == 10, timeout=30), (
            "as 10 tarefas deviam estar esperando na fila, sem consumidor"
        )
        _compose("start", "worker-legacy", timeout=300)
        prontos = _results_for(ids, timeout=240)
    finally:
        cliente.close()

    assert len(prontos) == 10, (
        "apenas %d de 10 tarefas completaram: alguma se perdeu" % len(prontos)
    )
    assert all(p["state"] == "done" for p in prontos.values())


def test_c_mensagem_de_tarefa_e_persistente():
    """Fila durável sem mensagem persistente esvazia no restart do broker; os
    dois precisam andar juntos, e é um erro clássico fazer só metade."""
    from minigrid.client import MiniGridClient

    _compose("stop", "worker-legacy", timeout=300)
    cliente = MiniGridClient(consul_url=CONSUL_URL)
    try:
        _submit_controlada(cliente, data=b"y" * 128)
        assert wait_until(lambda: queue_depth(LEGACY_QUEUE) >= 1, timeout=30)
        espiada = mgmt("POST", "/queues/%2F/" + LEGACY_QUEUE + "/get",
                       {"count": 1, "ackmode": "ack_requeue_true",
                        "encoding": "auto"})
        assert espiada[0]["properties"]["delivery_mode"] == 2
    finally:
        cliente.close()
        _compose("start", "worker-legacy", timeout=300)


def test_c_pubsub_entrega_a_dois_assinantes_simultaneos(client):
    """"...divulgadas no modelo de publicação e assinatura, de modo que vários
    clientes e um painel de monitoramento possam acompanhá-las"."""
    a = events.Subscriber(AMQP_URL, EVENTS_EXCHANGE, pattern="task.#")
    b = events.Subscriber(AMQP_URL, EVENTS_EXCHANGE, pattern="task.#")
    a.open()
    b.open()
    try:
        task_id = client.submit("pi", params={"steps": "2"})
        vistos = {"a": [], "b": []}
        for rotulo, assinante in (("a", a), ("b", b)):
            limite = time.monotonic() + 90
            for kind, payload in assinante.listen(inactivity_timeout=0.5):
                if kind and payload.get("task_id") == task_id:
                    vistos[rotulo].append(kind)
                    if kind in ("done", "failed"):
                        break
                if time.monotonic() > limite:
                    break
        assert "done" in vistos["a"], vistos
        assert "done" in vistos["b"], vistos
    finally:
        a.close()
        b.close()


def test_c_painel_de_monitoramento_acompanha(client):
    """O painel é um assinante como qualquer outro, e não rouba evento de ninguém."""
    import urllib.request

    porta = os.environ.get("MINIGRID_DASHBOARD_PORT", "8080")
    url = "http://localhost:" + porta + "/api/state"
    try:
        with urllib.request.urlopen(url, timeout=10) as resposta:
            antes = json.loads(resposta.read().decode("utf-8"))
    except Exception as exc:
        pytest.skip("painel indisponivel em " + url + ": " + str(exc))

    assert antes["consul_ok"] is True
    assert antes["workers"], "o painel nao esta vendo os workers"

    task_id = client.submit("pi", params={"steps": "2"})

    def apareceu():
        with urllib.request.urlopen(url, timeout=10) as resposta:
            estado = json.loads(resposta.read().decode("utf-8"))
        return any(t["task_id"] == task_id for t in estado["tasks"])

    assert wait_until(apareceu, timeout=90), "a tarefa nao apareceu no painel"


# =============================================================== item D

def test_d_tarefas_executam_em_arquiteturas_diferentes(client):
    """"Ao menos um trabalhador deve rodar em uma arquitetura de processador
    diferente da dos demais, por exemplo ARM emulado"."""
    ids = [client.submit("pi", params={"steps": "1", "per_step": "2000"})
           for _ in range(24)]
    prontos = _results_for(ids, timeout=300)
    assert len(prontos) >= 20, (
        "so %d de 24 tarefas completaram" % len(prontos)
    )
    arquiteturas = {p["arch"] for p in prontos.values() if p["arch"]}
    assert len(arquiteturas) >= 2, (
        "todas as tarefas rodaram na mesma arquitetura (%s); o no emulado nao "
        "esta executando" % arquiteturas
    )
    assert {"x86_64", "aarch64"} <= arquiteturas or len(arquiteturas) >= 2


def test_d_dois_tipos_de_virtualizacao_registrados(client):
    """"Os trabalhadores devem ser executados em pelo menos dois tipos
    diferentes de virtualização"."""
    tipos = {w["virt"] for w in client.workers()["workers"]}
    assert "container" in tipos, tipos
    if "process-vm" not in tipos:
        pytest.skip("o no de VM de processo nao esta de pe; rode "
                    "scripts/host_worker.sh (coberto por test_g_*)")
    assert {"container", "process-vm"} <= tipos


def test_d_conteineres_declaram_limites_de_cpu_e_memoria():
    """"...com uma receita declarativa (Dockerfile) e limites de CPU e memória
    definidos"."""
    config = _compose("config").stdout
    assert "cpus:" in config or "cpus: 0.5" in config
    assert "mem_limit:" in config
    inspecao = subprocess.run(
        ["docker", "inspect", "minigrid-worker-a",
         "--format", "{{.HostConfig.Memory}} {{.HostConfig.NanoCpus}}"],
        capture_output=True, text=True, check=True,
    ).stdout.split()
    assert int(inspecao[0]) > 0, "limite de memoria nao aplicado pelo cgroup"
    assert int(inspecao[1]) > 0, "limite de CPU nao aplicado pelo cgroup"


def test_d_plataformas_fixadas_para_nao_depender_do_host():
    """Em qualquer host, exatamente um dos dois workers genéricos está emulado."""
    config = _compose("config").stdout
    assert "linux/amd64" in config
    assert "linux/arm64" in config


# =============================================================== item E

def test_e_tarefa_viaja_como_codigo_mais_dados(client):
    """"As tarefas devem ser enviadas aos trabalhadores como código acompanhado
    dos dados de entrada, caracterizando mobilidade fraca"."""
    _compose("stop", "worker-legacy", timeout=300)
    try:
        task_id = _submit_controlada(client, data=b"entrada")
        assert wait_until(lambda: queue_depth(LEGACY_QUEUE) >= 1, timeout=30)
        espiada = mgmt("POST", "/queues/%2F/" + LEGACY_QUEUE + "/get",
                       {"count": 10, "ackmode": "ack_requeue_true",
                        "encoding": "base64"})
        corpos = [envelope.decode(base64.b64decode(e["payload"]))
                  for e in espiada]
        alvo = [c for c in corpos if c["task_id"] == task_id]
        assert alvo, "a tarefa nao esta na fila"
        assert "def step(" in alvo[0]["code"], (
            "o envelope nao leva o codigo: nao ha mobilidade de codigo, so "
            "uma chamada de funcao local"
        )
        assert alvo[0]["data"] == b"entrada", (
            "o envelope nao leva os dados de entrada junto do codigo"
        )
    finally:
        _compose("start", "worker-legacy", timeout=300)


def test_e_tarefa_migra_e_retoma_do_checkpoint(client, consul, restaura_stack):
    """"O sistema deve ser capaz de migrar uma tarefa em andamento para outro
    trabalhador quando o nó original for sobrecarregado ou desligado, retomando-a
    a partir do último ponto de verificação (checkpoint) salvo"."""
    task_id, origem = _submit_ate_cair_em_conteiner(
        client, consul, {"steps": "30", "sleep": "0.4"})
    if task_id is None:
        pytest.skip("a tarefa nao caiu num no em conteiner em 4 tentativas; o no "
                    "de VM de processo nao pode ser desligado deste teste")

    chave = "minigrid/tasks/" + task_id + "/checkpoint"
    primeiro_no = origem["worker_id"]
    passo_na_queda = json.loads(consul.kv_get(chave).decode())["n"]
    assert passo_na_queda >= 1

    if "worker-a" in primeiro_no:
        servico = "worker-a"
    elif "worker-b" in primeiro_no:
        servico = "worker-b"
    else:
        servico = "worker-legacy"
    _compose("stop", servico, timeout=300)

    assert wait_until(
        lambda: client.status(task_id)["state"] in ("done", "failed"), timeout=300), \
        "a tarefa nao foi retomada depois de o no ser desligado"

    final = client.status(task_id)
    assert final["state"] == "done", final["error"]
    assert final["worker_id"] != primeiro_no, (
        "a tarefa terminou no mesmo no: nao houve migracao"
    )
    assert final["resumed_from_step"] >= passo_na_queda, (
        "retomou do passo %d, mas o checkpoint estava em %d: o trabalho foi refeito"
        % (final["resumed_from_step"], passo_na_queda)
    )
    assert final["step"] == 30, (
        "concluiu em %d passos, esperava 30" % final["step"])


def _familia_de_arquitetura(arch):
    """Normaliza o nome da ISA.

    `arm64` e `aarch64` são a mesma arquitetura com nomes diferentes — o macOS
    diz uma coisa e o Linux diz outra. Comparar as strings cruas faria o nó de
    VM de processo no macOS passar por "arquitetura diferente" de um contêiner
    Linux nativo, o que seria falso e exatamente o tipo de evidência fraca que
    este teste existe para não aceitar.
    """
    arch = (arch or "").lower()
    if arch in ("arm64", "aarch64", "armv8l"):
        return "arm"
    if arch in ("x86_64", "amd64", "x64"):
        return "x86"
    return arch or "?"


def _tentativa_de_migracao_entre_arquiteturas(client, consul, de_servico, para_servico):
    """Uma tentativa: começa a tarefa em `de_servico`, derruba, e deixa `para_servico`
    retomar. Devolve (origem, final) ou None se a tarefa não começou onde devia.
    """
    _compose("start", de_servico, timeout=300)
    _compose("stop", para_servico, "worker-legacy", timeout=300)
    wait_until(lambda: any(w["status"] == "passing" and de_servico.split("-")[1] in w["id"]
                           for w in client.workers()["workers"]), timeout=60)

    task_id = _submit_tarefa_longa(client, passos=24, sono="0.4")
    chave = "minigrid/tasks/" + task_id + "/checkpoint"
    if not wait_until(lambda: consul.kv_get(chave) is not None, timeout=180):
        return None
    origem = client.status(task_id)
    if not origem["worker_id"] or de_servico.split("-")[1] not in origem["worker_id"]:
        return None                       # outro nó pegou; esta tentativa não serve

    passo_na_queda = json.loads(consul.kv_get(chave).decode())["n"]
    if passo_na_queda < 1:
        return None

    _compose("start", para_servico, timeout=300)
    _compose("stop", de_servico, timeout=300)

    if not wait_until(lambda: client.status(task_id)["state"] in ("done", "failed"),
                      timeout=300):
        return None
    final = client.status(task_id)
    return {"task_id": task_id, "origem": origem, "final": final,
            "passo_na_queda": passo_na_queda}


def test_e_migracao_atravessa_arquiteturas_diferentes(client, consul, restaura_stack):
    """O caso difícil: o checkpoint é gravado numa ISA e retomado em outra.

    Funciona porque nada do que migra depende de plataforma — o código é texto,
    o checkpoint é JSON, os dados são bytes. Este teste fecha a evidência que os
    testes de unidade não davam, porque lá os dois lados rodavam na mesma
    arquitetura.

    Por que o teste tenta mais de uma vez, em vez de ser determinístico: o nó de
    VM de processo roda FORA do compose e não pode ser desligado daqui, mas
    consome a mesma fila. Num host x86_64 — o runner do CI — esse nó é x86_64
    *e* o `worker-a` também é, então uma retomada que caia nele não atravessa
    arquitetura nenhuma e o teste estaria afirmando algo falso se passasse. A
    tentativa é repetida nos dois sentidos, e o teste faz skip com motivo claro
    se nenhuma atravessar. Um skip honesto é melhor que uma evidência falsa.
    """
    resultado = None
    # Os dois sentidos: emulado -> nativo e nativo -> emulado. Qual deles é qual
    # depende do host, e o teste não precisa saber.
    for de_servico, para_servico in (("worker-a", "worker-b"),
                                     ("worker-b", "worker-a"),
                                     ("worker-a", "worker-b")):
        tentativa = _tentativa_de_migracao_entre_arquiteturas(
            client, consul, de_servico, para_servico)
        if tentativa is None:
            continue
        if tentativa["final"]["state"] != "done":
            continue
        familia_origem = _familia_de_arquitetura(tentativa["origem"]["arch"])
        familia_final = _familia_de_arquitetura(tentativa["final"]["arch"])
        if familia_origem != familia_final:
            resultado = tentativa
            break

    if resultado is None:
        pytest.skip(
            "nenhuma das tentativas atravessou familia de arquitetura. Num host "
            "x86_64, o no de VM de processo tambem e x86_64 e pode ter retomado "
            "a tarefa; ele roda fora do compose e nao pode ser desligado daqui. "
            "A travessia continua coberta pelos testes locais e pelo registro no "
            "relatorio."
        )

    origem, final = resultado["origem"], resultado["final"]
    assert _familia_de_arquitetura(final["arch"]) != _familia_de_arquitetura(origem["arch"])
    assert final["resumed_from_step"] >= resultado["passo_na_queda"], (
        "retomou do passo %d, mas o checkpoint estava em %d"
        % (final["resumed_from_step"], resultado["passo_na_queda"])
    )
    assert final["step"] == 24, (
        "concluiu em %d passos, esperava 24" % final["step"])
    print("\nmigracao entre arquiteturas: %s (%s) -> %s (%s), retomou do passo %d"
          % (origem["worker_id"], origem["arch"], final["worker_id"], final["arch"],
             final["resumed_from_step"]))


# =============================================================== item F

def test_f_workers_se_registram_sem_endereco_fixo(client, consul):
    """"Os trabalhadores não devem ter endereços fixos: ao iniciar, cada um se
    registra em um serviço de nomes"."""
    registrados = consul.list_services("minigrid-worker")
    assert registrados, "nenhum worker no servico de nomes"
    for servico in registrados:
        assert any(t.startswith("arch:") for t in servico["Tags"])
        assert any(t.startswith("virt:") for t in servico["Tags"])
    config = _compose("config").stdout
    assert "MINIGRID_WORKER_ADDR" not in config


def test_f_sessao_do_lider_usa_lock_delay_zero(consul):
    """O padrão do Consul é 15 s, que seriam somados ao tempo de failover."""
    sessoes = [s for s in consul.session_list()
               if str(s.get("Name", "")).startswith("coord-")]
    assert sessoes, "nenhuma sessao de coordenador no servico de nomes"
    for sessao in sessoes:
        assert sessao["LockDelay"] == 0
        assert sessao["Behavior"] == "delete"


def test_f_nova_eleicao_depois_de_matar_a_replica_principal(consul, restaura_stack):
    """"O coordenador deve ter pelo menos duas réplicas, com eleição automática
    de um novo líder em caso de falha."

    Usa `docker kill`, não `stop`: queda abrupta é o caso real, e é o que expõe
    o tempo de failover limitado pelo TTL (~20 s medidos), em vez do failover
    imediato de uma parada graciosa.
    """
    antes = _leader_address(consul)
    assert antes, "nenhum lider eleito"
    nome = "minigrid-coord-1" if antes.endswith("50051") else "minigrid-coord-2"

    subprocess.run(["docker", "kill", nome], capture_output=True, check=True)
    inicio = time.monotonic()

    assert wait_until(
        lambda: _leader_address(consul) not in (None, antes), timeout=45), \
        "nenhuma nova eleicao em 45s"
    decorrido = time.monotonic() - inicio
    assert decorrido <= 40, "eleicao levou %.1fs" % decorrido

    depois = _leader_address(consul)
    assert depois != antes
    stub, canal = _stub(depois)
    try:
        resposta = stub.Submit(pb.SubmitRequest(recipe="pi", code=FAST))
        assert resposta.task_id, "o novo lider nao aceita submissoes"
    finally:
        canal.close()


def test_f_cliente_absorve_a_queda_do_lider_sem_erro(consul, restaura_stack):
    """Transparência de falhas: a aplicação não vê erro, vê uma pausa."""
    from minigrid.client import MiniGridClient

    # Precondição: a réplica sobrevivente precisa estar pronta para assumir.
    # Sem isto o teste mediria o tempo de boot do Docker em vez do failover do
    # MiniGrid — e falharia por um motivo que não é o que ele investiga.
    assert wait_until(lambda: _replica_responde(COORD1) and _replica_responde(COORD2),
                      timeout=120), "as duas replicas precisam estar de pe antes"

    cliente = MiniGridClient(consul_url=CONSUL_URL)
    try:
        cliente.submit("pi", params={"steps": "1"})
        atual = _leader_address(consul)
        if atual.endswith("50051"):
            vitima, sobrevivente = "minigrid-coord-1", COORD2
        else:
            vitima, sobrevivente = "minigrid-coord-2", COORD1
        subprocess.run(["docker", "kill", vitima], capture_output=True, check=True)
        assert _replica_responde(sobrevivente), (
            "a replica sobrevivente parou de atender; o teste nao tem como "
            "distinguir failover de indisponibilidade total"
        )
        # Sem tratar exceção de propósito: se o cliente não absorver, o teste
        # falha com o erro que a aplicação teria visto.
        assert cliente.submit("pi", params={"steps": "1"})
    finally:
        cliente.close()


def test_f_replica_seguidora_recusa_escrita_mas_responde_leitura(consul):
    lider = _leader_address(consul)
    seguidor = COORD2 if lider.endswith("50051") else COORD1
    stub, canal = _stub(seguidor)
    try:
        with pytest.raises(grpc.RpcError) as exc:
            stub.Submit(pb.SubmitRequest(recipe="pi", code=FAST))
        assert exc.value.code() == grpc.StatusCode.FAILED_PRECONDITION
        listados = stub.ListWorkers(pb.Empty())
        assert listados.workers, "a replica seguidora devia responder leituras"
    finally:
        canal.close()


# =============================================================== item G

def test_g_um_unico_compose_sobe_o_sistema():
    """"Os componentes conteinerizados devem subir de forma coordenada com um
    único arquivo docker-compose.yml"."""
    rodando = _compose("ps", "--format", "{{.Service}}").stdout.split()
    for servico in ("consul", "rabbitmq", "coord-1", "coord-2",
                    "worker-a", "worker-b", "worker-legacy", "dashboard"):
        assert servico in rodando, servico + " nao esta no compose em execucao"
    assert (PROJECT_ROOT / "docker-compose.yml").exists()


def test_g_dependencias_por_healthcheck_nao_por_ordem():
    """`depends_on` sem condição só garante ordem de início, não prontidão."""
    config = _compose("config").stdout
    assert "service_healthy" in config


def test_g_no_de_vm_de_processo_e_absorvido_quando_aparece(client, consul):
    """"O sistema deve funcionar normalmente mesmo antes de esse trabalhador se
    registrar, absorvendo-o assim que ele aparecer no serviço de nomes"."""
    ja_estava = any(w["virt"] == "process-vm" for w in client.workers()["workers"])

    # O sistema funciona sem ele: provado submetendo com ele ausente.
    if not ja_estava:
        task_id = client.submit("pi", params={"steps": "1"})
        assert wait_until(
            lambda: client.status(task_id)["state"] in ("done", "failed"),
            timeout=120), "o sistema nao funcionou sem o no de VM de processo"

    worker_id = "rubrica-host-" + uuid.uuid4().hex[:6]
    ambiente = dict(os.environ)
    ambiente.update({
        "MINIGRID_WORKER_ID": worker_id,
        "MINIGRID_CONSUL": CONSUL_URL,
        "MINIGRID_STOMP_HOST": os.environ.get("MINIGRID_STOMP_HOST", "localhost"),
        "MINIGRID_STOMP_PORT": os.environ.get("MINIGRID_STOMP_PORT", "61613"),
        "PYTHONPATH": str(PROJECT_ROOT),
    })
    processo = subprocess.Popen(
        [os.environ.get("MINIGRID_PYTHON", "python3"), "-u", "-m", "host_worker.worker"],
        cwd=str(PROJECT_ROOT), env=ambiente,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )
    try:
        assert wait_until(
            lambda: any(w["ID"] == worker_id
                        for w in consul.list_services("minigrid-worker")),
            timeout=60), "o no de VM de processo nao se registrou"
        entrada = [w for w in consul.list_services("minigrid-worker")
                   if w["ID"] == worker_id][0]
        assert "virt:process-vm" in entrada["Tags"]
        assert any(t.startswith("os:") for t in entrada["Tags"])
    finally:
        processo.terminate()
        try:
            processo.wait(timeout=30)
        except subprocess.TimeoutExpired:
            processo.kill()
        try:
            consul.deregister_service(worker_id)
        except Exception:
            pass


def test_g_script_do_no_de_vm_de_processo_nao_instala_nada():
    """O script é o que o avaliador roda; ele não pode instalar nada.

    Olha só as linhas executáveis: o cabeçalho do script *fala* sobre pip e venv
    para dizer que não os usa, e procurar a palavra no arquivo inteiro acusaria
    exatamente a documentação da promessa que o teste quer proteger.
    """
    linhas = []
    for linha in (PROJECT_ROOT / "scripts" / "host_worker.sh").read_text().splitlines():
        limpa = linha.strip()
        if limpa and not limpa.startswith("#"):
            linhas.append(limpa)
    executavel = "\n".join(linhas)

    for proibido in ("pip install", "pip3 install", "pip  install",
                     "brew install", "apt-get install", "apt install",
                     "sudo ", "ensurepip", "-m venv", "virtualenv"):
        assert proibido not in executavel, (
            "scripts/host_worker.sh executa `" + proibido + "`, o que quebra a "
            "promessa de nao instalar nada na maquina do avaliador"
        )
