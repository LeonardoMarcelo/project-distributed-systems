# MiniGrid Implementation Plan

> **Estado: concluído.** As 17 tarefas foram executadas. O registro do que foi
> decidido durante a execução, incluindo os seis defeitos de produto encontrados
> ao rodar o sistema de verdade, está em `docs/design.md` §12 e no relatório.
> Este documento fica como o plano *original*, para quem quiser comparar o que
> foi planejado com o que foi construído.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Construir o MiniGrid — middleware de execução distribuída de tarefas que cobre integralmente os itens A–G do enunciado de Sistemas Distribuídos, rodando por um único `docker compose up` mais um worker fora do Docker que não instala nada na máquina.

**Architecture:** Três camadas — aplicação cliente, biblioteca de middleware MiniGrid e transporte. O cliente fala gRPC com o coordenador líder, cujo endereço vem do Consul; o coordenador despacha tarefas (código-fonte + dados) por filas duráveis do RabbitMQ; workers heterogêneos consomem, executam em ambiente virtual e publicam transições de estado em pub/sub. Dois dos quatro workers rodam em contêiner com arquitetura de processador fixada (`linux/amd64` e `linux/arm64`, um sempre emulado); um embrulha um CLI legado; o quarto roda fora do Docker numa VM de processo, falando STOMP à mão sobre socket TCP com apenas a biblioteca padrão do Python.

**Tech Stack:** Python 3.12 (contêineres) e 3.9+ (host), gRPC + protobuf, RabbitMQ (AMQP + plugin STOMP), Consul (registro de serviços, health check TTL, KV com lock de sessão), FastAPI + SSE (painel), ImageMagick (programa legado), Docker Compose, pytest, GitHub Actions.

**Spec:** `docs/design.md`

## Global Constraints

- Raiz do projeto: a raiz deste repositorio, que é a raiz do próprio repositório. Todos os caminhos deste plano são relativos a ela e todo comando roda a partir dela. Design e plano vivem em `docs/design.md` e `docs/plan.md`.
- **Zero instalação na máquina física.** Nenhum `brew install`, `pip install`, `sudo`, daemon ou alteração de PATH fora de contêiner.
- **Módulos de stdlib pura** — `minigrid/envelope.py`, `minigrid/naming.py`, `minigrid/stomp.py`, `worker/executor.py`, `worker/sandbox.py`, `worker/legacy.py`, `worker/registry.py` e `host_worker/worker.py` importam **somente a biblioteca padrão** e são compatíveis com **Python 3.9+**: sem `match`, sem `X | Y` em anotação, sem `tomllib`. `minigrid/__init__.py` fica vazio.
- Módulos que podem usar terceiros (rodam só em contêiner ou no runner do CI): `minigrid/client.py`, `minigrid/events.py`, `minigrid/__main__.py`, `coordinator/*`, `worker/main.py`, `dashboard/*`, `tests/*`.
- Contêineres: imagem base `python:3.12-slim`, exceto `worker-legacy`, que precisa do ImageMagick.
- Limite de payload: **8 MB**. Limite de checkpoint: **512 KB** (limite de valor do Consul KV).
- Sessão do Consul: `TTL=10s`, `Behavior=delete`, `LockDelay=0s`, renovação a cada 5 s.
- Health check de worker no Consul: `TTL=10s`, `DeregisterCriticalServiceAfter=30s`, heartbeat a cada 4 s.
- Filas: `durable=True`, mensagens com `delivery_mode=2`, confirmação manual, `prefetch_count=1`.
- Contêineres de worker: `cpus: "0.5"`, `mem_limit: 256m`.
- `platform:` fixado — `worker-a: linux/amd64`, `worker-b: linux/arm64`. Nunca "o nativo e o emulado".
- Nomes de chave do Consul exatamente como no design §7.3; nomes de fila `minigrid.tasks`, `minigrid.tasks.legacy`, `minigrid.results`; exchange `minigrid.events` do tipo `topic`.
- Repositório dedicado e limpo: commits normais na `main`, sem escopo por caminho. O repositório público `LeonardoMarcelo/project-distributed-systems` só é criado ao fim (Task 17), então até lá o histórico é local.
- Trabalho direto na `main` do repositório local, criada na Task 1.

## Review Focus

Classes de entrada que a spec implica e que nenhuma tarefa exercitaria por padrão. Cada linha tem o teste que a fixa, na tarefa que possui o código.

1. **Worker do host importando, por acidente, um módulo que puxa `pika` ou `grpc`** — quebraria na máquina do avaliador, que não instalou nada. Teste em Task 2 e Task 11: subprocesso `python3 -I -S` importa os módulos de stdlib pura com sucesso.
2. **Payload acima de 8 MB** — deve ser rejeitado com erro claro na submissão, não estourar o broker nem o Consul. Teste em Task 9.
3. **Checkpoint acima de 512 KB** — deve falhar de forma legível e marcar a tarefa como `failed`, não corromper silenciosamente nem deixar a tarefa presa. Teste em Task 5.
4. **Receita que não define `step`, ou que lança exceção, ou que tenta `import os`** — deve virar tarefa `failed` com mensagem, nunca derrubar o worker. Testes em Task 4 e Task 5.
5. **`Watch` num id inexistente ou já concluído, e dois clientes no mesmo id** — deve entregar o estado atual e encerrar, não bloquear para sempre. Teste em Task 9.

---

## Estrutura de arquivos

```
project_caio/                     raiz do repo project-distributed-systems
├── README.md                     uso, pré-requisitos, alternativa do item G
├── docker-compose.yml
├── minigrid                      atalho shell → docker compose run --rm client
├── proto/minigrid.proto
├── minigrid/                     biblioteca do middleware
│   ├── __init__.py               vazio (preserva a pureza de stdlib)
│   ├── envelope.py               [stdlib] esquema do envelope de tarefa
│   ├── naming.py                 [stdlib] cliente Consul por HTTP
│   ├── stomp.py                  [stdlib] cliente STOMP sobre socket TCP
│   ├── client.py                 MiniGridClient + interceptador cliente
│   ├── events.py                 publicação/assinatura do pub/sub (pika)
│   ├── __main__.py               CLI da aplicação cliente
│   ├── _pb2/                     stubs gerados, versionados
│   └── recipes/                  pi.py, img_filter.py, compress.py
├── coordinator/
│   ├── Dockerfile
│   ├── interceptor.py            interceptador de servidor gRPC
│   ├── election.py               eleição de líder por sessão do Consul
│   ├── topology.py               declaração de exchanges e filas
│   ├── dispatch.py               roteamento por capacidade
│   ├── results.py                consumo de minigrid.results
│   ├── reaper.py                 TTL expirado → republica tarefa
│   └── server.py                 servidor gRPC + servicer
├── worker/
│   ├── Dockerfile
│   ├── Dockerfile.legacy
│   ├── sandbox.py                [stdlib] VM de processo
│   ├── executor.py               [stdlib] laço de passos + checkpoint
│   ├── legacy.py                 [stdlib] wrapper do ImageMagick
│   ├── registry.py               [stdlib] registro e heartbeat no Consul
│   └── main.py                   consumidor AMQP
├── host_worker/worker.py         [stdlib] worker fora do Docker
├── dashboard/
│   ├── Dockerfile
│   ├── app.py
│   └── static/index.html
├── scripts/                      host_worker.sh, cleanup.sh, bench_boot.sh, demo.sh
├── tests/
│   ├── conftest.py
│   ├── test_envelope.py  test_naming.py  test_stomp.py
│   ├── test_sandbox.py   test_executor.py  test_recipes.py
│   ├── test_election.py  test_coordinator.py  test_purity.py
│   └── test_rubrica.py           os itens A–G como asserções
├── docs/                         diagrama + relatório técnico
└── .github/workflows/ci.yml
```

---

### Task 1: Base de infraestrutura e spike do STOMP

Primeiro passo deliberado: provar que `NACK` com `requeue:true` por STOMP reentrega numa fila `/amq/queue/` **antes** de construir o worker do host em cima disso. Se falhar, o plano B (vendorizar o `pika`) entra aqui, sem retrabalho.

**Files:**
- Create: `.gitignore`
- Create: `docker-compose.yml` (só `consul` e `rabbitmq` nesta tarefa)
- Create: `rabbitmq/enabled_plugins`
- Create: `scripts/spike_stomp.py` (descartável, removido na Task 3)

**Interfaces:**
- Consumes: nada
- Produces: `consul` em `:8500`, `rabbitmq` em `:5672`, `:15672`, `:61613`; rede `minigrid_net`

- [ ] **Step 1: Inicializar o repositório local**

`.gitignore` cobre `__pycache__/`, `*.pyc`, `.pytest_cache/`, `blobs/`, `*.png` gerado em teste. Sem `.env` — este projeto não tem segredo nenhum, e o repositório vai ser público.

```bash
git init -b main
git add .gitignore && git commit -m "chore: inicializa o repositorio do MiniGrid"
```

- [ ] **Step 2: Escrever o compose com `consul` e `rabbitmq`**

`consul`: imagem `hashicorp/consul:1.19`, comando `agent -dev -client=0.0.0.0 -ui`, portas `8500:8500`, healthcheck `consul members`.
`rabbitmq`: imagem `rabbitmq:3.13-management`, portas `5672:5672`, `15672:15672`, `61613:61613`, monta `./rabbitmq/enabled_plugins` em `/etc/rabbitmq/enabled_plugins`, healthcheck `rabbitmq-diagnostics check_running`.
`enabled_plugins` contém: `[rabbitmq_management,rabbitmq_stomp].`

- [ ] **Step 3: Subir e verificar**

Run: `docker compose up -d && docker compose ps`
Expected: ambos `healthy`. `curl -s localhost:8500/v1/status/leader` devolve endereço entre aspas; `curl -s -u guest:guest localhost:15672/api/overview` devolve JSON.

- [ ] **Step 4: Escrever o spike**

`scripts/spike_stomp.py`, stdlib pura: declara a fila durável `spike.q` pela API HTTP de gerenciamento; abre socket em `localhost:61613`; envia `CONNECT`, `SEND` para `/amq/queue/spike.q`, `SUBSCRIBE` com `ack:client`; recebe `MESSAGE`; responde `NACK` com `requeue:true`; verifica que a mesma mensagem chega de novo; então `ACK`. Imprime `REQUEUE OK` ou `REQUEUE FAIL`.

- [ ] **Step 5: Rodar o spike**

Run: `python3 scripts/spike_stomp.py`
Expected: `REQUEUE OK`.
Se `REQUEUE FAIL`: parar, registrar no design §5.2 que o plano B foi acionado, e trocar `minigrid/stomp.py` por `vendor/pika` nas Tasks 3 e 11 antes de continuar.

- [ ] **Step 6: Commit**

```bash
git add docker-compose.yml rabbitmq/enabled_plugins scripts/spike_stomp.py
git commit -m "feat(minigrid): base consul+rabbitmq e spike de requeue por STOMP"
```

---

### Task 2: Envelope de tarefa e cliente Consul (stdlib pura)

**Files:**
- Create: `minigrid/__init__.py` (vazio), `minigrid/envelope.py`, `minigrid/naming.py`
- Test: `tests/test_envelope.py`, `tests/test_naming.py`, `tests/test_purity.py`, `tests/conftest.py`

**Interfaces:**
- Consumes: `consul` em `:8500` (Task 1)
- Produces:
  - `envelope.encode(task_id: str, recipe: str, code: str, data: bytes, params: dict, requires: list) -> bytes`
  - `envelope.decode(raw: bytes) -> dict` com chaves `task_id, recipe, code, data, params, requires, submitted_at`
  - `envelope.MAX_PAYLOAD = 8 * 1024 * 1024`; `envelope.PayloadTooLarge(Exception)`
  - `naming.Consul(base_url: str)` com métodos:
    `register_service(service_id, name, tags, ttl_seconds) -> None`,
    `pass_check(service_id) -> None`, `deregister_service(service_id) -> None`,
    `list_services(name) -> list[dict]` (cada um com `ID`, `Tags`, `Status`),
    `kv_put(key, value: bytes, acquire: str = None) -> bool`,
    `kv_get(key) -> bytes | None`, `kv_delete(key) -> None`,
    `session_create(name, ttl_seconds, behavior, lock_delay) -> str`,
    `session_renew(session_id) -> None`, `session_destroy(session_id) -> None`,
    `kv_watch(key, index: int, wait_seconds: int) -> tuple` devolvendo `(novo_index, valor_ou_None)`

- [ ] **Step 1: Escrever os testes do envelope**

```python
def test_encode_decode_roundtrip():
    raw = envelope.encode("t1", "pi", "def step(...): pass", b"\x00\x01", {"steps": "20"}, [])
    out = envelope.decode(raw)
    assert out["task_id"] == "t1" and out["recipe"] == "pi"
    assert out["data"] == b"\x00\x01" and out["params"]["steps"] == "20"

def test_encode_rejects_oversized_payload():
    with pytest.raises(envelope.PayloadTooLarge):
        envelope.encode("t1", "pi", "", b"x" * (envelope.MAX_PAYLOAD + 1), {}, [])
```

- [ ] **Step 2: Escrever os testes do cliente Consul** (precisam do Consul da Task 1 de pé)

```python
def test_register_and_list_service(consul):
    consul.register_service("w-test", "minigrid-worker", ["arch:test", "virt:test"], 10)
    ids = [s["ID"] for s in consul.list_services("minigrid-worker")]
    assert "w-test" in ids

def test_kv_put_get_delete(consul):
    consul.kv_put("minigrid/test/k", b"v")
    assert consul.kv_get("minigrid/test/k") == b"v"
    consul.kv_delete("minigrid/test/k")
    assert consul.kv_get("minigrid/test/k") is None

def test_only_one_session_acquires_the_same_key(consul):
    s1 = consul.session_create("a", 10, "delete", "0s")
    s2 = consul.session_create("b", 10, "delete", "0s")
    assert consul.kv_put("minigrid/test/lock", b"1", acquire=s1) is True
    assert consul.kv_put("minigrid/test/lock", b"2", acquire=s2) is False

def test_destroying_session_releases_the_key(consul):
    s1 = consul.session_create("a", 10, "delete", "0s")
    consul.kv_put("minigrid/test/lock2", b"1", acquire=s1)
    consul.session_destroy(s1)
    s2 = consul.session_create("b", 10, "delete", "0s")
    assert consul.kv_put("minigrid/test/lock2", b"2", acquire=s2) is True
```

`conftest.py` expõe a fixture `consul` apontando para `http://localhost:8500` e limpando `minigrid/test/` no teardown.

- [ ] **Step 3: Escrever o teste de pureza de stdlib** (Review Focus 1)

```python
STDLIB_ONLY = ["minigrid.envelope", "minigrid.naming"]

def test_stdlib_only_modules_import_without_site_packages():
    mods = ",".join(STDLIB_ONLY)
    r = subprocess.run(
        [sys.executable, "-I", "-S", "-c", f"import {mods}"],
        cwd=PROJECT_ROOT, capture_output=True, text=True,
    )
    assert r.returncode == 0, r.stderr
```

- [ ] **Step 4: Rodar os testes e confirmar que falham**

Run: `python3 -m pytest tests/test_envelope.py tests/test_naming.py tests/test_purity.py -v`
Expected: FAIL com `ModuleNotFoundError: No module named 'minigrid.envelope'`

- [ ] **Step 5: Implementar `minigrid/envelope.py`**

Serialização JSON com `data` em base64 (bytes não são JSON). O limite é checado sobre `data` antes de codificar.

- [ ] **Step 6: Implementar `minigrid/naming.py`**

`urllib.request` sobre a API HTTP do Consul. `kv_put` com `acquire` devolve o corpo `true`/`false` interpretado como booleano. `kv_get` decodifica o `Value` em base64 e devolve `None` em HTTP 404. `kv_watch` usa `?index=N&wait=Ns` e lê o cabeçalho `X-Consul-Index`. Todo método levanta exceção com corpo da resposta em erro inesperado.

- [ ] **Step 7: Rodar os testes e confirmar que passam**

Run: `python3 -m pytest tests/test_envelope.py tests/test_naming.py tests/test_purity.py -v`
Expected: PASS

- [ ] **Step 8: Commit**

```bash
git add minigrid tests
git commit -m "feat(minigrid): envelope de tarefa e cliente Consul em stdlib pura"
```

---

### Task 3: Cliente STOMP sobre socket TCP (stdlib pura)

Substitui o spike da Task 1 pela implementação de verdade.

**Files:**
- Create: `minigrid/stomp.py`
- Delete: `scripts/spike_stomp.py`
- Test: `tests/test_stomp.py`
- Modify: `tests/test_purity.py` (acrescentar `minigrid.stomp` a `STDLIB_ONLY`)

**Interfaces:**
- Consumes: `rabbitmq` em `:61613` (Task 1)
- Produces: `stomp.StompClient(host, port, login, passcode)` com
  `connect() -> None`, `send(destination: str, body: bytes, persistent: bool = True) -> None`,
  `subscribe(destination: str, ack: str = "client") -> None`,
  `receive(timeout: float) -> Frame | None` onde `Frame` tem `.command`, `.headers`, `.body`,
  `ack(frame) -> None`, `nack(frame, requeue: bool = True) -> None`, `close() -> None`

- [ ] **Step 1: Escrever os testes**

```python
def test_send_and_receive_then_ack(stomp, amqp_queue):
    stomp.send(f"/amq/queue/{amqp_queue}", b"hello")
    stomp.subscribe(f"/amq/queue/{amqp_queue}")
    frame = stomp.receive(timeout=5)
    assert frame.body == b"hello"
    stomp.ack(frame)
    assert stomp.receive(timeout=1) is None

def test_nack_with_requeue_redelivers_the_same_message(stomp, amqp_queue):
    stomp.send(f"/amq/queue/{amqp_queue}", b"again")
    stomp.subscribe(f"/amq/queue/{amqp_queue}")
    first = stomp.receive(timeout=5)
    stomp.nack(first, requeue=True)
    second = stomp.receive(timeout=5)
    assert second is not None and second.body == b"again"
    stomp.ack(second)

def test_body_with_null_byte_survives_the_frame(stomp, amqp_queue):
    payload = b"a\x00b\x00c"
    stomp.send(f"/amq/queue/{amqp_queue}", payload)
    stomp.subscribe(f"/amq/queue/{amqp_queue}")
    frame = stomp.receive(timeout=5)
    assert frame.body == payload
    stomp.ack(frame)
```

A fixture `amqp_queue` declara uma fila durável de nome aleatório pela API HTTP de gerenciamento e a remove no teardown.

- [ ] **Step 2: Rodar e confirmar falha**

Run: `python3 -m pytest tests/test_stomp.py -v`
Expected: FAIL com `ModuleNotFoundError: No module named 'minigrid.stomp'`

- [ ] **Step 3: Implementar `minigrid/stomp.py`**

Frames são `COMANDO\ncabeçalho:valor\n\ncorpo\x00`. Decisões que os testes fixam: `heart-beat:0,0` no `CONNECT` para não precisar de thread de heartbeat; `content-length` obrigatório em todo `SEND` e lido em todo `MESSAGE`, que é o que faz o terceiro teste passar — sem ele o byte nulo no corpo corta o frame. `persistent=True` manda `persistent:true`. `receive` usa `socket.settimeout` e devolve `None` no estouro. `nack` envia `NACK` com `id` do cabeçalho `ack` do frame e `requeue:true`.

- [ ] **Step 4: Rodar e confirmar passagem**

Run: `python3 -m pytest tests/test_stomp.py tests/test_purity.py -v`
Expected: PASS

- [ ] **Step 5: Remover o spike e commitar**

```bash
git rm scripts/spike_stomp.py
git add minigrid/stomp.py tests
git commit -m "feat(minigrid): cliente STOMP em stdlib pura sobre socket TCP"
```

---

### Task 4: VM de processo — interpretador isolado

Este é o segundo tipo de virtualização do item D e o ambiente do item G. O isolamento precisa ser demonstrável, não alegado.

**Files:**
- Create: `worker/__init__.py` (vazio), `worker/sandbox.py`
- Test: `tests/test_sandbox.py`
- Modify: `tests/test_purity.py` (acrescentar `worker.sandbox`)

**Interfaces:**
- Consumes: nada
- Produces:
  - `sandbox.run_step(code: str, state: dict, data: bytes, limits: Limits, legacy_allowed: bool) -> StepResult`
  - `sandbox.Limits` com `cpu_seconds: int = 10`, `address_space_mb: int = 256`, `file_size_mb: int = 16`, `processes: int = 8`
  - `sandbox.StepResult` com `state: dict`, `done: bool`, `result: bytes | None`, `error: str | None`
  - `sandbox.ALLOWED_IMPORTS: frozenset` — `{"math", "random", "json", "base64", "zlib", "hashlib", "struct", "time"}`

- [ ] **Step 1: Escrever os testes**

```python
PI_STEP = "def step(state, data, api):\n    n = state.get('n', 0) + 1\n    return {'n': n}, n >= 3, (b'done' if n >= 3 else None)"

def test_runs_one_step_and_returns_new_state():
    r = sandbox.run_step(PI_STEP, {}, b"", sandbox.Limits(), False)
    assert r.error is None and r.state == {"n": 1} and r.done is False

def test_signals_completion_and_result():
    r = sandbox.run_step(PI_STEP, {"n": 2}, b"", sandbox.Limits(), False)
    assert r.done is True and r.result == b"done"

def test_blocks_import_outside_the_allowlist():
    r = sandbox.run_step("import os\ndef step(s, d, a): return {}, True, b''", {}, b"", sandbox.Limits(), False)
    assert r.error is not None and "os" in r.error

def test_allows_import_inside_the_allowlist():
    r = sandbox.run_step("import math\ndef step(s, d, a): return {'v': math.floor(1.5)}, True, b''", {}, b"", sandbox.Limits(), False)
    assert r.error is None and r.state == {"v": 1}

def test_code_without_step_fails_as_task_error():       # Review Focus 4
    r = sandbox.run_step("x = 1", {}, b"", sandbox.Limits(), False)
    assert r.error is not None and "step" in r.error

def test_exception_inside_step_fails_as_task_error():   # Review Focus 4
    r = sandbox.run_step("def step(s, d, a): raise ValueError('boom')", {}, b"", sandbox.Limits(), False)
    assert r.error is not None and "boom" in r.error

def test_cpu_limit_terminates_an_infinite_loop():
    r = sandbox.run_step("def step(s, d, a):\n    while True: pass", {}, b"",
                         sandbox.Limits(cpu_seconds=1), False)
    assert r.error is not None

def test_memory_limit_terminates_a_runaway_allocation():
    r = sandbox.run_step("def step(s, d, a):\n    b = bytearray(10**9)\n    return {}, True, b''",
                         {}, b"", sandbox.Limits(address_space_mb=64), False)
    assert r.error is not None

def test_working_directory_is_removed_after_the_step(tmp_path):
    r = sandbox.run_step(
        "def step(s, d, a):\n    import os\n    return {'cwd': os.getcwd()}, True, b''",
        {}, b"", sandbox.Limits(), False)
    assert r.error is not None   # 'os' bloqueado: a lista branca vale também aqui
```

- [ ] **Step 2: Rodar e confirmar falha**

Run: `python3 -m pytest tests/test_sandbox.py -v`
Expected: FAIL com `ModuleNotFoundError: No module named 'worker.sandbox'`

- [ ] **Step 3: Implementar `worker/sandbox.py`**

O passo roda num subprocesso `[sys.executable, "-I", "-S", "-c", RUNNER]`, com `state`, `data` e `code` entregues pelo stdin em JSON e o resultado lido do stdout em JSON — o que mantém o processo pai limpo de qualquer coisa que o código enviado faça. No filho, antes de carregar o código: `resource.setrlimit` para `RLIMIT_CPU`, `RLIMIT_AS`, `RLIMIT_FSIZE`, `RLIMIT_NPROC`; `os.chdir` para um diretório temporário; e um import hook em `sys.meta_path` que levanta `ImportError` para qualquer nome fora de `ALLOWED_IMPORTS`. O código é carregado com `__builtins__` reduzido a lista branca. `legacy_allowed=True` acrescenta `run_legacy` ao objeto `api`. O pai apaga o diretório temporário no `finally` e converte qualquer saída não-JSON, código de saída diferente de zero ou estouro de `subprocess` timeout em `StepResult(error=...)`.

`resource` não existe no Windows: capturar `ImportError` no filho, seguir sem limites e devolver o aviso em `StepResult.error` apenas se o passo também falhar.

- [ ] **Step 4: Rodar e confirmar passagem**

Run: `python3 -m pytest tests/test_sandbox.py tests/test_purity.py -v`
Expected: PASS (9 testes)

- [ ] **Step 5: Commit**

```bash
git add worker tests
git commit -m "feat(minigrid): VM de processo com interpretador isolado e limites de recurso"
```

---

### Task 5: Executor com checkpoint e retomada

**Files:**
- Create: `worker/executor.py`
- Test: `tests/test_executor.py`
- Modify: `tests/test_purity.py` (acrescentar `worker.executor`)

**Interfaces:**
- Consumes: `sandbox.run_step`, `sandbox.Limits`, `sandbox.StepResult` (Task 4)
- Produces:
  - `executor.Transport` — protocolo com `publish_event(kind: str, payload: dict) -> None`, `save_checkpoint(task_id: str, state: dict) -> None`, `load_checkpoint(task_id: str) -> dict | None`, `send_result(task_id: str, result: bytes, meta: dict) -> None`
  - `executor.run_task(envelope: dict, transport: Transport, worker_id: str, limits, checkpoint_every: int = 1, should_stop: Callable[[], bool] = ...) -> str` devolvendo o estado final: `"done"`, `"failed"` ou `"yielded"`
  - `executor.MAX_CHECKPOINT = 512 * 1024`

- [ ] **Step 1: Escrever os testes** (com um `FakeTransport` que guarda eventos, checkpoints e resultados em memória)

```python
def test_runs_to_completion_and_sends_result(fake):
    assert executor.run_task(env(PI_STEP, steps=3), fake, "w1", L) == "done"
    assert fake.results[0][0] == b"done"
    assert [e[0] for e in fake.events] == ["running", "checkpoint", "checkpoint", "checkpoint", "done"]

def test_resumes_from_the_saved_checkpoint(fake):
    fake.checkpoints["t1"] = {"n": 2}
    executor.run_task(env(PI_STEP, task_id="t1"), fake, "w1", L)
    assert fake.events[0][1]["resumed_from_step"] == 2

def test_checkpoint_written_every_n_steps(fake):
    executor.run_task(env(PI_STEP, steps=4), fake, "w1", L, checkpoint_every=2)
    assert len([e for e in fake.events if e[0] == "checkpoint"]) == 2

def test_should_stop_yields_and_keeps_the_checkpoint(fake):
    assert executor.run_task(env(PI_STEP, steps=10), fake, "w1", L,
                             should_stop=after_n_calls(2)) == "yielded"
    assert fake.checkpoints["t1"]["n"] == 2
    assert fake.events[-1][0] == "migrated"

def test_step_error_marks_the_task_failed(fake):               # Review Focus 4
    assert executor.run_task(env("def step(s,d,a): raise ValueError('boom')"), fake, "w1", L) == "failed"
    assert fake.events[-1][0] == "failed" and "boom" in fake.events[-1][1]["error"]

def test_oversized_checkpoint_fails_the_task_readably(fake):   # Review Focus 3
    big = "def step(s, d, a): return {'blob': 'x' * 600000}, False, None"
    assert executor.run_task(env(big), fake, "w1", L) == "failed"
    assert "checkpoint" in fake.events[-1][1]["error"].lower()

def test_worker_id_and_arch_travel_in_the_result_meta(fake):
    executor.run_task(env(PI_STEP, steps=3), fake, "w1", L)
    meta = fake.results[0][1]
    assert meta["worker_id"] == "w1" and meta["arch"] == platform.machine()
```

- [ ] **Step 2: Rodar e confirmar falha**

Run: `python3 -m pytest tests/test_executor.py -v`
Expected: FAIL com `ModuleNotFoundError: No module named 'worker.executor'`

- [ ] **Step 3: Implementar `worker/executor.py`**

Laço: carrega checkpoint (ou `{}`), publica `running` com `resumed_from_step`, chama `sandbox.run_step` repetidamente, publica `checkpoint` e persiste a cada `checkpoint_every`, consulta `should_stop()` entre passos. O tamanho do checkpoint serializado é verificado contra `MAX_CHECKPOINT` **antes** de persistir; estouro vira `failed` com mensagem citando "checkpoint". `meta` do resultado traz `worker_id`, `arch` (`platform.machine()`), `virt`, `steps` e `resumed_from_step`.

- [ ] **Step 4: Rodar e confirmar passagem**

Run: `python3 -m pytest tests/test_executor.py tests/test_purity.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add worker/executor.py tests
git commit -m "feat(minigrid): executor com checkpoint, retomada e migração por yield"
```

---

### Task 6: Receitas de código móvel e wrapper do programa legado

**Files:**
- Create: `minigrid/recipes/__init__.py`, `recipes/pi.py`, `recipes/img_filter.py`, `recipes/compress.py`, `worker/legacy.py`
- Test: `tests/test_recipes.py`
- Modify: `tests/test_purity.py` (acrescentar `worker.legacy`)

**Interfaces:**
- Consumes: `sandbox.run_step` (Task 4), `executor.run_task` (Task 5)
- Produces:
  - `recipes.load(name: str) -> str` devolvendo o código-fonte da receita
  - `recipes.CATALOG: dict[str, dict]` com `requires` e `default_params` por receita
  - `legacy.run_legacy(program: str, args: list, stdin: bytes, workdir: str) -> bytes` levantando `legacy.LegacyFailed` com stderr em código de saída diferente de zero
  - `legacy.resolve_imagemagick() -> list` devolvendo `["convert"]` ou `["magick", "convert"]`

- [ ] **Step 1: Escrever os testes**

```python
def test_pi_converges_within_tolerance(fake):
    executor.run_task(env(recipes.load("pi"), params={"steps": "20", "per_step": "5000"}), fake, "w1", L)
    assert abs(json.loads(fake.results[0][0])["pi"] - 3.14159) < 0.05

def test_pi_is_deterministic_given_the_same_state():
    a = sandbox.run_step(recipes.load("pi"), {"n": 3, "inside": 10, "total": 20}, b"", L, False)
    b = sandbox.run_step(recipes.load("pi"), {"n": 3, "inside": 10, "total": 20}, b"", L, False)
    assert a.state == b.state

def test_compress_roundtrips(fake):
    executor.run_task(env(recipes.load("compress"), data=b"x" * 10000), fake, "w1", L)
    assert zlib.decompress(fake.results[0][0]) == b"x" * 10000

def test_img_filter_requires_the_legacy_capability():
    assert "legacy:imagemagick" in recipes.CATALOG["img.filter"]["requires"]

def test_run_legacy_returns_converted_bytes(tmp_path, sample_png):
    out = legacy.run_legacy(legacy.resolve_imagemagick()[0],
                            legacy.resolve_imagemagick()[1:] + ["-", "-resize", "32x32", "png:-"],
                            sample_png, str(tmp_path))
    assert out[:8] == b"\x89PNG\r\n\x1a\n"

def test_run_legacy_raises_with_stderr_on_failure(tmp_path):
    with pytest.raises(legacy.LegacyFailed) as e:
        legacy.run_legacy("false", [], b"", str(tmp_path))
    assert isinstance(e.value.stderr, str)
```

`test_run_legacy_*` marcados com `pytest.mark.skipif` quando o ImageMagick não existe localmente — no CI e no contêiner legado ele existe.

- [ ] **Step 2: Rodar e confirmar falha**

Run: `python3 -m pytest tests/test_recipes.py -v`
Expected: FAIL

- [ ] **Step 3: Implementar as três receitas**

`pi.py`: Monte Carlo com `random.Random(seed)` semeado a partir de `state["n"]`, para a determinação exigida pelo segundo teste; `per_step` amostras por passo, `steps` passos; resultado é JSON com `pi`, `inside`, `total`.
`compress.py`: um passo, `zlib.compress(data)`.
`img_filter.py`: um passo, chama `api.run_legacy` com os argumentos de redimensionamento vindos de `params`.
`recipes.load` lê o `.py` ao lado e devolve o texto; `CATALOG` declara `requires: ["legacy:imagemagick"]` só para `img.filter`.

- [ ] **Step 4: Implementar `worker/legacy.py`**

`subprocess.run` com lista de argumentos, `shell=False`, `cwd=workdir`, `input=stdin`, `capture_output=True`, `timeout`. `resolve_imagemagick` testa `convert` e cai para `magick convert`.

- [ ] **Step 5: Rodar e confirmar passagem**

Run: `python3 -m pytest tests/test_recipes.py tests/test_purity.py -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add minigrid/recipes worker/legacy.py tests
git commit -m "feat(minigrid): receitas de codigo movel e wrapper do ImageMagick"
```

---

### Task 7: Interface gRPC, stubs e interceptador de servidor

**Files:**
- Create: `proto/minigrid.proto`, `minigrid/_pb2/` (gerado e versionado), `coordinator/__init__.py`, `coordinator/interceptor.py`, `scripts/gen_proto.sh`
- Test: `tests/test_interceptor.py`, `tests/test_proto_sync.py`

**Interfaces:**
- Consumes: nada
- Produces:
  - `minigrid.proto` com o serviço do design §7.2 e as mensagens `SubmitRequest{recipe, code, data, params, requires}`, `SubmitReply{task_id}`, `TaskId{task_id}`, `TaskStatus{task_id, state, worker_id, arch, step, error}`, `TaskResult{task_id, state, result, meta}`, `TaskEvent{task_id, kind, at, worker_id, arch, step, error}`, `WorkerList{workers}`, `Worker{id, arch, virt, status, tags}`, `Empty{}`
  - `interceptor.TraceInterceptor(publish: Callable[[str, dict], None])` — `grpc.ServerInterceptor`
  - cada chamada publica `rpc.trace` com `method`, `peer`, `elapsed_ms`, `code`

- [ ] **Step 1: Escrever o teste do interceptador**

```python
def test_interceptor_records_method_peer_elapsed_and_code(grpc_server_with_echo):
    traces = grpc_server_with_echo.traces
    grpc_server_with_echo.stub.GetStatus(TaskId(task_id="x"))
    assert len(traces) == 1
    kind, payload = traces[0]
    assert kind == "rpc.trace"
    assert payload["method"].endswith("GetStatus")
    assert payload["peer"].startswith("ipv")
    assert payload["elapsed_ms"] >= 0 and payload["code"] == "OK"

def test_interceptor_records_the_code_of_a_failing_call(grpc_server_with_echo):
    with pytest.raises(grpc.RpcError):
        grpc_server_with_echo.stub.GetResult(TaskId(task_id="missing"))
    assert grpc_server_with_echo.traces[-1][1]["code"] == "NOT_FOUND"
```

- [ ] **Step 2: Escrever o teste de sincronia dos stubs**

```python
def test_versioned_stubs_match_the_proto(tmp_path):
    subprocess.run([... grpc_tools.protoc into tmp_path ...], check=True)
    for name in ("minigrid_pb2.py", "minigrid_pb2_grpc.py"):
        assert (tmp_path / name).read_text() == (PB2_DIR / name).read_text(), \
            f"{name} dessincronizado: rode scripts/gen_proto.sh"
```

- [ ] **Step 3: Rodar e confirmar falha**

Run: `python3 -m pytest tests/test_interceptor.py tests/test_proto_sync.py -v`
Expected: FAIL

- [ ] **Step 4: Escrever o `.proto` e gerar os stubs**

`scripts/gen_proto.sh` roda `python -m grpc_tools.protoc` dentro de um contêiner `python:3.12-slim` descartável (`docker run --rm -v`), para não exigir `grpcio-tools` na máquina. Saída em `minigrid/_pb2/`, com `__init__.py` e correção do import relativo que o `protoc` gera.

- [ ] **Step 5: Implementar `coordinator/interceptor.py`**

`grpc.ServerInterceptor.intercept_service` embrulha o `behavior` para cronometrar, ler `context.peer()` e capturar o código de status, inclusive quando o handler aborta. Publica pelo `publish` injetado, nunca direto no broker — é o que torna o teste possível sem RabbitMQ.

- [ ] **Step 6: Rodar e confirmar passagem**

Run: `python3 -m pytest tests/test_interceptor.py tests/test_proto_sync.py -v`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add proto minigrid/_pb2 coordinator scripts/gen_proto.sh tests
git commit -m "feat(minigrid): interface gRPC, stubs versionados e interceptador de servidor"
```

---

### Task 8: Eleição de líder por sessão do Consul

**Files:**
- Create: `coordinator/election.py`
- Test: `tests/test_election.py`

**Interfaces:**
- Consumes: `naming.Consul` (Task 2)
- Produces:
  - `election.LeaderElection(consul, name: str, addresses: dict, ttl: int = 10, renew_every: int = 5)`
  - `start() -> None` (thread de renovação), `stop() -> None` (destrói a sessão), `is_leader() -> bool`,
    `on_change(callback: Callable[[bool], None]) -> None`, `current_leader() -> dict | None`
  - chave `minigrid/coordinator/leader`, valor JSON `{"internal": ..., "external": ...}`

- [ ] **Step 1: Escrever os testes**

```python
def test_only_one_of_two_replicas_becomes_leader(consul):
    a, b = make_election(consul, "coord-1"), make_election(consul, "coord-2")
    a.start(); b.start()
    assert [a.is_leader(), b.is_leader()].count(True) == 1

def test_follower_takes_over_when_leader_stops_gracefully(consul):
    a, b = make_election(consul, "coord-1"), make_election(consul, "coord-2")
    a.start(); b.start()
    leader, follower = (a, b) if a.is_leader() else (b, a)
    leader.stop()
    assert wait_until(follower.is_leader, timeout=5)

def test_leader_publishes_both_addresses(consul):
    a = make_election(consul, "coord-1", addresses={"internal": "coord-1:50051", "external": "localhost:50051"})
    a.start()
    assert a.current_leader()["external"] == "localhost:50051"

def test_on_change_fires_when_leadership_is_acquired(consul):
    seen = []
    a = make_election(consul, "coord-1"); a.on_change(seen.append); a.start()
    assert wait_until(lambda: seen == [True], timeout=5)

def test_session_uses_zero_lock_delay(consul):
    a = make_election(consul, "coord-1"); a.start()
    info = consul.session_info(a.session_id)
    assert info["LockDelay"] == 0
```

`session_info(session_id) -> dict` é um método novo em `naming.Consul`; acrescentar junto com o teste correspondente em `tests/test_naming.py`.

- [ ] **Step 2: Rodar e confirmar falha**

Run: `python3 -m pytest tests/test_election.py -v`
Expected: FAIL

- [ ] **Step 3: Implementar `coordinator/election.py`**

Cria sessão com `TTL=10s`, `Behavior=delete`, `LockDelay=0s`. Thread daemon renova a cada 5 s e, quando não é líder, faz `kv_watch` bloqueante na chave e tenta adquirir assim que ela é liberada. `stop()` destrói a sessão, o que libera a chave na hora e dá failover imediato na parada graciosa.

- [ ] **Step 4: Rodar e confirmar passagem**

Run: `python3 -m pytest tests/test_election.py tests/test_naming.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add coordinator/election.py minigrid/naming.py tests
git commit -m "feat(minigrid): eleicao de lider por sessao do Consul com LockDelay zero"
```

---

### Task 9: Coordenador — servidor gRPC, topologia, despacho, resultados e reaper

**Files:**
- Create: `coordinator/topology.py`, `dispatch.py`, `results.py`, `reaper.py`, `server.py`, `coordinator/Dockerfile`, `minigrid/events.py`
- Test: `tests/test_coordinator.py`

**Interfaces:**
- Consumes: `envelope` (2), `naming.Consul` (2), `interceptor.TraceInterceptor` e stubs (7), `election.LeaderElection` (8)
- Produces:
  - `topology.declare(channel) -> None` — exchange `minigrid.events` (topic, durável) e filas duráveis `minigrid.tasks`, `minigrid.tasks.legacy`, `minigrid.results`
  - `events.Publisher(url)` com `publish(kind, payload)` para `minigrid.events` com routing key `task.<kind>` ou `rpc.trace`
  - `events.Subscriber(url, pattern="#")` com `listen() -> Iterator[tuple]`
  - `dispatch.route(requires: list) -> str` devolvendo o nome da fila
  - `dispatch.submit(channel, env: bytes, queue: str) -> None` com `delivery_mode=2`
  - `results.ResultCollector(channel, consul, blob_dir)` com `run() -> None`
  - `reaper.Reaper(consul, channel)` com `tick() -> list` devolvendo os ids republicados
  - `server.serve(...)` — servidor gRPC na porta 50051 com o `TraceInterceptor` instalado

- [ ] **Step 1: Escrever os testes**

```python
def test_submit_returns_a_task_id_quickly(coord):
    t0 = time.monotonic()
    reply = coord.stub.Submit(SubmitRequest(recipe="pi", code=PI_STEP, data=b"", params={"steps": "3"}))
    assert reply.task_id and (time.monotonic() - t0) < 0.2

def test_submit_rejects_payload_over_the_limit(coord):          # Review Focus 2
    with pytest.raises(grpc.RpcError) as e:
        coord.stub.Submit(SubmitRequest(recipe="compress", code="", data=b"x" * (8 * 1024 * 1024 + 1)))
    assert e.value.code() == grpc.StatusCode.INVALID_ARGUMENT

def test_submitted_task_lands_on_the_generic_queue(coord):
    coord.stub.Submit(SubmitRequest(recipe="pi", code=PI_STEP, params={"steps": "3"}))
    assert queue_depth("minigrid.tasks") == 1

def test_task_requiring_legacy_lands_on_the_legacy_queue(coord):
    coord.stub.Submit(SubmitRequest(recipe="img.filter", code="...", requires=["legacy:imagemagick"]))
    assert queue_depth("minigrid.tasks.legacy") == 1

def test_get_status_of_an_unknown_task_is_not_found(coord):
    with pytest.raises(grpc.RpcError) as e:
        coord.stub.GetStatus(TaskId(task_id="nope"))
    assert e.value.code() == grpc.StatusCode.NOT_FOUND

def test_watch_of_an_already_finished_task_yields_current_state_and_ends(coord):   # Review Focus 5
    tid = finish_a_task(coord)
    events = list(coord.stub.Watch(TaskId(task_id=tid)))
    assert events[-1].kind == "done"

def test_watch_of_an_unknown_task_ends_instead_of_hanging(coord):                  # Review Focus 5
    with pytest.raises(grpc.RpcError) as e:
        list(coord.stub.Watch(TaskId(task_id="nope")))
    assert e.value.code() == grpc.StatusCode.NOT_FOUND

def test_two_clients_watching_the_same_task_both_receive_the_transitions(coord):   # Review Focus 5
    tid = coord.stub.Submit(...).task_id
    a, b = watch_in_thread(tid), watch_in_thread(tid)
    publish_transition(tid, "done")
    assert "done" in a.kinds() and "done" in b.kinds()

def test_only_the_leader_accepts_submissions(two_coords):
    follower = two_coords.follower
    with pytest.raises(grpc.RpcError) as e:
        follower.stub.Submit(SubmitRequest(recipe="pi", code=PI_STEP))
    assert e.value.code() == grpc.StatusCode.FAILED_PRECONDITION

def test_result_collector_stores_the_blob_and_marks_the_task_done(coord, blob_dir):
    tid = coord.stub.Submit(...).task_id
    publish_result(tid, b"OUT", {"worker_id": "w1", "arch": "x86_64"})
    assert wait_until(lambda: coord.stub.GetResult(TaskId(task_id=tid)).result == b"OUT")

def test_reaper_republishes_a_task_whose_owner_vanished(coord, consul):
    tid = start_task_with_owner(coord, consul, worker="ghost")
    consul.deregister_service("ghost")
    assert tid in wait_for(lambda: coord.reaper.tick(), nonempty=True)
```

- [ ] **Step 2: Rodar e confirmar falha**

Run: `python3 -m pytest tests/test_coordinator.py -v`
Expected: FAIL

- [ ] **Step 3: Implementar `minigrid/events.py` e `coordinator/topology.py`**

`pika.BlockingConnection`. O `Publisher` reabre a conexão em `AMQPConnectionError` e nunca propaga erro de publicação de evento para o caminho da chamada — evento perdido é transiente por desenho (design §7.9), tarefa perdida não.

- [ ] **Step 4: Implementar `dispatch.py`, `results.py`, `reaper.py`**

`route` devolve `minigrid.tasks.legacy` se `requires` contém qualquer `legacy:*`, senão `minigrid.tasks`. `ResultCollector` grava o blob em `blob_dir/<task_id>` e atualiza `minigrid/tasks/<id>/meta`. `Reaper.tick` lista os serviços `minigrid-worker`, compara com os donos registrados em `minigrid/tasks/*/owner` e republica as tarefas cujo dono não está mais ativo.

- [ ] **Step 5: Implementar `coordinator/server.py`**

Servicer com os cinco RPCs. `Submit` valida o tamanho (`INVALID_ARGUMENT`), exige liderança (`FAILED_PRECONDITION`), grava `meta` no Consul e despacha. `Watch` é gerador: valida existência (`NOT_FOUND`), emite o estado atual, e só então segue o `Subscriber` filtrando pelo `task_id`, encerrando em `done` ou `failed` — é o que faz os três testes de Review Focus 5 passarem. `serve` instala o `TraceInterceptor` com o `publish` do `Publisher`, sobe a `LeaderElection` e roda `ResultCollector` e `Reaper` em threads só enquanto líder.

- [ ] **Step 6: Escrever o `coordinator/Dockerfile`**

`python:3.12-slim`, instala `grpcio`, `pika`; copia `minigrid/` e `coordinator/`; `CMD ["python", "-m", "coordinator.server"]`.

- [ ] **Step 7: Rodar e confirmar passagem**

Run: `python3 -m pytest tests/test_coordinator.py -v`
Expected: PASS

- [ ] **Step 8: Commit**

```bash
git add coordinator minigrid/events.py tests
git commit -m "feat(minigrid): coordenador com gRPC, despacho, coleta de resultado e reaper"
```

---

### Task 10: Worker em contêiner

**Files:**
- Create: `worker/registry.py`, `worker/main.py`, `worker/Dockerfile`, `worker/Dockerfile.legacy`
- Test: `tests/test_worker.py`
- Modify: `tests/test_purity.py` (acrescentar `worker.registry`)

**Interfaces:**
- Consumes: `executor.run_task` e `Transport` (5), `naming.Consul` (2), `events.Publisher` (9), `legacy` (6)
- Produces:
  - `registry.WorkerRegistration(consul, worker_id, arch, virt, caps, ttl=10, heartbeat_every=4)` com `start()`, `stop()`, `acquire_task(task_id) -> bool`, `release_task(task_id)`
  - `main.AmqpTransport(channel, consul, publisher)` implementando `executor.Transport`
  - `main.run(...)` — laço de consumo com `prefetch_count=1`, confirmação manual e handler de SIGTERM

- [ ] **Step 1: Escrever os testes**

```python
def test_worker_registers_with_arch_and_virt_tags(consul, worker_proc):
    tags = [s["Tags"] for s in consul.list_services("minigrid-worker") if s["ID"] == worker_proc.id][0]
    assert any(t.startswith("arch:") for t in tags) and "virt:container" in tags

def test_worker_consumes_a_task_and_publishes_done(worker_proc, submit):
    tid = submit("pi", steps=3)
    assert wait_for_event(tid, "done", timeout=30)

def test_two_workers_do_not_both_own_the_same_task(consul):
    r1, r2 = make_registration(consul, "w1"), make_registration(consul, "w2")
    assert [r1.acquire_task("t9"), r2.acquire_task("t9")].count(True) == 1

def test_sigterm_mid_task_requeues_and_publishes_migrated(worker_proc, submit):
    tid = submit("pi", steps=30)
    wait_for_event(tid, "running", timeout=30)
    worker_proc.terminate()                  # SIGTERM
    assert wait_for_event(tid, "migrated", timeout=30)
    assert queue_depth("minigrid.tasks") == 1

def test_another_worker_resumes_the_requeued_task_from_the_checkpoint(two_workers, submit):
    tid = submit("pi", steps=30)
    wait_for_event(tid, "running", timeout=30)
    two_workers.kill_the_one_running(tid)
    ev = wait_for_event(tid, "running", timeout=60, after_migration=True)
    assert ev["resumed_from_step"] > 0 and ev["worker_id"] != two_workers.first_id

def test_overload_threshold_yields_the_task_back_to_the_queue(worker_proc_overloaded, submit):
    tid = submit("pi", steps=30)
    assert wait_for_event(tid, "migrated", timeout=30)
    assert last_event(tid, "migrated")["reason"] == "overload"
```

- [ ] **Step 2: Rodar e confirmar falha**

Run: `python3 -m pytest tests/test_worker.py -v`
Expected: FAIL

- [ ] **Step 3: Implementar `worker/registry.py`**

Registro com `TTL=10s` e `DeregisterCriticalServiceAfter=30s`; thread de heartbeat a cada 4 s. `acquire_task` faz `kv_put("minigrid/tasks/<id>/owner", worker_id, acquire=session)` — é o lock que impede execução dupla após requeue.

- [ ] **Step 4: Implementar `worker/main.py`**

`AmqpTransport` mapeia `Transport` para `Publisher` e `Consul`. O laço: consome com `prefetch_count=1`, tenta `acquire_task` (se falhar, `ack` e descarta — outro worker é o dono), chama `executor.run_task` passando `should_stop` que devolve verdadeiro quando SIGTERM foi recebido **ou** quando a carga passa do limiar `MINIGRID_OVERLOAD_THRESHOLD`. Retorno `"done"`/`"failed"` → `basic_ack`; `"yielded"` → `basic_nack(requeue=True)`. `release_task` no `finally`.

- [ ] **Step 5: Escrever os dois Dockerfiles**

`Dockerfile`: `python:3.12-slim` + `pika` + `minigrid/` e `worker/`. `Dockerfile.legacy`: o mesmo mais `apt-get install -y --no-install-recommends imagemagick`, e `MINIGRID_CAPS=legacy:imagemagick`.

- [ ] **Step 6: Rodar e confirmar passagem**

Run: `python3 -m pytest tests/test_worker.py tests/test_purity.py -v`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add worker tests
git commit -m "feat(minigrid): worker em conteiner com registro, posse e migracao por requeue"
```

---

### Task 11: Worker do host — fora do Docker, stdlib pura

**Files:**
- Create: `host_worker/worker.py`, `scripts/host_worker.sh`
- Test: `tests/test_host_worker.py`
- Modify: `tests/test_purity.py` (acrescentar `host_worker.worker`)

**Interfaces:**
- Consumes: `stomp.StompClient` (3), `naming.Consul` (2), `executor.run_task` (5), `sandbox` (4), `registry.WorkerRegistration` (10)
- Produces: `worker.StompTransport(stomp_client, consul)` implementando `executor.Transport`; `worker.main()` lendo `MINIGRID_CONSUL`, `MINIGRID_STOMP_HOST`, `MINIGRID_STOMP_PORT`

- [ ] **Step 1: Escrever os testes**

```python
def test_host_worker_imports_with_no_third_party_packages():     # Review Focus 1
    r = subprocess.run([sys.executable, "-I", "-S", "-c", "import host_worker.worker"],
                       cwd=PROJECT_ROOT, capture_output=True, text=True)
    assert r.returncode == 0, r.stderr

def test_host_worker_registers_itself_as_process_vm(consul, host_worker_proc):
    tags = [s["Tags"] for s in consul.list_services("minigrid-worker")
            if s["ID"] == host_worker_proc.id][0]
    assert "virt:process-vm" in tags

def test_host_worker_waits_for_the_queue_instead_of_crashing(consul, broker_without_topology):
    p = start_host_worker()
    time.sleep(3)
    assert p.poll() is None          # ainda vivo, tentando

def test_host_worker_executes_a_task_end_to_end(host_worker_proc, submit):
    tid = submit("pi", steps=3)
    assert wait_for_event(tid, "done", timeout=60)
    assert last_event(tid, "done")["virt"] == "process-vm"
```

- [ ] **Step 2: Rodar e confirmar falha**

Run: `python3 -m pytest tests/test_host_worker.py -v`
Expected: FAIL

- [ ] **Step 3: Implementar `host_worker/worker.py`**

Mesma forma do `worker/main.py`, trocando AMQP por `StompClient`: assina `/amq/queue/minigrid.tasks` com `ack:client`; `StompTransport.publish_event` faz `SEND` para `/exchange/minigrid.events/task.<kind>`; `send_result` faz `SEND` para `/amq/queue/minigrid.results`; checkpoint vai no Consul KV. Registra-se com `virt:process-vm` e `arch:<platform.machine()>`. Na subida, tenta assinar em laço com recuo até a fila existir (design §7.5). `done`/`failed` → `ack`; `yielded` → `nack(requeue=True)`.

- [ ] **Step 4: Escrever `scripts/host_worker.sh`**

Verifica `python3` e avisa com mensagem clara se faltar; checa que `localhost:8500` e `localhost:61613` respondem e orienta a rodar `docker compose up` antes; então `exec python3 -m host_worker.worker` da raiz do projeto. **Sem `pip`, sem venv, sem `sudo`.**

- [ ] **Step 5: Rodar e confirmar passagem**

Run: `python3 -m pytest tests/test_host_worker.py tests/test_purity.py -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add host_worker scripts/host_worker.sh tests
git commit -m "feat(minigrid): worker do host em stdlib pura por STOMP, sem instalar nada"
```

---

### Task 12: Compose completo, CLI do cliente e README

**Files:**
- Modify: `docker-compose.yml` (de 2 para 9 serviços)
- Create: `minigrid/client.py`, `minigrid/__main__.py`, `client/Dockerfile`, `minigrid` (atalho shell), `scripts/cleanup.sh`, `README.md`
- Test: `tests/test_client.py`

**Interfaces:**
- Consumes: tudo das tasks anteriores
- Produces:
  - `client.MiniGridClient(consul_url)` com `submit(recipe, data=b"", params=None, code=None) -> str`, `status(task_id) -> dict`, `result(task_id) -> bytes`, `watch(task_id) -> Iterator[dict]`, `workers() -> list`
  - `__main__.py` — subcomandos `submit`, `status`, `result`, `watch`, `workers`
  - serviços `consul`, `rabbitmq`, `coord-1`, `coord-2`, `worker-a`, `worker-b`, `worker-legacy`, `dashboard`, `client`

- [ ] **Step 1: Escrever os testes do cliente**

```python
def test_client_resolves_the_leader_without_being_given_an_address(stack):
    c = MiniGridClient(CONSUL)
    assert c.submit("pi", params={"steps": "3"})

def test_client_survives_the_leader_being_killed_mid_session(stack):
    c = MiniGridClient(CONSUL)
    c.submit("pi", params={"steps": "3"})
    stack.kill_leader()
    assert c.submit("pi", params={"steps": "3"})      # transparência de falhas

def test_client_interceptor_measures_end_to_end_time(stack):
    c = MiniGridClient(CONSUL)
    c.submit("pi", params={"steps": "3"})
    assert c.last_call["elapsed_ms"] >= 0 and c.last_call["method"].endswith("Submit")

def test_watch_delivers_the_result_without_polling(stack):
    c = MiniGridClient(CONSUL)
    tid = c.submit("pi", params={"steps": "3"})
    assert [e["kind"] for e in c.watch(tid)][-1] == "done"
```

- [ ] **Step 2: Rodar e confirmar falha**

Run: `python3 -m pytest tests/test_client.py -v`
Expected: FAIL

- [ ] **Step 3: Implementar `minigrid/client.py`**

Resolve o líder por `naming.Consul.kv_get("minigrid/coordinator/leader")`, escolhe `external` ou `internal` por `MINIGRID_INSIDE_DOCKER`, abre o canal com o interceptador cliente instalado (injeta `x-minigrid-client-id`, grava `last_call`). Em `UNAVAILABLE` ou `FAILED_PRECONDITION`, relê a chave e tenta de novo com recuo exponencial, até `MINIGRID_MAX_RETRIES=5` — é daqui que sai a transparência de falhas do segundo teste. `submit` sem `code` carrega o código por `recipes.load(recipe)` e os `requires` por `recipes.CATALOG`.

- [ ] **Step 4: Implementar o CLI e o atalho**

`minigrid/__main__.py` com `argparse`. O atalho `./minigrid` é uma linha: `exec docker compose run --rm client python -m minigrid "$@"`.

- [ ] **Step 5: Completar o compose**

`coord-1` e `coord-2`: build do `coordinator/Dockerfile`, `50051:50051` e `50052:50051`, `MINIGRID_EXTERNAL_ADDR` distinto por réplica.
`worker-a`: `platform: linux/amd64`. `worker-b`: `platform: linux/arm64`. Ambos com `cpus: "0.5"` e `mem_limit: 256m`.
`worker-legacy`: `Dockerfile.legacy`, sem `platform`, `MINIGRID_CAPS=legacy:imagemagick`.
`dashboard`: `8080:8080`. `client`: build próprio, sem `command` (usado só por `run`).
Dependências por `condition: service_healthy`.

- [ ] **Step 6: Escrever `scripts/cleanup.sh` e o README**

`cleanup.sh`: `docker compose down -v --rmi local --remove-orphans`.
README: pré-requisitos (Docker e, só para o item G, `python3`), `docker compose up`, as quatro UIs (painel 8080, Consul 8500, RabbitMQ 15672), exemplos do `./minigrid`, como subir o worker do host, a nota de que o sistema funciona antes dele, a alternativa de contêiner avulso para o item G com a ressalva honesta, e `cleanup.sh`.

- [ ] **Step 7: Verificar a subida completa**

Run: `docker compose up -d --build && docker compose ps`
Expected: todos `healthy`/`running`. `./minigrid submit pi --steps 3` devolve id; `./minigrid watch <id>` termina em `done`.

- [ ] **Step 8: Rodar os testes e commitar**

Run: `python3 -m pytest tests/ -v`
Expected: PASS

```bash
git add docker-compose.yml minigrid client scripts README.md tests
git commit -m "feat(minigrid): compose completo, CLI do cliente e README"
```

---

### Task 13: Painel de monitoramento ao vivo

O conteúdo está fixado; a aparência é decisão do autor. **Antes do Step 3, oferecer protótipos de layout e esperar a escolha.**

**Files:**
- Create: `dashboard/app.py`, `dashboard/static/index.html`, `dashboard/Dockerfile`
- Test: `tests/test_dashboard.py`

**Interfaces:**
- Consumes: `events.Subscriber` (9), `naming.Consul` (2)
- Produces: `GET /` (página), `GET /events` (SSE), `GET /api/state` (JSON com `workers`, `leader`, `tasks`, `queue_depth`, `rpc_traces`)

- [ ] **Step 1: Escrever os testes**

```python
def test_state_endpoint_lists_workers_with_arch_and_virt(stack):
    s = get("/api/state")
    assert all("arch" in w and "virt" in w for w in s["workers"])

def test_state_endpoint_names_the_current_leader(stack):
    assert get("/api/state")["leader"]["internal"].startswith("coord-")

def test_sse_stream_emits_a_task_transition(stack, submit):
    with sse("/events") as stream:
        tid = submit("pi", steps=3)
        assert any(e["task_id"] == tid for e in stream.take(5, timeout=30))

def test_state_endpoint_survives_the_broker_being_unavailable(stack):
    stack.stop("rabbitmq")
    assert get("/api/state")["workers"] is not None      # degrada, não quebra
```

- [ ] **Step 2: Rodar e confirmar falha**

Run: `python3 -m pytest tests/test_dashboard.py -v`
Expected: FAIL

- [ ] **Step 3: Oferecer protótipos de layout ao autor**

Mostrar duas ou três opções de arranjo para os cinco painéis do design §6.1 e implementar a escolhida. Não seguir sem a resposta.

- [ ] **Step 4: Implementar `dashboard/app.py`**

FastAPI. Thread de fundo consome `minigrid.events` com fila exclusiva e temporária ligada a `#`, mantém estado em memória (últimos 200 eventos, últimos 50 rastreios de RPC) e empurra para as filas SSE conectadas. Thread separada consulta o Consul a cada 2 s para workers e líder, e a API de gerenciamento do RabbitMQ para a profundidade das filas. Falha de broker ou de Consul degrada o campo correspondente para `None` sem derrubar a resposta — é o quarto teste.

- [ ] **Step 5: Implementar `static/index.html`**

HTML e JS puros, sem build. Consome `/api/state` na carga e `/events` por `EventSource`. Os cinco painéis do design §6.1.

- [ ] **Step 6: Rodar e confirmar passagem**

Run: `python3 -m pytest tests/test_dashboard.py -v`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add dashboard tests
git commit -m "feat(minigrid): painel de monitoramento ao vivo por SSE"
```

---

### Task 14: A rubrica como testes

Cada teste é um item do enunciado. Roda local e no CI.

**Files:**
- Create: `tests/test_rubrica.py`, `scripts/bench_boot.sh`

**Interfaces:**
- Consumes: a pilha completa (Task 12), o painel (Task 13)
- Produces: os nove testes do design §6.2; `bench_boot.sh` imprimindo tempo de inicialização de contêiner nativo, contêiner emulado e VM de processo, em CSV

- [ ] **Step 1: Escrever os nove testes**

Nomes e asserções conforme a tabela do design §6.2: `test_a_interceptador`, `test_a_wrapper`, `test_b_assincrono`, `test_c_persistencia`, `test_c_pubsub`, `test_d_heterogeneidade`, `test_e_migracao`, `test_f_eleicao`, `test_g_worker_tardio`.

Pontos que os testes fixam e que não podem ser afrouxados: `test_c_persistencia` escala os workers a zero com `docker compose up -d --scale`, submete 10 tarefas, sobe **um** worker e exige as 10 concluídas; `test_d_heterogeneidade` exige que entre os `meta.arch` dos resultados apareçam **tanto** `x86_64` **quanto** `aarch64`; `test_f_eleicao` usa `docker kill` (não `stop`) no líder e aceita até 30 s.

- [ ] **Step 2: Rodar contra a pilha de pé**

Run: `docker compose up -d --build && python3 -m pytest tests/test_rubrica.py -v`
Expected: 9 PASS. Qualquer falha é defeito a corrigir na tarefa dona do código, não no teste.

- [ ] **Step 3: Escrever `scripts/bench_boot.sh`**

Mede com `docker run --rm` nativo, `docker run --rm --platform` emulado e um passo trivial na VM de processo; 5 repetições cada; imprime CSV. A saída alimenta a tabela comparativa do relatório (item D) com medição, não estimativa.

- [ ] **Step 4: Commit**

```bash
git add tests/test_rubrica.py scripts/bench_boot.sh
git commit -m "test(minigrid): a rubrica A-G como asseracoes automatizadas"
```

---

### Task 15: CI no GitHub Actions e roteiro da demonstração

**Files:**
- Create: `.github/workflows/ci.yml`, `scripts/demo.sh`
- Modify: `README.md` (badge e nota sobre onde o workflow mora)

**Interfaces:**
- Consumes: `tests/test_rubrica.py` (14)
- Produces: jobs `build` (buildx multi-arch) e `rubrica` (compose + pytest)

- [ ] **Step 1: Escrever `ci.yml`**

Job `build`: `docker/setup-qemu-action`, `docker/setup-buildx-action`, build de `worker` para `linux/amd64,linux/arm64`.
Job `rubrica`: `setup-qemu-action` (sem ele o `worker-b` arm64 não roda no runner x86_64), `pip install grpcio pytest` **no runner, que é descartável**, `docker compose up -d --build`, espera saúde, `scripts/host_worker.sh &`, `pytest tests/test_rubrica.py`, e `docker compose logs` no `if: failure()`.

- [ ] **Step 2: Escrever `scripts/demo.sh`**

Executa na ordem, com pausas rotuladas, os cinco pontos exigidos pelo enunciado para o vídeo: subida e registro; submissão por RPC e passagem pela fila; execução nos dois tipos de ambiente incluindo o de arquitetura diferente; `docker stop` no worker com tarefa em andamento e retomada por checkpoint; `docker kill` no líder e nova eleição. O vídeo não depende de digitação manual.

- [ ] **Step 3: Verificar o workflow sem depender do GitHub**

Run: `docker compose down -v && bash -x scripts/demo.sh`
Expected: o roteiro completo passa; os eventos aparecem no painel.
Nota: o `.github/workflows/ci.yml` fica na raiz deste repositório, então ele vai rodar de verdade assim que o repositório for publicado (Task 17). Até lá, esta verificação local é a única prova de que o workflow está correto — o que significa que os passos do `ci.yml` devem ser executáveis à mão, um por um, e é assim que este Step os valida.

- [ ] **Step 4: Commit**

```bash
git add .github scripts/demo.sh README.md
git commit -m "ci(minigrid): workflow da rubrica e roteiro da demonstracao"
```

---

### Task 16: Diagrama de arquitetura e relatório técnico

**Files:**
- Create: `docs/arquitetura.mmd`, `docs/arquitetura.png`, `docs/relatorio.md`

**Interfaces:**
- Consumes: medições de `bench_boot.sh` (14), o design inteiro
- Produces: os dois artefatos documentais da entrega

- [ ] **Step 1: Desenhar o diagrama**

Mermaid com as quatro camadas (aplicação, middleware, transporte, SO/ambiente virtual), o caminho da tarefa do RPC até o worker, o serviço de nomes, as duas réplicas do coordenador e o tipo de virtualização por nó. Exportar PNG por contêiner `mermaid-cli` descartável.

- [ ] **Step 2: Escrever o relatório**

As sete seções exigidas pelo enunciado, mais a seção de limitações do design §8.3. A tabela de comparação de virtualização usa a saída real do `bench_boot.sh`; a tabela de classificação de fluxos vem do design §7.9; as configurações do Consul vêm do design §7.3 com os valores que estão no código.

- [ ] **Step 3: Verificar o relatório contra o código**

Conferir, item por item do enunciado, que cada afirmação do relatório aponta para arquivo e teste existentes. Qualquer afirmação sem respaldo sai do relatório ou vira código.

- [ ] **Step 4: Commit**

```bash
git add docs
git commit -m "docs(minigrid): diagrama de arquitetura e relatorio tecnico"
```

---

### Task 17: Publicar o repositório público e montar o `.zip` da entrega

Última tarefa de propósito: o repositório nasce com o projeto inteiro pronto, nome certo e primeiro histórico limpo. **Publicar é ação externa e irreversível na prática — confirmar com o autor imediatamente antes de rodar o `gh repo create`, mesmo tendo autorização prévia.**

**Files:**
- Modify: `README.md` (badge do CI com a URL real, após o repositório existir)
- Create: `entrega/minigrid.zip` (ignorado pelo git)

**Interfaces:**
- Consumes: todas as tarefas anteriores
- Produces: `https://github.com/LeonardoMarcelo/project-distributed-systems` público; `.zip` com os quatro artefatos

- [ ] **Step 1: Verificar que não há nada sensível a publicar**

Run: `git log --oneline | head -30 && git ls-files | sort`
Expected: só arquivos deste projeto. Nenhum `.env`, nenhuma credencial, nada vindo do `msystem_api`. Repositório público expõe o histórico inteiro, não só o estado atual — então um segredo em commit antigo continua visível depois de removido.

- [ ] **Step 2: Rodar a rubrica inteira uma última vez, do zero**

Run: `docker compose down -v && docker compose up -d --build && scripts/host_worker.sh & && python3 -m pytest tests/ -v`
Expected: tudo PASS, partindo de estado limpo.

- [ ] **Step 3: Confirmar com o autor e publicar**

Confirmar nome e visibilidade, então:

```bash
gh repo create LeonardoMarcelo/project-distributed-systems --public --source=. --remote=origin --push
```

- [ ] **Step 4: Conferir o CI no GitHub e colocar o badge real**

Run: `gh run watch`
Expected: os dois jobs verdes. Se a rubrica falhar no runner e passar localmente, o defeito é real (arquitetura do runner, ausência do QEMU, tempo de boot) e vai consertado — não silenciado.
Então atualizar o badge do `README.md` com a URL do workflow e commitar.

- [ ] **Step 5: Montar o `.zip` da entrega**

Run: `git archive --format=zip -o entrega/minigrid.zip HEAD`
Expected: o `.zip` contém código, Dockerfiles, `docker-compose.yml`, scripts, README, diagrama e relatório. O vídeo é gravado à parte e anexado junto no AVA.

- [ ] **Step 6: Commit final**

```bash
git add README.md && git commit -m "docs: badge do CI" && git push
```
