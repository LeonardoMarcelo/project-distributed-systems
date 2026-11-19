# MiniGrid

[![rubrica](https://github.com/LeonardoMarcelo/project-distributed-systems/actions/workflows/ci.yml/badge.svg)](https://github.com/LeonardoMarcelo/project-distributed-systems/actions/workflows/ci.yml)

Sistema de computação em grade em miniatura. O usuário submete tarefas
computacionais; o middleware MiniGrid as distribui, executa e acompanha em nós
trabalhadores heterogêneos e virtualizados — escondendo de quem submete onde, em
que arquitetura de processador e em que ambiente virtual cada tarefa roda.

Trabalho da disciplina de Sistemas Distribuídos.

---

## Começando

Pré-requisito: Docker (ou OrbStack). Numa máquina limpa:

```bash
docker compose up -d        # sobe tudo
scripts/wait_healthy.sh     # espera os healthchecks (opcional)
./grid submit pi --steps 20 --wait
```

Quatro coisas para olhar enquanto roda:

| | |
|---|---|
| **Painel do MiniGrid** | <http://localhost:8080> |
| Serviço de nomes (Consul) | <http://localhost:8500/ui> |
| Filas (RabbitMQ) | <http://localhost:15672> — `guest` / `guest` |
| Coordenadores (gRPC) | `localhost:50051` e `localhost:50052` |

Se alguma dessas portas já estiver ocupada na sua máquina — 8080 costuma estar —
copie `.env.example` para `.env` e ajuste. O compose lê esse arquivo sozinho:

```bash
cp .env.example .env        # e edite a porta que conflitar
```

Para desligar e não deixar rastro na máquina:

```bash
scripts/cleanup.sh
```

O que esse comando remove, e onde cada coisa estava: imagens e volumes vivem
dentro da VM do Docker, não no seu sistema de arquivos; código e documentos
vivem apenas nesta pasta; e o nó que roda fora do Docker não escreve nada em
disco além de diretórios temporários por tarefa, que ele próprio apaga. Não há
`brew install`, `pip install`, daemon nem alteração de PATH em nenhum ponto.

---

## O nó que roda fora do Docker

O sistema funciona sem ele. Quando você o sobe, ele se registra no serviço de
nomes e passa a receber tarefas — é a segunda forma de virtualização do
trabalho, uma **VM de processo** (interpretador isolado), em contraste com os
contêineres.

```bash
scripts/host_worker.sh
```

**Não instala nada na sua máquina.** Sem `pip`, sem venv, sem `brew`, sem
`sudo`. O único pré-requisito é um `python3` 3.9 ou mais novo, porque esse nó
usa somente a biblioteca padrão: `socket` para falar STOMP com o RabbitMQ e
`urllib` para falar HTTP com o Consul.

Para ver o efeito: abra o painel, rode o script, e o nó aparece na lista com o
crachá `VM de processo`. Pare com `Ctrl+C` no meio de uma tarefa longa e ela
migra para outro nó, retomando do último ponto de verificação.

<details>
<summary>Alternativa, se você preferir não rodar nada fora do Docker</summary>

Dá para rodar esse mesmo nó dentro de um contêiner avulso, fora do compose:

```bash
docker run --rm --network minigrid_minigrid_net \
  -e MINIGRID_CONSUL=http://consul:8500 \
  -e MINIGRID_STOMP_HOST=rabbitmq \
  -e MINIGRID_WORKER_ID=host-em-conteiner \
  -v "$PWD":/app -w /app python:3.12-slim \
  python -m host_worker.worker
```

A ressalva honesta: assim o ambiente de VM de processo passa a estar *hospedado
dentro de um contêiner*. O isolamento do interpretador continua real (processo
separado, limites de recurso, lista branca de importação), mas o carregador
deixa de ser "não contêiner". O caminho recomendado continua sendo
`scripts/host_worker.sh`.
</details>

---

## O cliente

```bash
./grid recipes                 # o catálogo de código móvel
./grid workers                 # quem está registrado, e em que arquitetura
./grid submit pi --steps 20 --wait
./grid submit compress --input arquivo.bin --output arquivo.z --wait
./grid submit img.filter --input foto.png --output menor.png --param resize=64x64 --wait
./grid status <id>
./grid watch <id>              # acompanha por push, sem polling
./grid bench --count 12        # mostra em que arquitetura cada tarefa rodou
```

Repare no que **nenhum** desses comandos aceita: endereço de worker, quantidade
de workers, nome de fila, qual réplica de coordenador está no comando. O cliente
conhece um endereço de serviço de nomes e mais nada.

`./grid` é um atalho de uma linha sobre `docker compose run --rm client`. O
cliente é conteinerizado porque precisa de `grpcio`, que tem extensão compilada
— a meta de não instalar nada na sua máquina vale para ele também.

Para enviar código seu, em vez de uma receita do catálogo:

```bash
cat > minha_tarefa.py <<'PY'
def step(state, data, api):
    n = state.get("n", 0) + 1
    return {"n": n}, n >= 5, (b"pronto" if n >= 5 else None)
PY
./grid submit minha --code minha_tarefa.py --wait
```

---

## Arquitetura

```
┌─ aplicação cliente (CLI) ────────────────────────────────────┐
│  submit(receita, dados) → id · status · result · watch        │
├─ middleware MiniGrid (biblioteca importada pelo cliente) ─────┤
│  resolve o líder no serviço de nomes · interceptador cliente  │
├─ transporte: gRPC sobre HTTP/2 sobre TCP ─────────────────────┘
          │
    ┌─────▼──────┐   eleição: sessão + lock de chave no Consul
    │ coord-1 ★  │◄──────────────┐
    │ coord-2    │               │
    └─────┬──────┘          ┌────▼────┐  registro dos nós
          │ AMQP            │ Consul  │  + health check por TTL
   ┌──────▼─────────────┐   └────▲────┘
   │ RabbitMQ           │        │
   │ fila durável       │        │ os nós se registram ao subir
   │ + exchange topic   │────────┴──→ eventos ──→ painel :8080
   │ + plugin STOMP     │
   └──┬─────┬─────┬─────┘
      │     │     │     │
  worker-a worker-b worker-legacy │ nó do host
  contêiner contêiner  contêiner  │ fora do Docker
  linux/amd64 linux/arm64 ImageMagick │ VM de processo
  (chamadas  (+ conjunto  (wrapper)   │ (interpretador
   de sistema) de instruções)         │  isolado)
```

**O caminho de uma tarefa.** O cliente chama `Submit` por gRPC; o interceptador
cronometra e registra a origem; o coordenador líder grava o metadado no serviço
de nomes, publica o envelope — código-fonte mais dados — na fila durável, e
devolve o identificador na hora. Um nó qualquer consome, toma a posse da tarefa,
instancia o código no seu ambiente virtual e executa passo a passo, gravando um
ponto de verificação a cada N passos. Cada transição de estado vai para o
exchange de eventos, de onde o painel e o `watch` do cliente recebem por push.
O resultado volta por fila durável até o líder.

### Os dois tipos de virtualização, e qual interface cada um imita

| Ambiente | Interface imitada | Isolamento | Limites |
|---|---|---|---|
| Contêiner nativo | chamadas de sistema (kernel compartilhado, namespaces) | namespaces de PID, rede e montagem | cgroups: `cpus 0.5`, `mem 256m` |
| Contêiner emulado | chamadas de sistema **+ conjunto de instruções** | idem, mais tradução de ISA por QEMU/Rosetta | idem |
| VM de processo | biblioteca e linguagem (VM de bytecode do CPython + superfície restrita da stdlib) | espaço de nomes da linguagem; processo separado | `setrlimit`: CPU, memória, tamanho de arquivo |

A VM de processo **não** isola por namespaces: ela compartilha kernel e visão de
sistema de arquivos com o usuário do host. O que ela isola é o espaço de nomes
da linguagem (lista branca de importação, `__builtins__` reduzido, sem `open`) e
os limites de recurso. No macOS o kernel aceita `RLIMIT_AS` e não o aplica — o
próprio sandbox declara isso em `limits_skipped`, para o relatório não afirmar um
isolamento de memória que, naquele ambiente, não existe.

### Migração de tarefa

Três gatilhos, todos demonstráveis:

| Gatilho | Mecanismo |
|---|---|
| Desligamento do nó | SIGTERM → o nó cede entre passos, devolve a mensagem à fila (`nack` com requeue) e publica `migrated` |
| Sobrecarga | carga acima do limiar → mesmo caminho, com `reason=overload` |
| Queda abrupta | o nó não tem chance de devolver nada; o health check expira no serviço de nomes e o coordenador republica a tarefa |

Funciona entre ambientes heterogêneos porque nada do que migra depende de
plataforma: o código é texto, o ponto de verificação é JSON, os dados são bytes.
Um ponto de verificação gravado por um nó `aarch64` é retomado sem tradução por
um nó `x86_64` emulado ou pela VM de processo do host.

Recursos locais no destino: o diretório de trabalho temporário é recriado pelo
sandbox a cada passo, e os dados de entrada vêm do envelope da mensagem, não do
sistema de arquivos. Não existe conexão de rede por tarefa — o acesso externo é
mediado pela API do sandbox. É por isso que não há nada a reconectar.

---

## Testes

A suíte precisa da infraestrutura de pé:

```bash
docker compose up -d
python3 -m pytest tests/ -q
```

`tests/test_rubrica.py` é a parte interessante: cada teste é um item do
enunciado, verificado contra o sistema em execução. Ele mexe no estado do
sistema — derruba nós, mata o líder, escala réplicas — e restaura no fim, mas
não rode junto com uma demonstração.

```bash
scripts/host_worker.sh &                    # para os testes do item G
python3 -m pytest tests/test_rubrica.py -v
```

Alguns testes precisam de `grpcio`, `pika` ou `grpc_tools`, e são
automaticamente ignorados se a biblioteca não estiver presente. Nada disso é
necessário para **usar** o MiniGrid — só para desenvolvê-lo. O nó que roda fora
do Docker continua sem dependência alguma, e `tests/test_purity.py` verifica
isso a cada rodada.

### O CI roda a própria rubrica

`.github/workflows/ci.yml` sobe o compose inteiro num runner do GitHub e roda
`tests/test_rubrica.py`. O badge no topo deste README não diz "os testes
passam"; diz que os itens A–G do enunciado foram verificados contra o sistema
em execução.

Um detalhe que vale a pena: o runner do GitHub é x86_64, então **lá o nó
emulado é o `linux/arm64`** — o inverso da máquina Apple Silicon onde o projeto
foi desenvolvido. Isso é intencional, e prova que fixar as duas plataformas no
compose funciona nas duas direções.

### Medições

```bash
scripts/bench_boot.sh        # custo de inicialização por ambiente, em CSV
./grid bench --count 24      # em que arquitetura cada tarefa rodou
```

As tabelas comparativas do relatório saem desses dois comandos, não de
estimativa.

---

## Gravando a demonstração

```bash
scripts/demo.sh              # pausa entre etapas, para narrar
scripts/demo.sh --auto       # sem pausas, para conferir que tudo passa
```

O roteiro segue a ordem que o enunciado pede para o vídeo: subida e registro dos
nós; submissão por RPC e passagem pela fila; execução nos dois tipos de ambiente
incluindo o de arquitetura diferente; migração de tarefa em andamento após o
desligamento do nó; e eleição de novo coordenador após matar a réplica
principal. Cada etapa anuncia o que vai provar antes de executar, então a
gravação não depende de digitação ao vivo nem de ordem decorada.

---

## Estrutura

```
docker-compose.yml       sobe o sistema inteiro
grid                     atalho do cliente
proto/minigrid.proto     a IDL: a interface do middleware
minigrid/                a biblioteca do middleware
  envelope.py  naming.py  stomp.py     ← só biblioteca padrão
  client.py  events.py  __main__.py
  recipes/               código móvel: pi, compress, img.filter
coordinator/             gRPC, eleição, despacho, resultado, reaper
worker/                  executor, checkpoint, sandbox, wrapper do legado
  sandbox.py  _runner.py                ← a VM de processo
host_worker/worker.py    o nó de fora do Docker ← só biblioteca padrão
dashboard/               painel: FastAPI + SSE + HTML sem build
scripts/                 host_worker.sh, cleanup.sh, demo.sh, bench_boot.sh
tests/                   unitários + a rubrica A–G
docs/                    design, plano, diagrama, relatório
```

---

## O que este projeto não faz

Declarado de propósito, porque é mais útil que a alternativa:

- **Mobilidade forte.** A migração retoma do último ponto de verificação, não do
  estado exato de execução. É a mobilidade fraca que o enunciado pede.
- **Migração de contêiner ou de máquina virtual inteira** (o bônus do item E).
- **Payload acima de 8 MB.** Acima disso o desenho correto exigiria
  armazenamento de objetos; o limite é verificado na borda e recusado com erro
  claro.
- **Consul em cluster.** Roda em nó único, e portanto é ponto único de falha. As
  réplicas que o enunciado exige são as do coordenador.
- **Autenticação e autorização.** Nenhuma.
