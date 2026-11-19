# MiniGrid — Middleware para Execução Distribuída de Tarefas

**Data:** 2026-11-18
**Disciplina:** Sistemas Distribuídos
**Raiz do projeto:** `/Users/leonardomarcelo/Documents/ferramentas/pessoal/bolsa/project_caio`
**Repositório:** `LeonardoMarcelo/project-distributed-systems`, público, criado ao fim do desenvolvimento
**Documento de design** — aprovado antes do plano de implementação.

---

## 1. Intenção e o porquê

Entregar o trabalho final de Sistemas Distribuídos: um sistema de computação em grade
em miniatura, no qual o grupo escreve a própria camada de middleware entre a aplicação
e o transporte. O usuário submete tarefas computacionais e o middleware esconde dele
onde, em que arquitetura de processador e em que ambiente virtual cada tarefa roda.

O porquê declarado pelo autor: cobrir **100% dos itens A–G do enunciado** pelo caminho
mais simples possível, e ter um **diferencial** que destaque o trabalho. Duas restrições
pessoais guiaram as decisões: nada deve ser instalado na máquina física do autor, e o
projeto não deve depender de infraestrutura em nuvem (a AWS foi considerada e descartada
— ver §5).

A cobertura dos requisitos não é um detalhe de implementação aqui: é o objetivo do
produto. Toda decisão de design abaixo foi tomada perguntando "isto é demonstrável no
vídeo e defensável no relatório?".

---

## 2. Metas, não-metas e anti-metas

### Metas

1. Cobrir integralmente os itens A–G do enunciado, cada um demonstrável ao vivo.
2. `docker compose up` sobe o sistema inteiro, numa máquina limpa, sem passo manual anterior.
3. Zero instalação na máquina física: nenhum `brew install`, `pip install`, `sudo`,
   daemon ou alteração de PATH. O único pré-requisito no host é um `python3` qualquer.
4. Funcionar igual no Apple Silicon (arm64) do autor e numa máquina x86_64 do avaliador.
5. Produzir os quatro artefatos de entrega: diagrama de arquitetura, relatório técnico,
   código + receita de infraestrutura com README, e vídeo de demonstração.

### Não-metas

- Autenticação, autorização ou multi-tenancy.
- Escala além do necessário para a demonstração (4–5 workers, dezenas de tarefas).
- Armazenamento de objetos para blobs grandes. Payloads são limitados a 8 MB
  (§7.6); acima disso o desenho correto seria MinIO/S3, e isso fica declarado como
  trabalho futuro no relatório.
- **Mobilidade forte.** O enunciado pede mobilidade fraca; migração de estado de
  processo em nível de SO não entra.
- **Migração de contêiner ou VM inteira** (bônus do item E). Fica declarada como
  trabalho futuro, com a justificativa técnica no relatório.

### Anti-metas — o que torna o trabalho um fracasso mesmo funcionando

- Um sistema que só roda na máquina do autor.
- Qualquer coisa que exija instalação na máquina do avaliador.
- Uma demonstração que precise de coreografia manual para parecer funcionar
  (ordem mágica de comandos, tarefa plantada, estado pré-cozido).
- Um relatório que esconda limitações. Toda limitação conhecida é declarada
  explicitamente pelo próprio relatório — ver §8.3.

---

## 3. Restrições

| Restrição | Origem | Consequência no design |
|---|---|---|
| `docker compose up` único, funcionando de imediato | item G | Nenhum passo de build manual; imagens construídas pelo próprio compose |
| Um ambiente virtualizado fora do contêiner, por script | item G | Worker do host, §7.4 |
| Pelo menos um worker em arquitetura de processador diferente | item D | `platform:` fixado nos dois workers genéricos, §7.4 |
| Filas persistentes, nada se perde com worker desligado | item C | Fila durável + mensagens persistentes + confirmação manual |
| Coordenador com ≥2 réplicas e eleição automática | item F | Consul session + KV lock, §7.3 |
| Zero instalação no host | autor | Worker do host usa só a stdlib do Python, §7.4 |
| Avaliador provavelmente em x86_64 | praticidade | Arquiteturas fixadas em vez de "a nativa e a emulada" |
| Entrega em `.zip` | enunciado | Projeto autocontido na raiz do próprio repositório |

---

## 4. Visão geral da arquitetura

```
┌─ aplicação cliente (CLI) ────────────────────────────────────┐
│  minigrid.submit(receita, dados) → id                         │
│  minigrid.status(id) · minigrid.result(id) · minigrid.watch(id)│
├─ middleware MiniGrid (biblioteca Python importada) ───────────┤
│  resolve o líder no Consul · interceptador cliente            │
├─ transporte: gRPC sobre HTTP/2 sobre TCP ─────────────────────┘
          │
    ┌─────▼──────┐   eleição: session + KV lock no Consul
    │ coord-1 ★  │◄──────────────┐
    │ coord-2    │               │
    └─────┬──────┘          ┌────▼────┐  registro de workers
          │ AMQP            │ Consul  │  + health check TTL
   ┌──────▼─────────────┐   └────▲────┘  (UI em :8500)
   │ RabbitMQ           │        │
   │ fila durável       │        │ workers se registram ao subir
   │ + exchange topic   │────────┴──→ eventos ──→ painel (:8080)
   │ + plugin STOMP     │
   └──┬─────┬─────┬─────┘
      │     │     │     │
  worker-a worker-b worker-legacy │ worker-host
  contêiner contêiner  contêiner  │ fora do Docker
  linux/amd64 linux/arm64 wrapper │ VM de processo
  (syscalls) (ISA emulada) ImageMagick (interpretador isolado)
```

### Caminho de uma tarefa

1. O cliente chama `submit` na biblioteca do middleware. A biblioteca lê a receita
   (código-fonte Python) do catálogo local, monta o envelope com código + dados e
   resolve o endereço do coordenador líder consultando o Consul.
2. Chamada gRPC. O **interceptador do servidor** cronometra, registra o método e a
   origem (`context.peer()`) e publica um evento de rastreio.
3. O coordenador líder grava o registro da tarefa no Consul KV, publica o envelope na
   fila durável com `delivery_mode=2` e devolve o `task_id` imediatamente.
4. Um worker qualquer consome (prefetch 1), adquire a posse da tarefa no Consul KV,
   instancia o código recebido no seu ambiente virtual e executa passo a passo.
5. A cada N passos grava checkpoint em JSON no Consul KV. A cada transição de estado
   publica no exchange topic: `received`, `running`, `checkpoint`, `migrated`,
   `done`, `failed`.
6. O painel e o `watch(id)` do cliente recebem essas transições por push.
7. O resultado volta por fila durável até o coordenador líder, que o grava no volume
   compartilhado e marca a tarefa como concluída.

---

## 5. Abordagens consideradas

### 5.1 Onde o sistema roda

**A — Nuvem (AWS) com CI/CD do GitHub.** Foi a ideia inicial do autor. **Descartada.**
O item G exige que o avaliador rode `docker compose up` e o sistema suba. Infra na AWS
introduz credenciais, custo e um sistema que o avaliador não consegue reproduzir na
máquina dele — trabalha diretamente contra o requisito. Além disso, a heterogeneidade de
arquitetura, que na nuvem custaria instâncias Graviton, sai de graça localmente.

**B — Tudo local, orquestrado por um único compose.** **Escolhida.** Atende o item G
literalmente, roda offline, custa zero e é reproduzível na máquina de qualquer avaliador.
O instinto de CI/CD do autor foi preservado, mas reposicionado: o GitHub Actions deixa de
ser onde o sistema roda e passa a ser onde o sistema **se prova** (§6.2).

### 5.2 O ambiente virtualizado que não é contêiner (item G)

**A — Ambiente virtual Python local (`.venv`) na pasta do projeto.** Primeira escolha,
depois **descartada** a pedido do autor, que não queria nem a pasta do venv. Era
tecnicamente suficiente: o `pip` só escreve dentro do `.venv/`.

**B — QEMU dentro de um contêiner, bootando um Linux mínimo.** **Descartada.** É o tipo
de virtualização mais forte do enunciado (monitor de máquina virtual hospedado) e não
instala nada no host, mas custa boot lento, imagem grande e rede em modo usuário difícil
de ligar no RabbitMQ. Custo alto para um requisito que já é atendido de outra forma.

**C — Contêiner avulso, fora do compose, rodando o interpretador isolado.**
**Descartada como padrão, mantida como escape.** Esforço quase nulo, mas um avaliador
rigoroso pode entender que "o ambiente que não for contêiner" foi burlado, já que o
carregador ainda é um contêiner. Fica documentada no README como alternativa de uma
linha, com a ressalva honesta.

**D — Worker do host usando apenas a biblioteca padrão do Python, falando STOMP à mão
sobre socket TCP.** **Escolhida.** O worker precisa de três coisas: registrar-se no
Consul (HTTP/JSON → `urllib.request`), consumir da fila (AMQP binário → era o único
bloqueio) e publicar eventos. Habilitando o plugin `rabbitmq_stomp` no contêiner do
RabbitMQ, o worker fala um protocolo de texto direto no socket: `CONNECT`, `SUBSCRIBE`,
`MESSAGE`, `ACK`, `NACK`, `SEND`. Assinando `/amq/queue/minigrid.tasks` ele consome da
**mesma fila durável** que os workers em contêiner, e o `NACK` com `requeue:true`
preserva integralmente a migração por reentrega.

Por que é melhor e não só equivalente: o item A exige o sistema organizado em camadas,
incluindo a camada de transporte. Em todo o resto do projeto o transporte fica escondido
dentro do gRPC e do pika. Aqui o socket é aberto, o frame é montado à mão e a camada
aparece. E o relatório ganha uma linha legítima na tabela de fluxos: a mesma fila
persistente acessada por dois protocolos diferentes.

**Plano B registrado:** se o STOMP consumir tempo além do previsto, vendorizar o `pika`
(Python puro, sem extensão compilada) em `vendor/` e rodar com `PYTHONPATH=vendor`.
Mantém AMQP de verdade, zero instalação, ao custo de ~1 MB de código de terceiro no
repositório.

### 5.3 O diferencial

Três candidatos foram apresentados ao autor. Ele escolheu os dois primeiros juntos
(painel + CI); o terceiro vira trabalho futuro.

**Escolhido — painel "sala de controle" ao vivo.** Uma tela só onde todos os requisitos
ficam visíveis simultaneamente. Vende o vídeo.

**Escolhido — CI que roda a própria rubrica.** Cada item A–G vira uma asserção
automatizada no GitHub Actions. Vende o relatório.

**Descartado — WebAssembly como terceiro nível de virtualização.** Tarefas compiladas
para `.wasm` e executadas pelo wasmtime: código em bytecode de ISA virtual, migração
entre arquiteturas gratuita por construção. Conceitualmente o mais forte, mas adiciona
toolchain (wasi-sdk/clang) e é o item de maior risco de atraso. Entra no relatório como
trabalho futuro, com a justificativa.

---

## 6. O diferencial, em detalhe

### 6.1 Painel de monitoramento ao vivo (`:8080`)

Serviço FastAPI que assina o exchange de eventos com uma fila exclusiva e temporária
ligada a `#`, consulta o Consul a cada 2 s, e serve uma página única em JavaScript puro
(sem passo de build) alimentada por Server-Sent Events.

O que a tela mostra, e qual requisito cada parte evidencia:

| Painel | Evidencia |
|---|---|
| Lista de workers com crachá de arquitetura e de tipo de virtualização, entrando e saindo ao vivo | D, F, G |
| Coroa do líder, pulando de réplica quando a principal cai | F |
| Profundidade da fila, subindo com todos os workers desligados | C |
| Raias de tarefas, com cartões migrando de raia e seta de migração | E |
| Lista de chamadas RPC com tempo de resposta e origem, vinda do interceptador | A |

O desenho visual da página fica aberto: na implementação serão oferecidos protótipos
para o autor reagir antes de fechar o layout. Ver §9.

### 6.2 CI que roda a rubrica (GitHub Actions)

Dois jobs. O primeiro faz build multi-arquitetura das imagens com `buildx` e
`docker/setup-qemu-action`. O segundo sobe o compose inteiro no runner e roda uma suíte
`pytest` em que cada teste é um item do enunciado:

| Teste | Asserção |
|---|---|
| `test_a_interceptador` | Existe evento de rastreio com `elapsed_ms` e origem para cada RPC |
| `test_a_wrapper` | `img.filter` devolve imagem transformada válida |
| `test_b_assincrono` | `Submit` responde em < 200 ms; `Watch` entrega o resultado depois |
| `test_c_persistencia` | Com 0 workers, 10 tarefas submetidas; ao subir 1 worker, as 10 completam |
| `test_c_pubsub` | Dois assinantes simultâneos recebem as mesmas transições |
| `test_d_heterogeneidade` | Entre os resultados aparecem `x86_64` e `aarch64` |
| `test_e_migracao` | Mata o worker no meio da tarefa; há evento `migrated` e o passo retomado é > 0 |
| `test_f_eleicao` | Mata o líder; novo líder no KV em ≤ 30 s e o cliente recupera sem erro |
| `test_g_worker_tardio` | Sistema saudável antes do worker do host; ele sobe, registra e executa |

O runner do GitHub é x86_64, então nele o worker emulado é o `linux/arm64` — o inverso do
Mac do autor. Isso é intencional: prova que o desenho de arquiteturas fixadas (§7.4)
funciona nas duas direções. O README recebe o badge do workflow.

---

## 7. Decisões de implementação

### 7.1 Pilha técnica

| Papel | Escolha | Motivo |
|---|---|---|
| Linguagem | Python 3.12 nos contêineres; stdlib pura no host | Já é a linguagem do autor; o interpretador é também o segundo ambiente de virtualização |
| RPC cliente↔coordenador | gRPC + protobuf | IDL explícita em `.proto`, empacotamento pelo framework, streaming de servidor para a notificação assíncrona, e **interceptador é recurso nativo** — o termo do enunciado é o nome da feature |
| Middleware de mensagens | RabbitMQ (imagem `management`) + plugin `stomp` | Filas duráveis e exchange topic num só broker; UI em `:15672` serve de evidência no vídeo |
| Serviço de nomes e coordenação | Consul | Registro de serviços, health check por TTL, KV com lock por sessão para eleição, tudo pela API HTTP/JSON — acessível da stdlib. UI própria em `:8500` |
| Painel | FastAPI + SSE + HTML/JS puro | Sem passo de build |
| Programa legado (wrapper) | ImageMagick `convert` | CLI legado real, saída visual para o vídeo |

### 7.2 Interface do middleware (`.proto`)

```proto
service MiniGrid {
  rpc Submit    (SubmitRequest) returns (SubmitReply);        // assíncrona: devolve o id
  rpc GetStatus (TaskId)        returns (TaskStatus);         // síncrona
  rpc GetResult (TaskId)        returns (TaskResult);         // síncrona
  rpc Watch     (TaskId)        returns (stream TaskEvent);   // assíncrona: notifica por push
  rpc ListWorkers(Empty)        returns (WorkerList);         // introspecção
}
```

O cliente nunca vê endereço nem quantidade de workers. As quatro transparências
oferecidas, e como:

- **Acesso** — `submit/status/result` são idênticas para tarefa de cálculo, tarefa de
  código enviado e serviço legado embrulhado; o cliente não sabe qual é qual.
- **Localização** — o endereço do coordenador vem do Consul, não de configuração; o
  worker que executa é escolhido pelo broker.
- **Migração** — a tarefa troca de worker e o `task_id` e o `watch` continuam válidos.
- **Falhas** — queda do líder e queda do worker são absorvidas pela biblioteca, sem
  erro visível ao cliente.

### 7.3 Eleição de líder (Consul)

Cada réplica de coordenador:

1. Cria sessão: `PUT /v1/session/create` com
   `{"Name":"coord-N","TTL":"10s","Behavior":"delete","LockDelay":"0s"}`.
2. Renova a sessão a cada 5 s.
3. Tenta adquirir: `PUT /v1/kv/minigrid/coordinator/leader?acquire=<session>`, com o
   corpo contendo os endereços interno e externo em JSON.
4. Quem falha vira seguidor e observa a chave com consulta bloqueante
   (`?index=N&wait=10s`), assumindo quando ela é liberada.

Duas configurações que precisam ser conscientes, e vão explicadas no relatório:

- **`LockDelay: 0s`** — o padrão do Consul é 15 s, que seria somado ao tempo de failover.
  Zerar reduz o tempo de eleição pela metade.
- **`Behavior: delete` com TTL 10 s** — numa parada graciosa (SIGTERM) o coordenador
  destrói a própria sessão e o failover é imediato. Numa queda abrupta (`docker kill`),
  o failover é limitado pelo TTL, e o Consul recolhe sessões expiradas entre TTL e
  2×TTL — por isso a asserção do CI é ≤ 30 s, não ≤ 10 s. A demonstração usa
  `docker kill` justamente para mostrar o caso real e narrar o limite.

**Estrutura de chaves:**

```
minigrid/coordinator/leader          → {"internal":"coord-1:50051","external":"localhost:50051"}
minigrid/tasks/<id>/meta             → receita, parâmetros, estado, timestamps
minigrid/tasks/<id>/checkpoint       → estado serializado em JSON
minigrid/tasks/<id>/owner            → id do worker, adquirido com a sessão do worker
```

A chave `owner`, adquirida com lock de sessão, resolve um problema real: depois de um
`nack`/requeue, o worker antigo pode ainda estar vivo. Só o detentor do lock escreve
checkpoint e resultado, o que impede execução dupla de corromper a saída.

### 7.4 Workers e virtualização

| Worker | Virtualização | `platform` | Interface imitada | Papel na demonstração |
|---|---|---|---|---|
| `worker-a` | Contêiner | `linux/amd64` | Chamadas de sistema (kernel compartilhado, namespaces, cgroups) | Worker genérico |
| `worker-b` | Contêiner | `linux/arm64` | Chamadas de sistema **+ conjunto de instruções** quando emulado | Heterogeneidade de ISA |
| `worker-legacy` | Contêiner | nativo | Chamadas de sistema | Wrapper do ImageMagick |
| `worker-host` | VM de processo | fora do Docker | Biblioteca/linguagem: VM de bytecode do CPython + superfície restrita de stdlib | Item G, registro tardio, heterogeneidade de SO |

**Arquiteturas fixadas, não relativas.** O compose declara `platform:` explicitamente nos
dois workers genéricos. Em qualquer host, exatamente um dos dois está emulado: no Apple
Silicon do autor é o `amd64`; no runner x86_64 do CI e na máquina do avaliador é o
`arm64`. Isso elimina a dependência da arquitetura de quem corrige.

**Limites declarados** em todos os contêineres de worker: `cpus: "0.5"` e
`mem_limit: 256m`, via cgroups.

**O worker do host como VM de processo — isolamento real.** Esta é a crítica mais provável
do avaliador ("isto é interpretador isolado ou só executar o código no próprio
processo?"), e o isolamento tem que ser demonstrável:

- subprocesso em `python3 -I -S`: sem `site-packages`, sem variáveis de ambiente, sem
  o diretório corrente no `sys.path`;
- `resource.setrlimit` para tempo de CPU, espaço de endereçamento, tamanho de arquivo e
  número de processos — o **análogo direto dos limites de cgroup do contêiner**, o que
  deixa a tabela comparativa do relatório simétrica;
- `__builtins__` reduzido a lista branca, com import hook bloqueando qualquer módulo
  fora do permitido; o código recebe uma API explícita (`read_input`, `emit_checkpoint`,
  `run_legacy`), sem `os` e sem socket;
- diretório de trabalho temporário por tarefa, destruído ao fim.

`resource` não existe no Windows: o README declara macOS/Linux e o script avisa e segue
sem os limites se não conseguir aplicá-los.

**Comparação exigida pelo item D** (medida na implementação, não estimada): tempo de
inicialização e grau de isolamento de contêiner nativo, contêiner emulado e VM de
processo. A tabela do relatório vem de medição real feita por um script.

### 7.5 Tarefas: código móvel e checkpoint

**Toda tarefa viaja como código mais dados** — é a mobilidade fraca do item E. O catálogo
de receitas fica no lado do cliente, em `minigrid/recipes/*.py`; `submit("pi", …)` lê o
arquivo e envia o **código-fonte** no envelope. O cliente também pode enviar fonte
arbitrária. O worker recebe, instancia e executa no seu ambiente virtual.

Contrato que toda receita implementa:

```python
def step(state, data, api):
    """Executa um passo. Devolve (novo_state, concluido: bool, resultado: bytes | None)."""
```

O executor do worker: carrega o checkpoint se existir, senão o estado inicial; chama
`step` em laço; publica progresso a cada passo e grava checkpoint a cada N passos.
`state` é JSON — neutro de arquitetura, igual ao código-fonte, e é exatamente por isso
que a migração entre arm64, x86_64 e host funciona.

Exigência sobre quem escreve receita, documentada: `step` precisa ser determinística
dado o `state`, porque uma reentrega reexecuta a partir do último checkpoint.

**Receitas iniciais:**

- `pi` — Monte Carlo em 20 passos de ~1 s. É a tarefa da demonstração de migração.
- `img.filter` — chama `api.run_legacy("convert", [...])`; o wrapper fica exposto ao
  código móvel como chamada de biblioteca, o que realiza literalmente o
  "tratando-o como um serviço como qualquer outro" do enunciado.
- `compress` — compressão de um blob, tarefa curta para encher a fila nos testes.

**Roteamento por capacidade.** O envelope traz `requires: []`. O coordenador publica em
`minigrid.tasks` (genérica) ou `minigrid.tasks.legacy`, ambas duráveis. `worker-a`,
`worker-b` e `worker-host` consomem a primeira; `worker-legacy`, a segunda. Os workers
anunciam capacidade como tag no Consul, junto da arquitetura
(`arch:<platform.machine()>`) e do tipo de virtualização (`virt:container|process-vm`).
A mensagem de resultado carrega a arquitetura de quem executou — é o que o
`test_d_heterogeneidade` e o painel leem.

**Ordem de criação da topologia.** Quem declara exchanges e filas é o coordenador líder,
no boot. O `worker-host` assina `/amq/queue/minigrid.tasks` por STOMP, o que **exige que a
fila já exista** — então ele tenta em laço com recuo até conseguir, em vez de assumir
ordem de subida. Isso é também o comportamento que o item G pede: o sistema funciona antes
dele e o absorve quando aparece.

**ImageMagick 6 vs 7.** No Debian bookworm o binário é `convert`; no ImageMagick 7 ele foi
substituído por `magick`. O wrapper detecta qual existe e usa o disponível, para a imagem
base não precisar ser fixada numa distribuição específica.

**Gatilhos de migração** — três, todos demonstráveis:

1. **Desligamento** — SIGTERM no worker: o handler faz `nack(requeue=True)`, publica
   `migrated` e sai. O broker reentrega a outro worker, que retoma do checkpoint.
2. **Sobrecarga** — o worker mede a própria carga e, acima de um limiar configurado,
   devolve a tarefa à fila com motivo `overload`.
3. **Queda abrupta** — o coordenador líder observa a expiração do health check TTL do
   worker no Consul; se ele detinha a posse de uma tarefa, republica a tarefa a partir
   do último checkpoint. É o mecanismo que cobre o worker do host, cujo consumo por
   STOMP tem semântica de ack ligeiramente diferente.

**Migração entre ambientes heterogêneos**, para o relatório: funciona porque nada no que
migra é dependente de plataforma — código é texto, checkpoint é JSON, dados são bytes.
Recursos locais que precisam ser reestabelecidos no destino: o diretório de trabalho
temporário é recriado; os dados de entrada são recarregados do envelope da mensagem, não
do sistema de arquivos; não há conexão de rede aberta por tarefa, por desenho — o acesso
externo é mediado pela API do executor.

### 7.6 Resultados e blobs

Resultados voltam por fila durável `minigrid.results`, consumida pelo coordenador líder,
que grava o blob num volume Docker compartilhado pelas duas réplicas. O worker do host
publica na mesma fila por STOMP e **não** precisa do volume — os dados de entrada viajam
dentro da mensagem. Limite documentado de 8 MB por payload. Checkpoints, por serem
pequenos, ficam no Consul KV (limite de 512 KB por valor).

### 7.7 Como o cliente acha o coordenador

A biblioteca do middleware lê `GET /v1/kv/minigrid/coordinator/leader?raw` no Consul,
extrai o endereço e abre o canal gRPC. Em falha de RPC (`UNAVAILABLE`) ela relê a chave e
tenta de novo, com recuo exponencial e um número máximo de tentativas. É daqui que vêm as
transparências de localização e de falhas. A chave guarda os dois endereços, interno e
externo, porque há consumidores dos dois lados da fronteira do Docker (§7.12).

### 7.8 Interceptadores (item A)

- **Servidor** — `grpc.ServerInterceptor`: captura método, origem (`context.peer()`),
  tempo de início e fim, e código de status. Escreve linha estruturada em log e publica
  `rpc.trace` no exchange de eventos, de onde o painel monta o quadro de latência.
- **Cliente** — interceptador unário na biblioteca do middleware: injeta um
  `x-minigrid-client-id` nos metadados e mede o tempo ponta a ponta.

### 7.9 Classificação dos fluxos (item C)

O relatório traz a tabela completa. As linhas já determinadas pelo design:

| Fluxo | Sincronismo | Persistência | Granularidade |
|---|---|---|---|
| `Submit` cliente→coordenador | assíncrona (devolve id) | transiente | discreta |
| `GetStatus` / `GetResult` | síncrona | transiente | discreta |
| `Watch` coordenador→cliente | assíncrona | transiente | fluxo |
| Tarefa coordenador→worker (AMQP) | assíncrona | **persistente** | discreta |
| Tarefa coordenador→worker do host (STOMP) | assíncrona | **persistente** | discreta |
| Eventos de estado (pub/sub) | assíncrona | transiente | fluxo |
| Resultado worker→coordenador | assíncrona | **persistente** | discreta |
| Registro e heartbeat no Consul | síncrona | transiente | discreta |
| Consulta bloqueante do seguidor no Consul | síncrona com espera longa | transiente | discreta |

### 7.10 Orquestração (item G)

Um `docker-compose.yml` na raiz do projeto sobe: `consul`, `rabbitmq`, `coord-1`,
`coord-2`, `worker-a`, `worker-b`, `worker-legacy`, `dashboard`. Dependências por
healthcheck, não por ordem de declaração, para que o sistema suba sozinho.

`scripts/host_worker.sh` sobe o worker do host, depois, sem instalar nada. O sistema
funciona antes dele; quando ele aparece no Consul, é absorvido e passa a receber tarefas.

`scripts/cleanup.sh` derruba tudo e remove imagens e volumes, para zerar a máquina
depois da apresentação.

### 7.11 Estrutura de diretórios

```
project_caio/                  raiz do repositório project-distributed-systems
├── README.md                  instruções de uso, pré-requisitos, alternativas
├── docker-compose.yml
├── proto/minigrid.proto
├── minigrid/                  a biblioteca do middleware (cliente + comum)
│   ├── client.py              submit/status/result/watch + interceptador cliente
│   ├── naming.py              cliente Consul (HTTP, stdlib)
│   ├── events.py              publicação e assinatura do pub/sub
│   ├── __main__.py            CLI da aplicação cliente (roda em contêiner, §7.12)
│   └── recipes/               código móvel: pi.py, img_filter.py, compress.py
├── coordinator/               servidor gRPC, eleição, despacho, coleta de resultado
├── worker/                    executor, checkpoint, sandbox, wrapper do legado
├── dashboard/                 FastAPI + SSE + index.html
├── scripts/                   host_worker.sh, cleanup.sh, bench_boot.sh, demo.sh
├── tests/                     a rubrica como testes (roda local e no CI)
├── docs/                      diagrama de arquitetura + relatório técnico
└── .github/workflows/         build multi-arch + rubrica
```

Nota: o projeto ganhou repositório próprio e público
(`LeonardoMarcelo/project-distributed-systems`), o que simplificou duas coisas que antes
eram problema. O `.github/workflows/` fica na raiz do repositório, que é a mesma raiz do
projeto — então o CI roda de verdade e o badge do README funciona, sem espelhamento nem
repositório paralelo. E o `.zip` da entrega é o próprio repositório, sem recorte.

### 7.12 Onde cada componente roda, e por que

A meta de zero instalação no host só se sustenta se estiver explícito o que atravessa a
fronteira do Docker. Esta subseção fecha isso, porque é onde o desenho pode falhar sem
ninguém notar.

| Componente | Roda onde | Dependências externas |
|---|---|---|
| `consul`, `rabbitmq`, `coord-1`, `coord-2`, `worker-a`, `worker-b`, `worker-legacy`, `dashboard` | contêiner | dentro da imagem |
| **CLI do cliente** | **contêiner**, via `docker compose run --rm client …` | dentro da imagem |
| `worker-host` | host, fora do Docker | **nenhuma** — só a stdlib do Python |
| Suíte de testes da rubrica | host do CI (runner descartável) ou contêiner | `grpcio` + `pytest`, instalados só no runner |

O ponto que isso resolve: o CLI do cliente precisa de `grpcio`, que tem extensão
compilada e exigiria instalação. Então **o cliente é um componente conteinerizado**, e o
README expõe um atalho `./minigrid submit pi --steps 20` que é um invólucro de uma linha
sobre `docker compose run`. O enunciado não exige o cliente fora de contêiner; exige
apenas que ele não conheça endereço nem quantidade de workers, o que continua verdade.

O `worker-host` é o único processo na máquina física, e ele nunca fala gRPC — só HTTP com
o Consul (`localhost:8500`) e STOMP com o RabbitMQ (`localhost:61613`), ambas as portas
publicadas pelo compose. É por isso que a stdlib basta.

Consequência para quem escreve o código do `worker-host`: ele precisa rodar no `python3`
que o avaliador tiver, então o código fica compatível com **Python 3.9+** — sem
`match`, sem `X | Y` em anotação, sem `tomllib`. Os contêineres usam 3.12 e não têm essa
restrição.

### 7.13 Geração dos stubs do protobuf

Os stubs são gerados por `grpcio-tools` em etapa de build da imagem, **e** uma copia
gerada é versionada em `minigrid/_pb2/`. O motivo da duplicação: a suíte de testes roda
fora dos contêineres no CI e precisa dos stubs sem recompilar o `.proto`. Um teste
compara o `.proto` com os stubs versionados e falha se estiverem dessincronizados, para a
copia versionada não apodrecer.

---

## 8. Artefatos de entrega

### 8.1 Diagrama de arquitetura

Em camadas, mostrando aplicação, middleware, transporte e ambiente virtual de cada nó;
o caminho da tarefa do RPC até o worker; o serviço de nomes; as réplicas do coordenador;
e o tipo de virtualização por nó. Produzido como fonte versionável (Mermaid ou
Draw.io/XML) mais PNG exportado.

### 8.2 Relatório técnico

Seções fechadas pelo enunciado: interface do middleware e transparências; frameworks de
comunicação e motivo da escolha; classificação dos fluxos; comparação entre ambientes de
virtualização; estratégia de checkpoint e migração; configurações do Consul; bibliotecas
utilizadas. Todas as tabelas comparativas vêm de medição, não de estimativa.

### 8.3 Limitações declaradas pelo próprio relatório

Isto é deliberado: quem declara a própria limitação não é pego nela.

- A VM de processo compartilha kernel e visão de sistema de arquivos com o usuário do
  host; não há isolamento por namespaces. O que ela isola é o espaço de nomes da
  linguagem e os limites de recurso.
- A migração é fraca: retoma do último checkpoint, não do estado exato de execução.
- Migração de contêiner ou VM inteira (bônus) não foi implementada.
- Blobs acima de 8 MB não são suportados; o desenho correto exigiria armazenamento de
  objetos.
- O Consul roda em nó único. As réplicas exigidas pelo enunciado são as do coordenador;
  um Consul em nó único é ponto único de falha e isso está consciente.

### 8.4 Vídeo de demonstração

`scripts/demo.sh` executa o roteiro na ordem, para que a gravação não dependa de
coreografia manual: subida dos componentes e registro dos workers no Consul; submissão
por RPC e passagem pela fila; execução nos dois tipos de ambiente, incluindo o nó de
arquitetura diferente; migração de tarefa em andamento após desligar o worker; e eleição
de novo coordenador após matar a réplica principal.

---

## 9. O que fica a critério de quem constrói

- **Desenho visual do painel.** O conteúdo está definido em §6.1; a aparência não. Serão
  oferecidos protótipos ao autor antes de fechar o layout.
- Nomes exatos de módulos, funções e eventos, desde que a estrutura de §7.11 e as chaves
  de §7.3 sejam respeitadas.
- Valores de ajuste: intervalo de checkpoint, limiar de sobrecarga, recuo das tentativas,
  intervalo de atualização do painel. Padrões sensatos, configuráveis por variável de
  ambiente.
- Escolha entre Mermaid e Draw.io para o diagrama.
- Versões exatas das imagens base, desde que fixadas por tag e funcionando nas duas
  arquiteturas.

---

## 10. Riscos

| Risco | Mitigação |
|---|---|
| `NACK` com `requeue:true` por STOMP não se comportar como esperado numa fila `/amq/queue/` | **Primeiro passo do plano é um spike descartável** contra o broker real, antes de construir em cima: assinar, receber, recusar, confirmar a reentrega. Se falhar, cai no plano B imediatamente, sem retrabalho |
| STOMP à mão consome mais tempo que o previsto | Plano B pronto: vendorizar o `pika` (§5.2) |
| Worker emulado lento demais para a demonstração | Tarefas da demo são curtas; o `pi` usa passos de ~1 s |
| Failover de líder lento por causa do TTL | `LockDelay: 0s`, TTL 10 s, destruição graciosa de sessão; asserção honesta de ≤ 30 s |
| ImageMagick indisponível para `linux/arm64` | O worker legado roda em plataforma nativa, sem emulação |
| Reexecução dupla após requeue corromper resultado | Lock de posse por sessão no Consul KV (§7.3) |
| CI instável derrubar a confiança no projeto | O CI é evidência, não dependência: o sistema roda localmente sem ele |

---

## 11. Decisões tomadas e o que ainda está aberto

As lacunas levantadas na revisão do documento foram resolvidas nele mesmo (§7.12, §7.13,
§7.5). As duas que dependiam do autor foram decididas em 18/11/2026:

1. **Onde o projeto mora — decidido.** Repositório próprio, dedicado e público:
   `LeonardoMarcelo/project-distributed-systems`, com raiz local em
   `/Users/leonardomarcelo/Documents/ferramentas/pessoal/bolsa/project_caio`. O projeto
   saiu do `msystem_api`, que é repositório de trabalho. Isso resolve a questão do CI por
   construção (§7.11) e elimina o risco de o CI da empresa reagir a commits de trabalho
   acadêmico. O repositório público é criado **ao fim** do desenvolvimento, não no começo,
   para o nome e o primeiro commit já nascerem certos.

2. **Prazo de entrega — ainda sem resposta; assumido curto.** A ordem de construção é
   fechar A–G funcionando primeiro, depois o painel, depois o CI, depois os artefatos
   documentais. Se o prazo for folgado, o painel sobe antes e passa a servir de ferramenta
   de depuração durante o desenvolvimento — é a única coisa que muda.

Nota sobre o nome do repositório: o autor pediu `project_system_distribuieds`, "nome em
inglês". `distribuieds` não é palavra inglesa, então o nome adotado é
`project-distributed-systems`. Como o repositório só é criado na última tarefa, trocar
isso não tem custo.

---

## 12. O que a construção mudou no desenho, e por quê

Este documento é o registro do que foi decidido **antes** de construir. A seção
abaixo registra onde a implementação divergiu dele, para quem ler o projeto não
precisar adivinhar qual dos dois está certo. As decisões que mudaram, mudaram
por evidência — quase toda ela só apareceu quando o sistema subiu de verdade.

### 12.1 Defeitos que a execução real revelou

**O worker emulado não executava nenhuma tarefa.** O §7.4 previa
`RLIMIT_AS` de 192 MB nos workers. `RLIMIT_AS` limita *espaço de endereçamento
virtual*, e a camada de emulação de ISA reserva muito espaço virtual para as
próprias estruturas de tradução. Toda tarefa no `worker-a` (linux/amd64 sob
Rosetta) morria antes do primeiro passo com
`rosetta error: mmap_anonymous_rw mmap failed`. Medido: o emulado precisa de
**≥768 MB** só para iniciar (falha em 192 e em 384, passa em 768); o nativo roda
com 192 MB. O padrão subiu para 1 GB e o `mem_limit` do cgroup para 512m.

A parte importante: **a quebra era invisível**. O nó aparecia registrado e
saudável no painel e no serviço de nomes, e simplesmente nunca entregava nada. O
item D estava morto sem nenhum sinal de erro. Antes da correção, 0 de 12 tarefas
no nó emulado; depois, ele passou a executar normalmente.

Consequência para o relatório: o teto de memória real dos contêineres vem do
*cgroup*, que é aplicado pelo kernel. `RLIMIT_AS` é uma guarda secundária,
relevante onde não há cgroup — isto é, no nó de VM de processo.

**Falha de tarefa nunca chegava ao estado autoritativo.** Só o resultado de
sucesso passava pela fila durável; a falha apenas publicava um evento, que é
transiente por desenho (§7.9). O estado no serviço de nomes ficava `received`
para sempre. Três consequências visíveis para quem usa: `GetStatus` mentia,
`watch()` esperava um evento terminal que já havia passado e nunca retornava, e
o reaper enxergava a tarefa como viva. Corrigido com
`Transport.report_failure`, que relata a falha pelo mesmo caminho durável do
resultado.

**`GetStatus` não reportava progresso.** O estado saltava de `received` direto
para `done`, com `worker_id` vazio e `step` em zero durante toda a execução,
pelo mesmo motivo: os eventos de ciclo de vida não eram gravados em lugar
nenhum. Isso quebrava uma das três operações que o enunciado nomeia. Corrigido
com `worker/progress.py`, que projeta `running`/`checkpoint`/`migrated` no KV;
quem escreve é o nó que detém o lock de posse, logo não há escritores
concorrentes.

**O orçamento de retentativa do cliente era menor que o failover medido.** O
§7.3 mediu ~20 s de failover por expiração de TTL, mas o cliente desistia em
~15 s (5 tentativas com recuo exponencial) — a transparência de falhas quebrava
exatamente no caso que ela existe para cobrir. O limite passou a ser um
*deadline* de 45 s, não uma contagem de tentativas.

**O limite de payload anunciado era falso.** O §7.6 declara 8 MB, mas o teto
padrão de mensagem do gRPC é 4 MB, então um payload de 8 MB era recusado pelo
transporte com `RESOURCE_EXHAUSTED` antes de chegar à validação. O limite real
era metade do documentado, e o erro era opaco. Corrigido com
`envelope.GRPC_MAX_MESSAGE`, aplicado nas três pontas a partir de uma constante
única.

### 12.2 Decisões que mudaram por engenharia

| Previsto | Implementado | Motivo |
|---|---|---|
| `RLIMIT_NPROC` entre os limites do sandbox | ausente | É um limite por *usuário*, não por processo; com a máquina do avaliador rodando centenas de processos, recusaria qualquer `fork`, inclusive o do wrapper legado. A criação de processo passou a ser controlada pela superfície da API |
| Sessão do worker com `Behavior=delete` | `Behavior=release` | Com `delete`, a morte do nó apagaria a chave `owner` junto com o lock, e o reaper — que procura justamente por tarefas cujo dono desapareceu — não teria o que republicar. O terceiro gatilho de migração ficaria morto |
| Atalho do cliente chamado `./minigrid` | `./grid` | `minigrid` já é o diretório do pacote Python na raiz |
| Portas fixas | `${VAR:-padrão}` com `.env.example` | O avaliador roda isto numa máquina que já tem as próprias coisas de pé |
| Cliente sem volume | `./:/work` como diretório de trabalho | Sem isso o contêiner não vê os arquivos de quem o chamou, e `--input`/`--output` ficam inutilizáveis — o que impediria a demonstração do wrapper legado |

### 12.3 Limitação que o sistema agora declara sozinho

O §8.3 previa declarar no relatório que a VM de processo não isola por
namespaces. A implementação foi além: o sandbox **reporta em tempo de execução**
quais limites não foram aplicados, em `limits_skipped`. No macOS o kernel aceita
`RLIMIT_AS` e o ignora, e nem `setrlimit` nem `getrlimit` revelam isso — então o
sandbox declara a não-aplicação explicitamente, e o teste correspondente exige
uma das duas coisas: limite aplicado, ou não-aplicação declarada. Não há
caminho em que o sistema passe por isolado sem ser.

### 12.4 Medições disponíveis para o relatório

| O que | Valor medido |
|---|---|
| Failover do coordenador, parada graciosa | 0,21 s |
| Failover do coordenador, queda abrupta (`docker kill`) | 20,15 s |
| `RLIMIT_AS` mínimo para o nó emulado iniciar | 768 MB (falha em 384) |
| Custo de um passo: nativo vs emulado | 0,04 s vs 0,18 s (~4,5×) |
| Limite de memória: contêiner nativo | `MemoryError`, aplicado |
| Limite de memória: contêiner emulado | morto por sinal (`rosetta mmap failed`) |
| Limite de memória: VM de processo no macOS | **não aplicado**, e declarado |
| Leitura de profundidade de fila: estatística vs autoritativa | 2–5 s de atraso vs 0,007 s |

Custo de inicialização por ambiente (`scripts/bench_boot.sh`, 3 repetições, host
`aarch64`, máquina sob carga — a razão entre ambientes é o que interessa):

| Ambiente | Interface imitada | Média |
|---|---|---|
| Contêiner nativo | chamadas de sistema | 0,381 s |
| Contêiner emulado | chamadas de sistema **+ conjunto de instruções** | 0,516 s (≈1,35× o nativo) |
| VM de processo | biblioteca e linguagem | 0,050 s (≈7,6× mais barato que o contêiner) |

A ordenação é a esperada e vale ser dita no relatório: quanto mais fina a camada
que o ambiente imita, mais barato iniciá-lo. A VM de processo só precisa de um
`fork`/`exec` de interpretador; o contêiner precisa de namespaces, cgroups e uma
árvore de sistema de arquivos em camadas; o contêiner emulado paga tudo isso
mais a tradução de instruções. O preço do isolamento aparece como tempo de
partida — e é por isso que a comparação do item D não é um detalhe: ela mostra
que não existe ambiente de virtualização "melhor", existe troca entre custo de
partida e grau de isolamento.

`scripts/bench_boot.sh` produz a tabela de custo de inicialização por ambiente
em CSV; `./grid bench` produz a distribuição de tarefas por arquitetura.
