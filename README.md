# Freela Marketplace

Projeto de referência para um marketplace de contratação de freelancers construído com arquitetura de microsserviços em Java e Spring.

A aplicação representa um cenário em que clientes contratam freelancers para a execução de trabalhos. O núcleo do sistema é o gerenciamento dos contratos firmados entre as partes. A partir desse domínio, outros serviços mantêm informações relacionadas a notificações, reputação e auditoria.

## Visão geral

O sistema é composto por seis aplicações Spring Boot:

- `eureka-server`: registro e descoberta dos serviços.
- `api-gateway`: ponto de entrada HTTP da aplicação.
- `contrato-service`: gerenciamento dos contratos entre clientes e freelancers.
- `notificacao-service`: registra as notificações de clientes e freelancers a partir dos eventos de contrato.
- `reputacao-service`: mantém o histórico agregado de cada freelancer a partir dos eventos de contrato.
- `auditoria-service`: registra todos os eventos publicados pela plataforma, para consulta posterior.

A infraestrutura local utiliza PostgreSQL e Apache Kafka, além de Zipkin, Loki e Grafana para observabilidade.

```text
 Cliente HTTP  (X-Correlation-Id opcional)
      |
      v
 +-------------+   Eureka :8761   +--------------------------------------------+
 | API Gateway | ---------------> | contrato-service :8081                     |
 |    :8080    |                  |   contrato + outbox_eventos (1 transação)  |
 +-------------+                  |   relay da outbox (a cada 1 s)             |
      |                           +----------------------+---------------------+
      | consultas                                        | publica (chave = contratoId)
      |                                                  v
      |                       Kafka :9092  freela.contrato.eventos.v1 (3 partições)
      |                                                  |
      |                  +-------------------------------+-------------------------------+
      |                  v                               v                               v
      +------> notificacao-service :8082     reputacao-service :8083     auditoria-service :8084
               (grupo de consumo próprio)    (grupo de consumo próprio)  (grupo de consumo próprio)

 PostgreSQL :5432  contrato_db | notificacao_db | reputacao_db | auditoria_db
 Observabilidade   logs: Loki :3100 + Grafana :3000    traces: Zipkin :9411
```

Nenhum serviço consumidor chama o `contrato-service`: tudo o que eles sabem sobre os contratos chega pelos eventos.

## Documentação

| Item (§12 da atividade) | Onde |
|---|---|
| visão geral da arquitetura e serviços participantes | este README e [docs/arquitetura.md](docs/arquitetura.md) |
| tópicos, eventos e formato das mensagens | [docs/eventos.md](docs/eventos.md) (contrato de comunicação entre os serviços) |
| estratégia de particionamento, idempotência, publicação transacional (Outbox) e falhas | [docs/mensageria.md](docs/mensageria.md) |
| centralização de logs e configuração de tracing | [docs/observabilidade.md](docs/observabilidade.md) |
| inicialização da infraestrutura e das aplicações | [Infraestrutura local](#infraestrutura-local) e [Execução das aplicações](#execução-das-aplicações) |
| exemplos de chamadas que geram eventos | [Exemplos de chamadas](#exemplos-de-chamadas) |
| testes automatizados | [Testes automatizados](#testes-automatizados) |
| evidências da execução (§13) | [docs/evidencias-passo-a-passo.md](docs/evidencias-passo-a-passo.md) |

## Domínio

O domínio principal está no `contrato-service`.

Um contrato representa o vínculo entre um cliente e um freelancer para a execução de um trabalho. Cada contrato possui:

- identificador;
- cliente;
- freelancer;
- título do trabalho;
- valor;
- status;
- data de criação.

Os estados disponíveis são:

```text
ATIVO
ENTREGA_REGISTRADA
CONCLUIDO
CANCELADO
```

O fluxo de negócio previsto pelo modelo é:

```text
ATIVO
  |
  v
ENTREGA_REGISTRADA
  |
  v
CONCLUIDO
```

Um contrato ativo ou com entrega registrada também pode ser cancelado. Um contrato concluído não pode ser cancelado, e cancelar um contrato já cancelado não tem efeito (operação idempotente).

Cliente e freelancer precisam ser pessoas diferentes.

O `contrato-service` utiliza uma organização inspirada em Domain-Driven Design, separando domínio, aplicação e infraestrutura.

```text
contrato-service
└── src/main/java/br/com/freela/contrato
    ├── application
    ├── domain
    │   ├── event
    │   ├── model
    │   ├── repository
    │   └── shared
    └── infrastructure
        ├── messaging
        ├── observabilidade
        ├── persistence
        └── web
```

O Aggregate `Contrato` concentra as regras relacionadas às mudanças de estado e produz um evento de domínio para cada transição:

| Transição | Evento |
|---|---|
| criação | `ContratoCriado` |
| `ATIVO` → `ENTREGA_REGISTRADA` | `EntregaRegistrada` |
| `ENTREGA_REGISTRADA` → `CONCLUIDO` | `ContratoConcluido` |
| `ATIVO`/`ENTREGA_REGISTRADA` → `CANCELADO` | `ContratoCancelado` (com `statusAnterior`) |

Os eventos implementam a interface selada `ContratoEvento` e carregam o retrato do contrato no momento em que ocorreram (contrato, cliente, freelancer, título, valor e status), para que os serviços consumidores não precisem consultar o `contrato-service`.

Os conceitos do domínio são value objects (records imutáveis que validam a si mesmos):

| Value object | Regra |
|---|---|
| `ContratoId`, `ClienteId`, `FreelancerId` | obrigatórios; tipos distintos impedem trocar cliente por freelancer |
| `Dinheiro` | positivo, sempre com 2 casas decimais (`HALF_UP`), como a coluna `numeric(19,2)` |
| `Titulo` | obrigatório, sem espaços nas pontas, até 160 caracteres |

As fronteiras (entidade JPA, DTOs REST e mensagens publicadas) continuam com tipos primitivos; a conversão fica nos adapters.

Violações de regra lançam `RegraDeNegocioException`. A persistência usa controle de concorrência otimista (`@Version`): duas alterações simultâneas no mesmo contrato não são gravadas ao mesmo tempo — a segunda recebe HTTP 409.

## Serviços

### contrato-service

Responsável pelo ciclo de vida dos contratos.

Porta:

```text
8081
```

Banco:

```text
contrato_db
```

Principais recursos HTTP:

```text
POST /api/contratos
POST /api/contratos/{id}/entrega
POST /api/contratos/{id}/conclusao
POST /api/contratos/{id}/cancelamento
GET  /api/contratos
GET  /api/contratos/{id}
```

Erros são devolvidos como `ProblemDetail` (RFC 7807):

| Situação | HTTP |
|---|---:|
| corpo ou parâmetro inválido | 400 |
| contrato inexistente | 404 |
| alteração concorrente no mesmo contrato | 409 |
| regra de negócio violada (ex.: concluir sem entrega) | 422 |

Exemplo de criação de contrato:

```json
{
  "clienteId": "11111111-1111-1111-1111-111111111111",
  "freelancerId": "22222222-2222-2222-2222-222222222222",
  "titulo": "Construção de API de pagamentos",
  "valor": 3500.00
}
```

### notificacao-service

Mantém notificações relacionadas aos acontecimentos do marketplace.

Porta:

```text
8082
```

Banco:

```text
notificacao_db
```

Consome os eventos de contrato e registra uma notificação para cada destinatário: cliente e freelancer na criação,
na conclusão e no cancelamento; só o cliente quando a entrega é registrada. Cada notificação guarda o evento de
origem (`eventId`, tipo), o contrato, o destinatário e seu papel, a mensagem e o momento de criação.

Endpoints de consulta:

```text
GET /api/notificacoes
GET /api/notificacoes/contrato/{contratoId}
GET /api/notificacoes/destinatario/{destinatarioId}
```

Os três consumidores também expõem o tratamento de falhas (ver [Mensageria](#mensageria)):

```text
GET    /api/{notificacoes|reputacoes|auditoria}/falhas?status=FALHA
POST   /api/{notificacoes|reputacoes|auditoria}/falhas/contrato/{contratoId}/reprocessamento
POST   /api/{notificacoes|reputacoes|auditoria}/simulacoes/falhas/{contratoId}
DELETE /api/{notificacoes|reputacoes|auditoria}/simulacoes/falhas/{contratoId}
```

### reputacao-service

Mantém informações agregadas sobre a atividade dos freelancers.

Porta:

```text
8083
```

Banco:

```text
reputacao_db
```

Consome os eventos de contrato e mantém, para cada freelancer, a quantidade de contratos recebidos, concluídos e
cancelados e o valor total dos contratos concluídos. `EntregaRegistrada` não altera a reputação.

Endpoints de consulta:

```text
GET /api/reputacoes
GET /api/reputacoes/{freelancerId}
```

### auditoria-service

Responsável pelo armazenamento de registros associados aos eventos do sistema.

Porta:

```text
8084
```

Banco:

```text
auditoria_db
```

Consome todos os eventos do tópico, inclusive de tipos ou versões que os outros serviços não conhecem. Cada registro
guarda:

- `eventId` (único), tipo e versão do evento;
- `sequencia`, a ordem de registro atribuída pelo banco;
- `contratoId`;
- data e hora do evento (`occurredAt`) e do recebimento (`recebidoEm`);
- `correlationId` e serviço produtor;
- a mensagem original, como recebida;
- tópico, partição e offset de onde foi lida.

Endpoints de consulta:

```text
GET /api/auditoria
GET /api/auditoria/contrato/{contratoId}
GET /api/auditoria/correlacao/{correlationId}
GET /api/auditoria/evento/{eventId}
```

## API Gateway

O `api-gateway` é o ponto de entrada HTTP para os microsserviços.

Porta:

```text
8080
```

As rotas configuradas são:

| Caminho | Serviço |
|---|---|
| `/api/contratos/**` | `contrato-service` |
| `/api/notificacoes/**` | `notificacao-service` |
| `/api/reputacoes/**` | `reputacao-service` |
| `/api/auditoria/**` | `auditoria-service` |

O Gateway utiliza Eureka para localizar as instâncias dos serviços.

Também existe suporte ao header:

```text
X-Correlation-Id
```

Quando o header não é enviado pelo cliente, ou não está no formato aceito (`[A-Za-z0-9._:-]`, até 100 caracteres),
o Gateway gera um UUID, o encaminha para o serviço de destino e o devolve na resposta, inclusive em erros do próprio
Gateway (rota inexistente, serviço indisponível).

## Eureka Server

O Eureka Server mantém o registro das aplicações disponíveis no ambiente.

Porta:

```text
8761
```

Interface web:

```text
http://localhost:8761
```

Os microsserviços utilizam, por padrão:

```text
http://localhost:8761/eureka/
```

como endereço do service registry.

## PostgreSQL

O ambiente utiliza uma única instância PostgreSQL com bancos separados para cada serviço.

```text
Host:     localhost
Porta:    5432
Usuário:  freela
Senha:    freela
```

Bancos criados durante a inicialização:

```text
contrato_db
notificacao_db
reputacao_db
auditoria_db
```

O script de criação dos bancos está em:

```text
infra/postgres/init-databases.sql
```

Os serviços utilizam Hibernate com `ddl-auto: update` para criação e atualização das tabelas locais.

## Mensageria

As mudanças no ciclo de vida do contrato são publicadas no Kafka com o padrão Transactional Outbox:

- tópico `freela.contrato.eventos.v1` (3 partições), chave `contratoId`;
- o evento é gravado na tabela `outbox_eventos` na mesma transação do contrato e publicado por um relay agendado;
- o header `X-Correlation-Id` é aceito ou gerado, devolvido na resposta e gravado em cada evento;
- `notificacao-service`, `reputacao-service` e `auditoria-service` consomem o tópico, cada um no seu grupo de consumo,
  sem nenhuma chamada HTTP ao `contrato-service`;
- cada consumidor usa 3 threads, uma por partição: contratos diferentes são processados em paralelo, e os eventos de um
  mesmo contrato, em ordem;
- cada consumidor registra o `eventId` de cada mensagem processada (`eventos_processados`), na mesma transação do
  efeito: uma mensagem recebida de novo não gera notificação, contagem ou registro repetido;
- uma mensagem que falha é tentada 4 vezes (mensagens inválidas, nenhuma), registrada na tabela
  `mensagens_com_falha` e publicada na DLT do serviço (`freela.contrato.eventos.v1.<serviço>.dlt`). Os eventos
  seguintes do mesmo contrato ficam estacionados até o reprocessamento pela API.

Detalhes em [docs/arquitetura.md](docs/arquitetura.md), [docs/mensageria.md](docs/mensageria.md) e
[docs/eventos.md](docs/eventos.md) (especificação das mensagens).

## Apache Kafka

O Apache Kafka é executado em modo KRaft, sem ZooKeeper.

Para aplicações executadas diretamente na máquina:

```text
localhost:9092
```

Para aplicações executadas dentro da rede Docker:

```text
kafka:19092
```

O broker possui listeners separados para comunicação interna e externa.

O ambiente também inclui o Kafka UI.

```text
http://localhost:8090
```

## Logs

Os serviços e o gateway utilizam logs em nível `INFO` com um formato comum contendo data, nível, nome da aplicação,
identificadores de correlação (`correlationId`, `contratoId`, `eventId`, `traceId`), thread, logger e mensagem. O catálogo
completo, o caminho da correlação e a política de dados sensíveis estão em [docs/observabilidade.md](docs/observabilidade.md).

Os logs do Gateway e dos quatro serviços também são enviados ao Loki (`loki-logback-appender`) e consultados no
Grafana, em `http://localhost:3000` (Explore, datasource Loki). Os campos do MDC são pesquisáveis, por exemplo
`{app=~".+"} | correlationId="demo-etapa9"` traz a operação em todos os serviços.

Exemplo:

```text
2026-10-05 19:51:38.150 INFO  service=notificacao-service correlationId=demo-etapa8 contratoId=f84e5557-... eventId=f58e6d9b-... traceId=6ac429f9cbdac86bd4b1e0adc28391bf thread=notificacao-contrato-eventos-2-C-1 logger=b.c.f.n.i.m.ProcessadorDeEventos - notificacao.evento.recebido eventType=EntregaRegistrada particao=2 offset=17
```

O código registra pontos importantes do fluxo, incluindo:

```text
gateway.requisicao.inicio / gateway.requisicao.fim (status e durationMs)
http.contrato.criar / http.contrato.entrega / http.contrato.conclusao / http.contrato.cancelamento
contrato.criacao.inicio / contrato.criacao.sucesso
outbox.evento.registrado / outbox.evento.publicado / outbox.publicacao.falha
notificacao.evento.recebido / notificacao.registrada / notificacao.evento.processado / notificacao.evento.falha
reputacao.evento.recebido / reputacao.atualizada / reputacao.evento.ignorado / reputacao.evento.processado
<servico>.evento.duplicado / <servico>.evento.estacionado / <servico>.evento.falha-definitiva
auditoria.evento.recebido / auditoria.evento.processado / auditoria.evento.falha
```

O `correlationId` da requisição HTTP é gravado em cada evento e restaurado nos consumidores a partir dos headers da
mensagem, então uma mesma operação pode ser seguida do Gateway até a auditoria. O Gateway valida o
`X-Correlation-Id` recebido (ou gera um) e o devolve em toda resposta. Títulos, valores, textos de notificação e
conteúdo de mensagens não aparecem nos logs.

## Rastreamento distribuído

O Gateway e os quatro serviços enviam spans ao Zipkin (`spring-boot-starter-zipkin`, amostragem de 100%). Uma operação
iniciada no Gateway forma um único trace: Gateway, `contrato-service` e os três consumidores. O contexto de tracing
(`traceparent`) é gravado na outbox junto com o evento e publicado como header da mensagem, e os consumidores continuam
o mesmo trace. O `traceId` aparece nos logs.

```text
http://localhost:9411
```

Detalhes e exemplo em [docs/observabilidade.md](docs/observabilidade.md#rastreamento-distribuído-zipkin).

## Infraestrutura local

Os serviços de infraestrutura estão definidos em:

```text
infra/docker-compose.yml
```

Para iniciar o ambiente (PostgreSQL, Kafka, Kafka UI, Zipkin, Loki e Grafana):

```bash
cd infra
docker compose up -d
```

Para verificar os containers:

```bash
docker compose ps
```

Para encerrar:

```bash
docker compose down
```

Os dados do PostgreSQL são mantidos em volume Docker.

Para remover também os dados persistidos:

```bash
docker compose down -v
```

## Execução das aplicações

A partir da raiz do projeto, cada módulo é iniciado separadamente com Maven, um por terminal. O Eureka Server deve
subir primeiro.

Eureka Server:

```bash
mvn -pl eureka-server spring-boot:run
```

API Gateway:

```bash
mvn -pl api-gateway spring-boot:run
```

Contrato Service:

```bash
mvn -pl contrato-service spring-boot:run
```

Notificação Service:

```bash
mvn -pl notificacao-service spring-boot:run
```

Reputação Service:

```bash
mvn -pl reputacao-service spring-boot:run
```

Auditoria Service:

```bash
mvn -pl auditoria-service spring-boot:run
```

## Portas

| Componente | Porta |
|---|---:|
| API Gateway | `8080` |
| contrato-service | `8081` |
| notificacao-service | `8082` |
| reputacao-service | `8083` |
| auditoria-service | `8084` |
| Eureka Server | `8761` |
| Kafka | `9092` |
| Kafka UI | `8090` |
| Zipkin | `9411` |
| Loki | `3100` |
| Grafana | `3000` |
| PostgreSQL | `5432` |

## Testes automatizados

```bash
mvn clean test
```

São 67 testes: de domínio (JUnit puro) e de integração com PostgreSQL e Kafka reais (Testcontainers, mesmas imagens do
`docker-compose`). Eles cobrem producer (Outbox), consumidores, duplicidade, ordenação, falhas, logs e tracing. É
preciso ter o Docker em execução; sem ele, os testes de integração são ignorados.

## Exemplos de chamadas

Com a infraestrutura e as aplicações em execução, todas as chamadas passam pelo Gateway. Cada transição do contrato
gera um evento.

```bash
# ContratoCriado
curl -i -X POST http://localhost:8080/api/contratos \
  -H 'Content-Type: application/json' \
  -H 'X-Correlation-Id: teste-contrato-001' \
  -d '{
    "clienteId": "11111111-1111-1111-1111-111111111111",
    "freelancerId": "22222222-2222-2222-2222-222222222222",
    "titulo": "Construção de API de pagamentos",
    "valor": 3500.00
  }'

# EntregaRegistrada e ContratoConcluido ({id} = "id" devolvido na criação)
curl -i -X POST http://localhost:8080/api/contratos/{id}/entrega   -H 'X-Correlation-Id: teste-contrato-001'
curl -i -X POST http://localhost:8080/api/contratos/{id}/conclusao -H 'X-Correlation-Id: teste-contrato-001'

# ContratoCancelado (de um contrato ATIVO ou com entrega registrada)
curl -i -X POST http://localhost:8080/api/contratos/{id}/cancelamento -H 'X-Correlation-Id: teste-contrato-002'
```

Consultas:

```bash
curl http://localhost:8080/api/contratos
curl http://localhost:8080/api/contratos/{id}
```

Cerca de um segundo depois (o intervalo do relay da outbox), os consumidores já reagiram aos eventos:

```bash
curl http://localhost:8080/api/notificacoes/contrato/{id}
curl http://localhost:8080/api/reputacoes/22222222-2222-2222-2222-222222222222
curl http://localhost:8080/api/auditoria/correlacao/teste-contrato-001
```

## Tecnologias

```text
Java 21
Spring Boot 4.1
Spring Cloud
Spring Cloud Gateway
Netflix Eureka
Spring Data JPA
PostgreSQL 16
Apache Kafka 4
Micrometer Tracing (Brave) e Zipkin
Loki e Grafana (loki-logback-appender)
Docker Compose
Maven
```
