# Especificação das mensagens

## Canal

| Item | Valor |
|---|---|
| Tópico | `freela.contrato.eventos.v1` (3 partições) |
| Produtor | `contrato-service` (via Outbox) |
| Consumidores | `notificacao-service`, `reputacao-service`, `auditoria-service` (um grupo de consumo por serviço) |
| Chave da mensagem | `contratoId` (UUID em texto) |
| Headers | `eventId`, `eventType`, `contratoId`, `correlationId`; `traceparent` (W3C Trace Context) quando a operação tem trace |

Todos os eventos usam o mesmo tópico e a mesma chave, para que os eventos de um contrato fiquem na mesma partição e
sejam consumidos na ordem em que ocorreram.

## Envelope (comum a todos os eventos)

| Campo | Tipo | Obrigatório | Descrição |
|---|---|---|---|
| `eventId` | UUID | sim | identificador único do evento; base da idempotência nos consumidores |
| `eventType` | texto | sim | `ContratoCriado`, `EntregaRegistrada`, `ContratoConcluido` ou `ContratoCancelado` |
| `eventVersion` | inteiro | sim | versão do formato da mensagem (atual: `1`) |
| `occurredAt` | data/hora ISO-8601 UTC | sim | momento em que o evento ocorreu no domínio |
| `contratoId` | UUID | sim | contrato relacionado (igual à chave) |
| `correlationId` | texto | sim | identificador da operação que originou o evento (header `X-Correlation-Id`) |
| `producer` | texto | sim | serviço produtor (`contrato-service`) |
| `payload` | objeto | sim | dados do contrato no momento do evento |

### Payload

| Campo | Tipo | Obrigatório | Descrição |
|---|---|---|---|
| `contratoId` | UUID | sim | contrato |
| `clienteId` | UUID | sim | cliente contratante |
| `freelancerId` | UUID | sim | freelancer contratado |
| `titulo` | texto (≤ 160) | sim | título do trabalho |
| `valor` | decimal (2 casas) | sim | valor do contrato |
| `status` | texto | sim | status do contrato **depois** do evento |
| `statusAnterior` | texto | só em `ContratoCancelado` | status antes do cancelamento |

## Eventos

| Evento | Quando ocorre | `status` | Campos extras |
|---|---|---|---|
| `ContratoCriado` | contrato criado | `ATIVO` | — |
| `EntregaRegistrada` | freelancer registrou a entrega | `ENTREGA_REGISTRADA` | — |
| `ContratoConcluido` | contrato concluído após a entrega | `CONCLUIDO` | — |
| `ContratoCancelado` | contrato ativo ou com entrega cancelado | `CANCELADO` | `statusAnterior` |

### Consumidores e efeito de cada evento

| Evento | `notificacao-service` | `reputacao-service` | `auditoria-service` |
|---|---|---|---|
| `ContratoCriado` | notifica cliente e freelancer | `contratosRecebidos + 1` | registra |
| `EntregaRegistrada` | notifica o cliente | ignora (sem impacto na reputação) | registra |
| `ContratoConcluido` | notifica cliente e freelancer | `contratosConcluidos + 1`; `valorTotalConcluido + valor` | registra |
| `ContratoCancelado` | notifica cliente e freelancer | `contratosCancelados + 1` | registra |
| tipo desconhecido | ignora | ignora | registra |

### Campos lidos por consumidor

Cada consumidor desserializa só o que usa e ignora campos desconhecidos (*tolerant reader*).

| Serviço | Campos obrigatórios para o serviço | Versões aceitas |
|---|---|---|
| `notificacao-service` | `eventId`, `eventType`, `eventVersion`, `contratoId`, `payload.clienteId`, `payload.freelancerId`, `payload.titulo` | `1` |
| `reputacao-service` | `eventId`, `eventType`, `eventVersion`, `contratoId`, `payload.freelancerId`, `payload.valor` | `1` |
| `auditoria-service` | `eventId`, `eventType`, `occurredAt`, `contratoId` (guarda também `eventVersion`, `correlationId`, `producer` e a mensagem inteira, como recebida) | todas |

Uma mensagem com JSON inválido, campo obrigatório ausente ou versão não aceita é tratada como **mensagem inválida**
(`EventoInvalidoException`): não é retentada, vai para a DLT do serviço e fica registrada como `INVALIDA`
([mensageria.md](mensageria.md#tratamento-de-falhas)).

## Exemplos (capturados do tópico)

### `ContratoCriado`

Chave: `b754c7b9-fe76-422f-812f-d51ee7492688`

Headers: `eventId=3e0ac030-e0cc-4dfe-addd-706ee6bb6f1f`, `eventType=ContratoCriado`,
`contratoId=b754c7b9-fe76-422f-812f-d51ee7492688`, `correlationId=demo-etapa2`

```json
{
  "eventId": "3e0ac030-e0cc-4dfe-addd-706ee6bb6f1f",
  "eventType": "ContratoCriado",
  "eventVersion": 1,
  "occurredAt": "2026-10-01T13:03:47.760657339Z",
  "contratoId": "b754c7b9-fe76-422f-812f-d51ee7492688",
  "correlationId": "demo-etapa2",
  "producer": "contrato-service",
  "payload": {
    "contratoId": "b754c7b9-fe76-422f-812f-d51ee7492688",
    "clienteId": "11111111-1111-1111-1111-111111111111",
    "freelancerId": "22222222-2222-2222-2222-222222222222",
    "titulo": "Construção de API de pagamentos",
    "valor": 3500.00,
    "status": "ATIVO"
  }
}
```

### `EntregaRegistrada`

Chave: `01a7a354-985d-4022-aa0d-49074a563d00` (partição 2, offset 1)

Headers: `eventId=cbf3c473-b1e8-40cb-8810-de33cdf981cf`, `eventType=EntregaRegistrada`,
`contratoId=01a7a354-985d-4022-aa0d-49074a563d00`, `correlationId=demo-etapa3`

```json
{
  "eventId": "cbf3c473-b1e8-40cb-8810-de33cdf981cf",
  "eventType": "EntregaRegistrada",
  "eventVersion": 1,
  "occurredAt": "2026-10-01T13:45:05.046863546Z",
  "contratoId": "01a7a354-985d-4022-aa0d-49074a563d00",
  "correlationId": "demo-etapa3",
  "producer": "contrato-service",
  "payload": {
    "contratoId": "01a7a354-985d-4022-aa0d-49074a563d00",
    "clienteId": "11111111-1111-1111-1111-111111111111",
    "freelancerId": "22222222-2222-2222-2222-222222222222",
    "titulo": "Construção de API de pagamentos",
    "valor": 3500.00,
    "status": "ENTREGA_REGISTRADA"
  }
}
```

### `ContratoConcluido`

Chave: `01a7a354-985d-4022-aa0d-49074a563d00` (partição 2, offset 2 — logo após a entrega do mesmo contrato)

Headers: `eventId=49233675-2753-4382-8839-eb43ed83f8cd`, `eventType=ContratoConcluido`,
`contratoId=01a7a354-985d-4022-aa0d-49074a563d00`, `correlationId=demo-etapa3`

```json
{
  "eventId": "49233675-2753-4382-8839-eb43ed83f8cd",
  "eventType": "ContratoConcluido",
  "eventVersion": 1,
  "occurredAt": "2026-10-01T13:45:05.124372895Z",
  "contratoId": "01a7a354-985d-4022-aa0d-49074a563d00",
  "correlationId": "demo-etapa3",
  "producer": "contrato-service",
  "payload": {
    "contratoId": "01a7a354-985d-4022-aa0d-49074a563d00",
    "clienteId": "11111111-1111-1111-1111-111111111111",
    "freelancerId": "22222222-2222-2222-2222-222222222222",
    "titulo": "Construção de API de pagamentos",
    "valor": 3500.00,
    "status": "CONCLUIDO"
  }
}
```

### `ContratoCancelado`

```json
{
  "eventId": "d312909c-72db-4e75-a3ec-0e167f604899",
  "eventType": "ContratoCancelado",
  "eventVersion": 1,
  "occurredAt": "2026-10-01T13:05:44.357057240Z",
  "contratoId": "f896a460-b2ec-42a7-9d11-7952480d0f1c",
  "correlationId": "demo-cancelamento",
  "producer": "contrato-service",
  "payload": {
    "contratoId": "f896a460-b2ec-42a7-9d11-7952480d0f1c",
    "clienteId": "33333333-3333-3333-3333-333333333333",
    "freelancerId": "22222222-2222-2222-2222-222222222222",
    "titulo": "Landing page",
    "valor": 800.00,
    "status": "CANCELADO",
    "statusAnterior": "ATIVO"
  }
}
```
