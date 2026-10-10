# Observabilidade

## Correlação de uma operação

Toda operação iniciada por uma requisição HTTP recebe um `correlationId`, que acompanha a operação até os
consumidores, a DLT e a auditoria.

- **Formato aceito:** `[A-Za-z0-9._:-]{1,100}` Um valor fora do
  formato é trocado por um UUID novo antes de ser logado ou encaminhado;
- **Sem header:** o gateway gera um UUID. Uma chamada direta a um serviço, sem passar pelo gateway, recebe um id
  gerado pelo próprio serviço.
- **`correlationId` × `traceId`:** o `correlationId` identifica a operação de negócio, pode ser escolhido pelo cliente
  e fica gravado nos eventos e na auditoria. O `traceId` é técnico, gerado pelo tracing, e mostra no Zipkin
  o caminho e o tempo de cada serviço.

## Formato dos logs

Todas as aplicações, exceto o `eureka-server`, usam o mesmo padrão:

```text
%d %-5level service=<app> correlationId=<mdc> contratoId=<mdc> eventId=<mdc> traceId=<mdc> thread=<thread> logger=<logger> - <chave> campo=valor ...
```

Cada linha começa com o contexto (serviço e identificadores) e termina com uma chave fixa (`<origem>.<objeto>.<fato>`)
seguida de campos `nome=valor`. Isso permite filtrar por texto nos arquivos e por campo no Loki (ver abaixo).

## Dados sensíveis

**Regra:** os logs registram identificadores (UUIDs), tipos, status, contadores, partição, offset e durações. Nunca
registram título, valor, texto de notificação, corpo de requisição ou conteúdo de mensagem.

O conteúdo completo das mensagens existe só onde ele é necessário para diagnóstico e auditoria, fora dos logs:
`auditoria_eventos.conteudo`, `mensagens_com_falha.conteudo` e as DLTs.

## Rastreamento distribuído (Zipkin)

Todas as aplicações, exceto o `eureka-server`, enviam spans ao Zipkin (`http://localhost:9411`). Uma operação iniciada
no Gateway forma um único trace até os consumidores:

```text
api-gateway        SERVER   http post                    (span raiz)
api-gateway        CLIENT   http post                    -> injeta o header traceparent na chamada ao serviço
contrato-service   SERVER   http post /api/contratos/... -> OutboxPublicadorDeEventos grava o traceparent atual
                                                            em outbox_eventos.traceparent (mesma transação)
                   (relay, até 1 s depois)               -> OutboxRelayService publica com o header traceparent
notificacao-service CONSUMER freela.contrato.eventos.v1 process  \
reputacao-service   CONSUMER freela.contrato.eventos.v1 process   > filhos do span do contrato-service
auditoria-service   CONSUMER freela.contrato.eventos.v1 process  /
```

O ponto que exige código é a outbox: a mensagem é publicada por um relay agendado, fora da thread e do contexto da
requisição, então o contexto de tracing é gravado junto com o evento e devolvido como header na publicação. Se o
Kafka estiver fora do ar e o evento for publicado minutos depois, os consumidores continuam no mesmo trace.

## Centralização dos logs (Loki e Grafana)

O gateway e os quatro serviços enviam os logs ao Loki, além de escrevê-los no console. Assim, uma operação pode ser
acompanhada em um só lugar, o Grafana (`http://localhost:3000`, Explore, datasource **Loki** já provisionado).s

Consultas (Grafana Explore ou `GET /loki/api/v1/query_range`):

| Para | Consulta |
|---|---|
| uma operação, em todos os serviços | `{app=~".+"} \| correlationId="demo-etapa9"` |
| tudo sobre um contrato | `{app=~".+"} \|= "<contratoId>"` |
| um evento: publicação e consumo | `{app=~".+"} \| eventId="<eventId>"` |
| as linhas de um trace do Zipkin | `{app=~".+"} \| traceId="<traceId>"` |
| falhas de consumo | `{app=~".+"} \|~ "evento\\.(falha\|falha-definitiva\|estacionado)"` |
