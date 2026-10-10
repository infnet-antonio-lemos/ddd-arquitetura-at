# Mensageria

## Publicação transacional (Transactional Outbox)

Gravar o contrato no Postgres e publicar no Kafka são duas operações em sistemas diferentes, sem transação comum
(*dual write*). Se a aplicação cair entre as duas, ou o contrato muda e o evento se perde, ou os consumidores reagem a
algo que nunca foi gravado.

Por isso o evento é gravado **na mesma transação** que altera o contrato, na tabela `outbox_eventos` do próprio
`contrato_db`. Um processo separado (relay) lê essa tabela e publica no Kafka.

```text
ContratoApplicationService (@Transactional)
  1. salva o contrato                      -> UPDATE contratos
  2. publicador.publicar(eventos)          -> INSERT outbox_eventos (status = PENDENTE)
  commit, ou rollback dos dois

OutboxRelayService (a cada 1 s, @Transactional)
  1. pg_try_advisory_xact_lock             -> só um relay ativo por vez
  2. lê os PENDENTE em ordem de id
  3. envia um por vez e espera a confirmação do Kafka
       sucesso -> PUBLICADO
       falha   -> registra o erro e interrompe o lote
```

- **Sem evento fora da transação:** o `OutboxPublicadorDeEventos` usa `@Transactional(propagation = MANDATORY)`, então
  chamá-lo sem uma transação aberta é erro.
- **Kafka fora do ar:** a API continua respondendo. Os eventos ficam `PENDENTE`, com `tentativas` e `ultimo_erro`, e
  são publicados quando o Kafka volta.
- **Producer:** `acks=all` e `enable.idempotence=true`, para que os reenvios internos não dupliquem nem reordenem as
  mensagens.
- **Conteúdo da linha:** o envelope JSON pronto para publicar, a chave, o `correlation_id` e o `traceparent` da
  operação.

## Tópico e particionamento

| Item | Valor |
|---|---|
| Tópico | `freela.contrato.eventos.v1`, com 3 partições |
| Chave | `contratoId` |
| Headers | `eventId`, `eventType`, `contratoId`, `correlationId` e `traceparent` |
| Grupos de consumo | um por serviço: `notificacao-service`, `reputacao-service`, `auditoria-service` |
| DLT | `freela.contrato.eventos.v1.<serviço>.dlt`, uma por consumidor |

Todos os eventos do contrato usam o mesmo tópico e a mesma chave. Assim, os eventos de um contrato caem sempre na
mesma partição, onde o Kafka preserva a ordem. Tópicos separados por tipo de evento perderiam essa garantia.

O formato das mensagens está em [eventos.md](eventos.md).

## Consumo

Cada serviço tem o seu grupo de consumo, então todos recebem todos os eventos.

| Configuração | Valor | Motivo |
|---|---|---|
| `auto-offset-reset` | `earliest` | um serviço que sobe depois do produtor não perde eventos |
| `listener.ack-mode` | `record` | o offset é confirmado depois de cada mensagem processada |
| `listener.concurrency` | 3 | uma thread por partição |
| `allow.auto.create.topics` | `false` | cada serviço declara o tópico com 3 partições (`NewTopic`), e a ordem de subida não importa |

```text
ContratoEventosListener (@KafkaListener)
  1. coloca correlationId, contratoId e eventId dos headers no contexto de log (MDC)
  2. ProcessadorDeEventos
     a. lê o JSON em um DTO do próprio serviço e traduz para o modelo local
     b. o contrato tem falha pendente? -> a mensagem fica ESTACIONADA
     c. ReceptorIdempotente: evento já processado? -> duplicado, nada é feito
     d. evento novo -> serviço de aplicação -> banco do serviço
  3. exceção -> retentativas e, se não resolver, DLT
```

- **Entrega *at-least-once*:** o offset só é confirmado depois do commit no banco. Uma queda entre os dois reentrega a
  mensagem, que é reconhecida como duplicada.
- **Sem HTTP entre os serviços:** os eventos carregam o retrato do contrato, e nenhum consumidor consulta o
  `contrato-service`.
- **Sem biblioteca compartilhada:** cada consumidor tem o próprio DTO e lê só os campos que usa. O contrato de
  comunicação é o [eventos.md](eventos.md).

## Ordem e processamento concorrente

Eventos de contratos diferentes são processados em paralelo. Os eventos do mesmo contrato são processados um de cada
vez, na ordem em que ocorreram.

**Paralelismo:**
- Cada serviço usa 3 threads, uma por partição. Contratos em partições diferentes são processados ao mesmo tempo.
- O paralelismo máximo de um grupo é o número de partições; threads ou instâncias além disso ficam ociosas.
- O custo da ordem: uma mensagem lenta ou em retentativa atrasa os outros contratos da mesma partição.

**Reputação: o mesmo freelancer em partições diferentes.** Notificação e auditoria só inserem linhas, mas a reputação
atualiza a linha do freelancer, e contratos do mesmo freelancer podem estar em partições diferentes. Sem proteção,
duas threads liam o mesmo valor e uma atualização se perdia. O `ReputacaoService` resolve assim:

```text
1. insert ... on conflict (freelancer_id) do nothing   -> garante que a linha existe
2. select ... for no key update                        -> trava a linha até o commit
3. reputacao.aplicar(evento)                           -> a regra fica no agregado
```

A segunda thread espera a primeira e lê o valor já atualizado. A trava pessimista foi escolhida no lugar do `@Version`
porque, com ele, cada conflito viraria uma falha de processamento, com retentativas e risco de DLT.

## Mensagens duplicadas

Com entrega *at-least-once*, a mesma mensagem pode chegar mais de uma vez: o relay cai antes de marcar `PUBLICADO`, o
consumidor cai antes de confirmar o offset, ou os offsets do grupo são voltados. Em todos os casos o `eventId` é o
mesmo, e é por ele que cada consumidor reconhece o que já processou.

Cada consumidor tem a tabela `eventos_processados`, com `event_id` como chave primária:

```text
ReceptorIdempotente.processarUmaVez (@Transactional)
  1. insert into eventos_processados (event_id, ...) on conflict (event_id) do nothing
       1 linha  -> evento novo: executa o processamento
       0 linhas -> evento já processado: nada é feito (log <serviço>.evento.duplicado)
  2. commit: o registro do eventId e o efeito são gravados juntos
```

- **Atomicidade:** se o processamento falhar, o rollback desfaz também o registro do `eventId`, e a reentrega processa
  normalmente.
- **Duplicatas ao mesmo tempo:** se duas threads tentam registrar o mesmo `eventId`, o Postgres faz a segunda esperar
  a primeira. Só uma processa.
- **Garantia extra no banco:** `unique (event_id, destinatario_id)` na notificação e `unique (event_id)` na auditoria.

## Tratamento de falhas

| Tipo | Exemplo | Tratamento |
|---|---|---|
| **Mensagem inválida** | JSON quebrado, campo obrigatório ausente, versão desconhecida | sem retentativas, porque nunca vai ser processável: DLT e registro como `INVALIDA` |
| **Falha de processamento** | banco fora do ar, erro inesperado | 4 tentativas com 1 s de intervalo; se todas falharem, DLT e registro como `FALHA` |

Na falha definitiva, o `RecuperadorDeFalhas`:

1. grava a mensagem em `mensagens_com_falha`, no banco do serviço: status, tópico, partição, offset, `eventId`,
   `contratoId`, `correlationId`, conteúdo original e erro;
2. publica a mensagem original na DLT do serviço;
3. registra o log `<serviço>.evento.falha-definitiva`;
4. confirma o offset, e a partição segue com as mensagens seguintes.

A tabela existe além da DLT porque é ela que permite saber, a cada mensagem nova, se o contrato tem falha pendente, e
reprocessar por contrato pela API.

**O que acontece com as outras mensagens:**
- **Outras partições:** não são afetadas, porque cada uma tem a sua thread.
- **Outros contratos da mesma partição:** esperam só durante as retentativas, cerca de 3 s.
- **O mesmo contrato:** enquanto houver mensagem `FALHA` pendente, os eventos seguintes são gravados como
  `ESTACIONADA`, sem efeito, para não passarem na frente do evento que falhou. Mensagens `INVALIDA` não bloqueiam o
  contrato.

**Reprocessamento:** processa as mensagens `FALHA` e `ESTACIONADA` do contrato na ordem de partição e offset, pelo
mesmo caminho do listener, e marca cada uma como `REPROCESSADA`. Se alguma falhar de novo, ele para e o contrato
continua bloqueado.

| Endpoint (`<serviço>` = `notificacoes`, `reputacoes` ou `auditoria`) | Uso |
|---|---|
| `GET /api/<serviço>/falhas?status=FALHA` | consulta as mensagens com falha |
| `POST /api/<serviço>/falhas/contrato/{contratoId}/reprocessamento` | reprocessa as pendências do contrato |
| `POST` / `DELETE /api/<serviço>/simulacoes/falhas/{contratoId}` | liga ou desliga uma falha simulada, para demonstrar o caminho de erro |
