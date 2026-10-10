# Arquitetura

## Visão geral

```text
 Cliente HTTP
     |  X-Correlation-Id (opcional)
     v
 +-------------+     Eureka      +-----------------------------------------------+
 | API Gateway | --------------> | contrato-service                              |
 |    :8080    |                 |                                               |
 +-------------+                 |  Controller -> ContratoApplicationService     |
                                 |                   |  (uma transação local)     |
                                 |                   +-> contratos      (UPDATE)  |
                                 |                   +-> outbox_eventos (INSERT)  |
                                 |                                               |
                                 |  OutboxRelay (@Scheduled, 1s)                 |
                                 |    lê PENDENTE em ordem -> publica -> PUBLICADO|
                                 +----------------------|------------------------+
                                                        v
                                     Kafka: freela.contrato.eventos.v1
                                     (3 partições, chave = contratoId)
                                                        |
                         +------------------------------+------------------------------+
                         v                              v                              v
                notificacao-service             reputacao-service              auditoria-service
                (grupo próprio)                  (grupo próprio)                (grupo próprio)
```

Cada consumidor tem seu próprio banco (`notificacao_db`, `reputacao_db`, `auditoria_db`) e seu próprio grupo de
consumo, então todos recebem todos os eventos:

| Serviço | Reage a | Grava |
|---|---|---|
| `notificacao-service` | os 4 eventos | uma notificação por destinatário (cliente e/ou freelancer) |
| `reputacao-service` | criado, concluído, cancelado | contadores e valor total concluído por freelancer |
| `auditoria-service` | qualquer evento do tópico | o evento, a mensagem original e a origem (partição/offset) |

O efeito de cada evento em cada serviço está em [eventos.md](eventos.md).

Logs centralizados (Loki e Grafana, `http://localhost:3000`), correlação (`X-Correlation-Id`) e rastreamento
distribuído (Zipkin, `http://localhost:9411`) estão em [observabilidade.md](observabilidade.md).

## Por que a comunicação é assíncrona

- **Desacoplamento temporal:** o `contrato-service` não depende de notificação, reputação ou auditoria estarem no ar.
  Uma operação no contrato é concluída mesmo que todos os consumidores (ou o próprio Kafka) estejam indisponíveis.
- **Sem chamadas HTTP entre serviços:** o produtor não conhece os consumidores. Um novo interessado passa a consumir o
  tópico sem nenhuma alteração no `contrato-service`.
- **Eventos autocontidos:** cada evento carrega o retrato do contrato (cliente, freelancer, título, valor, status), para
  que nenhum consumidor precise consultar o `contrato-service`.