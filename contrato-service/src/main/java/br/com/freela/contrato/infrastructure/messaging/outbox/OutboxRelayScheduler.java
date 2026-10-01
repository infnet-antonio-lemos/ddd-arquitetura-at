package br.com.freela.contrato.infrastructure.messaging.outbox;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

@Configuration
@EnableScheduling
@ConditionalOnProperty(name = "freela.outbox.relay.habilitado", havingValue = "true", matchIfMissing = true)
public class OutboxRelayScheduler {
    private final OutboxRelayService relay;

    public OutboxRelayScheduler(OutboxRelayService relay) { this.relay = relay; }

    @Scheduled(fixedDelayString = "${freela.outbox.relay.intervalo-ms:1000}")
    public void executar() {
        relay.publicarPendentes();
    }
}
