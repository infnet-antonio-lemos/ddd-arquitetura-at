package br.com.freela.reputacao.infrastructure.messaging;

import br.com.freela.reputacao.application.ReputacaoService;
import br.com.freela.reputacao.support.IntegrationTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Duration;
import java.util.UUID;

import static br.com.freela.reputacao.support.MensagensDeContrato.evento;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@IntegrationTest
class ContratoEventosListenerIntegrationTest {
    private static final Duration ESPERA = Duration.ofSeconds(30);

    @Autowired KafkaTemplate<String, String> kafka;
    @Autowired ReputacaoService service;

    private final UUID cliente = UUID.randomUUID();
    private final UUID freelancer = UUID.randomUUID();

    @Test
    void historicoDoFreelancerEhPersistidoAPartirDosEventos() {
        var concluido = UUID.randomUUID();
        var cancelado = UUID.randomUUID();
        kafka.send(evento("ContratoCriado", concluido, cliente, freelancer));
        kafka.send(evento("EntregaRegistrada", concluido, cliente, freelancer));
        kafka.send(evento("ContratoConcluido", concluido, cliente, freelancer));
        kafka.send(evento("ContratoCriado", cancelado, cliente, freelancer));
        kafka.send(evento("ContratoCancelado", cancelado, cliente, freelancer));

        await().atMost(ESPERA).untilAsserted(() -> assertThat(service.buscar(freelancer)).hasValueSatisfying(r -> {
            assertThat(r.getContratosRecebidos()).isEqualTo(2);
            assertThat(r.getContratosConcluidos()).isEqualTo(1);
            assertThat(r.getContratosCancelados()).isEqualTo(1);
            assertThat(r.getValorTotalConcluido()).isEqualByComparingTo("800.00");
        }));
    }

    @Test
    void eventoSemImpactoNaoCriaRegistroDeReputacao() {
        var contratoId = UUID.randomUUID();
        var outroFreelancer = UUID.randomUUID();
        kafka.send(evento("EntregaRegistrada", contratoId, cliente, outroFreelancer));
        kafka.send(evento("ContratoCriado", contratoId, cliente, freelancer));

        await().atMost(ESPERA).until(() -> service.buscar(freelancer).isPresent());
        assertThat(service.buscar(outroFreelancer)).isEmpty();
    }
}
