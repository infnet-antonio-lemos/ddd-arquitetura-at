package br.com.freela.reputacao.infrastructure.messaging;

import br.com.freela.reputacao.application.ReputacaoService;
import br.com.freela.reputacao.support.IntegrationTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Duration;
import java.util.UUID;
import java.util.stream.IntStream;

import static br.com.freela.reputacao.support.MensagensDeContrato.evento;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@IntegrationTest
class ReputacaoConcorrenciaIntegrationTest {
    private static final int CONTRATOS = 30;

    @Autowired KafkaTemplate<String, String> kafka;
    @Autowired ReputacaoService service;

    @Test
    void contratosDoMesmoFreelancerEmParticoesDiferentesNaoPerdemAtualizacao() {
        var cliente = UUID.randomUUID();
        var freelancer = UUID.randomUUID();
        var contratos = IntStream.range(0, CONTRATOS).mapToObj(i -> UUID.randomUUID()).toList();

        contratos.forEach(id -> kafka.send(evento("ContratoCriado", id, cliente, freelancer)));
        contratos.forEach(id -> kafka.send(evento("ContratoConcluido", id, cliente, freelancer)));

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(service.buscar(freelancer)).hasValueSatisfying(r -> {
            assertThat(r.getContratosRecebidos()).isEqualTo(CONTRATOS);
            assertThat(r.getContratosConcluidos()).isEqualTo(CONTRATOS);
            assertThat(r.getValorTotalConcluido()).isEqualByComparingTo("24000.00");
        }));
    }
}
