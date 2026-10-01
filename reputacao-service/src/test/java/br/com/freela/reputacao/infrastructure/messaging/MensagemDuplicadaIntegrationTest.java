package br.com.freela.reputacao.infrastructure.messaging;

import br.com.freela.reputacao.application.ReputacaoService;
import br.com.freela.reputacao.support.IntegrationTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.UUID;

import static br.com.freela.reputacao.support.ConsumoKafka.aguardarConsumoCompleto;
import static br.com.freela.reputacao.support.MensagensDeContrato.json;
import static br.com.freela.reputacao.support.MensagensDeContrato.registro;
import static org.assertj.core.api.Assertions.assertThat;

@IntegrationTest
class MensagemDuplicadaIntegrationTest {
    @Autowired KafkaTemplate<String, String> kafka;
    @Autowired KafkaAdmin kafkaAdmin;
    @Autowired ReputacaoService service;

    private final UUID contratoId = UUID.randomUUID();
    private final UUID cliente = UUID.randomUUID();
    private final UUID freelancer = UUID.randomUUID();

    @Test
    void eventosRecebidosDuasVezesNaoIncrementamOsContadoresDeNovo() {
        publicarDuasVezes("ContratoCriado");
        publicarDuasVezes("ContratoConcluido");
        aguardarConsumoCompleto(kafkaAdmin, "reputacao-service");

        assertThat(service.buscar(freelancer)).hasValueSatisfying(r -> {
            assertThat(r.getContratosRecebidos()).isEqualTo(1);
            assertThat(r.getContratosConcluidos()).isEqualTo(1);
            assertThat(r.getValorTotalConcluido()).isEqualByComparingTo("800.00");
        });
    }

    private void publicarDuasVezes(String eventType) {
        var eventId = UUID.randomUUID();
        var mensagem = json(eventId, eventType, 1, contratoId, cliente, freelancer);
        kafka.send(registro(eventId, eventType, contratoId, mensagem)).join();
        kafka.send(registro(eventId, eventType, contratoId, mensagem)).join();
    }
}
