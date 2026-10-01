package br.com.freela.auditoria.infrastructure.messaging;

import br.com.freela.auditoria.application.AuditoriaService;
import br.com.freela.auditoria.support.IntegrationTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.UUID;

import static br.com.freela.auditoria.support.ConsumoKafka.aguardarConsumoCompleto;
import static br.com.freela.auditoria.support.MensagensDeContrato.json;
import static br.com.freela.auditoria.support.MensagensDeContrato.registro;
import static org.assertj.core.api.Assertions.assertThat;

@IntegrationTest
class MensagemDuplicadaIntegrationTest {
    @Autowired KafkaTemplate<String, String> kafka;
    @Autowired KafkaAdmin kafkaAdmin;
    @Autowired AuditoriaService service;

    @Test
    void mesmaMensagemRecebidaDuasVezesGeraUmUnicoRegistro() {
        var eventId = UUID.randomUUID();
        var contratoId = UUID.randomUUID();
        var mensagem = json(eventId, "ContratoCriado", 1, contratoId, UUID.randomUUID(), UUID.randomUUID());

        var primeira = kafka.send(registro(eventId, "ContratoCriado", contratoId, mensagem)).join().getRecordMetadata();
        kafka.send(registro(eventId, "ContratoCriado", contratoId, mensagem)).join();
        aguardarConsumoCompleto(kafkaAdmin, "auditoria-service");

        assertThat(service.porContrato(contratoId)).singleElement().satisfies(e -> {
            assertThat(e.getEventId()).isEqualTo(eventId);
            assertThat(e.getKafkaOffset()).isEqualTo(primeira.offset());
        });
    }
}
