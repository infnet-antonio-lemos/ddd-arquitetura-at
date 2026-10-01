package br.com.freela.notificacao.infrastructure.messaging;

import br.com.freela.notificacao.application.NotificacaoService;
import br.com.freela.notificacao.domain.Notificacao;
import br.com.freela.notificacao.domain.Papel;
import br.com.freela.notificacao.support.IntegrationTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.UUID;

import static br.com.freela.notificacao.support.ConsumoKafka.aguardarConsumoCompleto;
import static br.com.freela.notificacao.support.MensagensDeContrato.json;
import static br.com.freela.notificacao.support.MensagensDeContrato.registro;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

@IntegrationTest
class MensagemDuplicadaIntegrationTest {
    @Autowired KafkaTemplate<String, String> kafka;
    @Autowired KafkaAdmin kafkaAdmin;
    @Autowired NotificacaoService service;

    private final UUID contratoId = UUID.randomUUID();
    private final UUID cliente = UUID.randomUUID();
    private final UUID freelancer = UUID.randomUUID();

    @Test
    void mesmaMensagemRecebidaDuasVezesNotificaCadaDestinatarioUmaVez() {
        var eventId = UUID.randomUUID();
        var mensagem = json(eventId, "ContratoCriado", 1, contratoId, cliente, freelancer);

        kafka.send(registro(eventId, "ContratoCriado", contratoId, mensagem)).join();
        kafka.send(registro(eventId, "ContratoCriado", contratoId, mensagem)).join();
        aguardarConsumoCompleto(kafkaAdmin, "notificacao-service");

        assertThat(service.porContrato(contratoId))
                .extracting(Notificacao::getPapel, Notificacao::getDestinatarioId)
                .containsExactlyInAnyOrder(tuple(Papel.CLIENTE, cliente), tuple(Papel.FREELANCER, freelancer));
    }
}
