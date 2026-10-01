package br.com.freela.auditoria.infrastructure.messaging;

import br.com.freela.auditoria.application.AuditoriaService;
import br.com.freela.auditoria.domain.EventoAuditoria;
import br.com.freela.auditoria.support.IntegrationTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Duration;
import java.util.UUID;

import static br.com.freela.auditoria.support.MensagensDeContrato.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@IntegrationTest
class ContratoEventosListenerIntegrationTest {
    private static final Duration ESPERA = Duration.ofSeconds(30);

    @Autowired KafkaTemplate<String, String> kafka;
    @Autowired AuditoriaService service;

    private final UUID contratoId = UUID.randomUUID();
    private final UUID cliente = UUID.randomUUID();
    private final UUID freelancer = UUID.randomUUID();

    @Test
    void registraOEventoComOsDadosConsultaveis() {
        var eventId = UUID.randomUUID();
        var mensagem = json(eventId, "ContratoCriado", 1, contratoId, cliente, freelancer);

        var enviado = kafka.send(registro(eventId, "ContratoCriado", contratoId, mensagem)).join().getRecordMetadata();

        await().atMost(ESPERA).until(() -> service.porEvento(eventId).isPresent());
        var auditado = service.porEvento(eventId).orElseThrow();
        assertThat(auditado.getEventType()).isEqualTo("ContratoCriado");
        assertThat(auditado.getEventVersion()).isEqualTo(1);
        assertThat(auditado.getContratoId()).isEqualTo(contratoId);
        assertThat(auditado.getOccurredAt()).isNotNull();
        assertThat(auditado.getRecebidoEm()).isAfterOrEqualTo(auditado.getOccurredAt());
        assertThat(auditado.getCorrelationId()).isEqualTo(CORRELATION_ID);
        assertThat(auditado.getProducer()).isEqualTo("contrato-service");
        assertThat(auditado.getConteudo()).isEqualTo(mensagem);
        assertThat(auditado.getTopico()).isEqualTo(TOPICO);
        assertThat(auditado.getParticao()).isEqualTo(enviado.partition());
        assertThat(auditado.getKafkaOffset()).isEqualTo(enviado.offset());
        assertThat(service.porCorrelacao(CORRELATION_ID)).extracting(EventoAuditoria::getEventId).contains(eventId);
    }

    @Test
    void historicoDoContratoFicaNaOrdemEmQueFoiConsumido() {
        kafka.send(evento("ContratoCriado", contratoId, cliente, freelancer));
        kafka.send(evento("EntregaRegistrada", contratoId, cliente, freelancer));
        kafka.send(evento("ContratoConcluido", contratoId, cliente, freelancer));

        await().atMost(ESPERA).untilAsserted(() -> assertThat(service.porContrato(contratoId))
                .extracting(EventoAuditoria::getEventType)
                .containsExactly("ContratoCriado", "EntregaRegistrada", "ContratoConcluido"));
    }

    @Test
    void registraTambemTiposEVersoesQueOsOutrosServicosNaoConhecem() {
        var eventId = UUID.randomUUID();
        kafka.send(registro(eventId, "ContratoReaberto", contratoId,
                json(eventId, "ContratoReaberto", 2, contratoId, cliente, freelancer)));

        await().atMost(ESPERA).untilAsserted(() -> assertThat(service.porEvento(eventId)).hasValueSatisfying(e -> {
            assertThat(e.getEventType()).isEqualTo("ContratoReaberto");
            assertThat(e.getEventVersion()).isEqualTo(2);
        }));
    }
}
