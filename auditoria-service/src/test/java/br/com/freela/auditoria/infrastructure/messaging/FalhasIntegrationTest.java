package br.com.freela.auditoria.infrastructure.messaging;

import br.com.freela.auditoria.application.AuditoriaService;
import br.com.freela.auditoria.domain.EventoAuditoria;
import br.com.freela.auditoria.support.IntegrationTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Duration;
import java.util.UUID;

import static br.com.freela.auditoria.infrastructure.messaging.MensagemComFalha.Status.*;
import static br.com.freela.auditoria.support.LeitorDeTopico.registrosComChave;
import static br.com.freela.auditoria.support.MensagensDeContrato.evento;
import static br.com.freela.auditoria.support.MensagensDeContrato.registro;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static org.awaitility.Awaitility.await;

@IntegrationTest
class FalhasIntegrationTest {
    private static final Duration ESPERA = Duration.ofSeconds(30);
    private static final String DLT = "freela.contrato.eventos.v1.auditoria-service.dlt";

    @Autowired KafkaTemplate<String, String> kafka;
    @Autowired ConsumerFactory<String, String> consumidores;
    @Autowired AuditoriaService service;
    @Autowired MensagemComFalhaRepository falhas;
    @Autowired ProcessadorDeEventos processador;
    @Autowired SimuladorDeFalhas simulador;

    private final UUID cliente = UUID.randomUUID();
    private final UUID freelancer = UUID.randomUUID();

    @Test
    void mensagemInvalidaFicaRegistradaENaDlt() {
        var contratoId = UUID.randomUUID();

        kafka.send(registro(UUID.randomUUID(), "ContratoCriado", contratoId, "{\"eventId\":\"nao-e-uuid\"}"));

        assertThat(registrosComChave(consumidores, DLT, contratoId)).hasSize(1);
        await().atMost(ESPERA).untilAsserted(() -> assertThat(falhas.findByStatusOrderByIdAsc(INVALIDA))
                .filteredOn(f -> contratoId.equals(f.getContratoId())).hasSize(1));
        assertThat(service.porContrato(contratoId)).isEmpty();
    }

    @Test
    void falhaEstacionaOsEventosSeguintesDoContratoEOReprocessamentoMantemAOrdemDeRegistro() {
        var contratoComFalha = UUID.randomUUID();
        var outroContrato = UUID.randomUUID();
        simulador.ativar(contratoComFalha);

        kafka.send(evento("ContratoCriado", contratoComFalha, cliente, freelancer));
        kafka.send(evento("EntregaRegistrada", contratoComFalha, cliente, freelancer));
        kafka.send(evento("ContratoCriado", outroContrato, cliente, freelancer));

        await().atMost(ESPERA).untilAsserted(() -> assertThat(falhas.pendentesDoContrato(contratoComFalha))
                .extracting(MensagemComFalha::getEventType, MensagemComFalha::getStatus)
                .containsExactly(tuple("ContratoCriado", FALHA), tuple("EntregaRegistrada", ESTACIONADA)));
        await().atMost(ESPERA).untilAsserted(() -> assertThat(service.porContrato(outroContrato)).hasSize(1));
        assertThat(service.porContrato(contratoComFalha)).isEmpty();

        simulador.desativar(contratoComFalha);
        assertThat(processador.reprocessar(contratoComFalha).reprocessadas()).isEqualTo(2);
        kafka.send(evento("ContratoConcluido", contratoComFalha, cliente, freelancer));

        await().atMost(ESPERA).untilAsserted(() -> assertThat(service.porContrato(contratoComFalha))
                .extracting(EventoAuditoria::getEventType)
                .containsExactly("ContratoCriado", "EntregaRegistrada", "ContratoConcluido"));
    }
}
