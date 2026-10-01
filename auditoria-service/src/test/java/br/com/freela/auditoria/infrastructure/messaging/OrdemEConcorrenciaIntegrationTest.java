package br.com.freela.auditoria.infrastructure.messaging;

import br.com.freela.auditoria.application.AuditoriaService;
import br.com.freela.auditoria.domain.EventoAuditoria;
import br.com.freela.auditoria.support.IntegrationTest;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;

import static br.com.freela.auditoria.support.MensagensDeContrato.evento;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@IntegrationTest
class OrdemEConcorrenciaIntegrationTest {
    private static final List<String> CICLO = List.of("ContratoCriado", "EntregaRegistrada", "ContratoConcluido");

    @Autowired KafkaTemplate<String, String> kafka;
    @Autowired AuditoriaService service;
    @Autowired KafkaListenerEndpointRegistry registry;

    @Test
    void cadaParticaoEhConsumidaPorUmaThreadPropria() {
        var container = (ConcurrentMessageListenerContainer<?, ?>) registry.getListenerContainer("auditoria-contrato-eventos");

        assertThat(container.getConcurrency()).isEqualTo(3);
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(container.getContainers())
                .allSatisfy(c -> assertThat(c.getAssignedPartitions()).hasSize(1))
                .flatExtracting(c -> c.getAssignedPartitions().stream().map(TopicPartition::partition).toList())
                .containsExactlyInAnyOrder(0, 1, 2));
    }

    @Test
    void eventosDoMesmoContratoSaoProcessadosEmOrdemComContratosEmParalelo() {
        var cliente = UUID.randomUUID();
        var freelancer = UUID.randomUUID();
        var contratos = IntStream.range(0, 30).mapToObj(i -> UUID.randomUUID()).toList();

        CICLO.forEach(tipo -> contratos.forEach(id -> kafka.send(evento(tipo, id, cliente, freelancer))));

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(contratos).allSatisfy(id -> assertThat(service.porContrato(id)).hasSize(3)));
        for (var id : contratos) {
            var porOrdemDeProcessamento = service.porContrato(id);
            assertThat(porOrdemDeProcessamento).extracting(EventoAuditoria::getEventType).containsExactlyElementsOf(CICLO);
            assertThat(porOrdemDeProcessamento).extracting(EventoAuditoria::getKafkaOffset).isSorted();
            assertThat(porOrdemDeProcessamento).extracting(EventoAuditoria::getParticao).containsOnly(porOrdemDeProcessamento.getFirst().getParticao());
        }
        assertThat(contratos).extracting(id -> service.porContrato(id).getFirst().getParticao()).containsOnly(0, 1, 2).contains(0, 1, 2);
    }
}
