package br.com.freela.contrato.infrastructure.messaging.outbox;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.slf4j.MDC;
import org.springframework.data.domain.Limit;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

class OutboxRelayServiceTest {
    private final SpringDataOutboxRepository outbox = mock(SpringDataOutboxRepository.class);
    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, String> kafka = mock(KafkaTemplate.class);
    private final JdbcClient jdbc = mock(JdbcClient.class, RETURNS_DEEP_STUBS);
    private final OutboxRelayService relay = new OutboxRelayService(outbox, kafka, jdbc, 100, 1000);

    private final OutboxEventoJpaEntity primeiro = evento("ContratoCriado");
    private final OutboxEventoJpaEntity segundo = evento("EntregaRegistrada");

    @BeforeEach
    void setUp() {
        lockDisponivel(true);
        when(outbox.findByStatusOrderByIdAsc(eq(StatusOutbox.PENDENTE), any(Limit.class))).thenReturn(List.of(primeiro, segundo));
    }

    @Test
    void devePublicarEmOrdemEMarcarComoPublicado() {
        when(kafka.send(qualquerRecord())).thenAnswer(inv -> sucesso(inv.getArgument(0)));

        assertThat(relay.publicarPendentes()).isEqualTo(2);

        ArgumentCaptor<ProducerRecord<String, String>> enviados = ArgumentCaptor.captor();
        verify(kafka, times(2)).send(enviados.capture());
        assertThat(enviados.getAllValues()).extracting(r -> new String(r.headers().lastHeader("eventType").value()))
                .containsExactly("ContratoCriado", "EntregaRegistrada");
        assertThat(primeiro.getStatus()).isEqualTo(StatusOutbox.PUBLICADO);
        assertThat(segundo.getStatus()).isEqualTo(StatusOutbox.PUBLICADO);
    }

    @Test
    void deveInterromperOLoteNaPrimeiraFalhaParaPreservarAOrdem() {
        when(kafka.send(qualquerRecord())).thenReturn(CompletableFuture.failedFuture(new KafkaException("Send failed")));

        assertThat(relay.publicarPendentes()).isZero();

        verify(kafka, times(1)).send(qualquerRecord());
        assertThat(primeiro.getStatus()).isEqualTo(StatusOutbox.PENDENTE);
        assertThat(primeiro.getTentativas()).isEqualTo(1);
        assertThat(primeiro.getUltimoErro()).isEqualTo("KafkaException: Send failed");
        assertThat(segundo.getTentativas()).isZero();
    }

    @Test
    void deveTratarFalhaLancadaDiretamentePeloSend() {
        when(kafka.send(qualquerRecord())).thenThrow(new KafkaException("Metadata indisponível"));

        assertThat(relay.publicarPendentes()).isZero();

        assertThat(primeiro.getUltimoErro()).isEqualTo("KafkaException: Metadata indisponível");
        assertThat(segundo.getTentativas()).isZero();
    }

    @Test
    void naoDevePublicarSemOLockDoRelay() {
        lockDisponivel(false);

        assertThat(relay.publicarPendentes()).isZero();

        verifyNoInteractions(outbox, kafka);
    }

    @Test
    void naoDeveDeixarChavesNoMdcDepoisDoLote() {
        when(kafka.send(qualquerRecord())).thenReturn(CompletableFuture.failedFuture(new KafkaException("x")));

        relay.publicarPendentes();

        assertThat(MDC.getCopyOfContextMap()).isNullOrEmpty();
    }

    private static ProducerRecord<String, String> qualquerRecord() {
        return any();
    }

    private void lockDisponivel(boolean disponivel) {
        when(jdbc.sql(anyString()).param(any()).query(Boolean.class).single()).thenReturn(disponivel);
    }

    private static OutboxEventoJpaEntity evento(String tipo) {
        var contratoId = UUID.randomUUID();
        return new OutboxEventoJpaEntity(UUID.randomUUID(), contratoId, tipo, "freela.contrato.eventos.v1",
                contratoId.toString(), "{}", "teste-relay");
    }

    private static CompletableFuture<SendResult<String, String>> sucesso(ProducerRecord<String, String> record) {
        var metadata = new RecordMetadata(new TopicPartition(record.topic(), 0), 0, 0, 0, 0, 0);
        return CompletableFuture.completedFuture(new SendResult<>(record, metadata));
    }
}
