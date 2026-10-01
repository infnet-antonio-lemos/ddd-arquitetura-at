package br.com.freela.contrato.infrastructure.messaging.outbox;

import br.com.freela.contrato.infrastructure.observabilidade.ContextoMdc;
import br.com.freela.contrato.infrastructure.observabilidade.Correlacao;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Limit;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;

/**
 * Lê a outbox em ordem de inserção e publica no Kafka.
 * Um único relay ativo por vez (advisory lock) e parada na primeira falha preservam a ordem por contrato.
 */
@Service
public class OutboxRelayService {
    private static final Logger log = LoggerFactory.getLogger(OutboxRelayService.class);
    private static final long CHAVE_LOCK_RELAY = 1_947_382_001L;

    private final SpringDataOutboxRepository outbox;
    private final KafkaTemplate<String, String> kafka;
    private final JdbcClient jdbc;
    private final int tamanhoLote;
    private final long timeoutEnvioMs;

    public OutboxRelayService(SpringDataOutboxRepository outbox, KafkaTemplate<String, String> kafka, JdbcClient jdbc,
                              @Value("${freela.outbox.relay.tamanho-lote:100}") int tamanhoLote,
                              @Value("${freela.outbox.relay.timeout-envio-ms:10000}") long timeoutEnvioMs) {
        this.outbox = outbox;
        this.kafka = kafka;
        this.jdbc = jdbc;
        this.tamanhoLote = tamanhoLote;
        this.timeoutEnvioMs = timeoutEnvioMs;
    }

    @Transactional
    public int publicarPendentes() {
        if (!adquirirLock()) {
            log.debug("outbox.relay.lock-indisponivel");
            return 0;
        }
        var pendentes = outbox.findByStatusOrderByIdAsc(StatusOutbox.PENDENTE, Limit.of(tamanhoLote));
        int publicados = 0;
        for (var evento : pendentes) {
            try (var mdc = contextoDeLog(evento)) {
                if (!publicar(evento)) break;
            }
            publicados++;
        }
        return publicados;
    }

    private boolean publicar(OutboxEventoJpaEntity evento) {
        try {
            var metadata = kafka.send(paraRecord(evento))
                    .orTimeout(timeoutEnvioMs, TimeUnit.MILLISECONDS)
                    .join()
                    .getRecordMetadata();
            evento.marcarPublicado();
            log.info("outbox.evento.publicado eventId={} eventType={} contratoId={} topico={} particao={} offset={}",
                    evento.getEventId(), evento.getEventType(), evento.getAggregateId(),
                    metadata.topic(), metadata.partition(), metadata.offset());
            return true;
        } catch (RuntimeException ex) {
            var erro = ex instanceof CompletionException && ex.getCause() != null ? ex.getCause() : ex;
            evento.registrarFalha(erro.getClass().getSimpleName() + ": " + erro.getMessage());
            log.warn("outbox.publicacao.falha eventId={} eventType={} contratoId={} tentativas={} erro={}",
                    evento.getEventId(), evento.getEventType(), evento.getAggregateId(), evento.getTentativas(),
                    erro.getClass().getSimpleName());
            return false;
        }
    }

    private static ContextoMdc contextoDeLog(OutboxEventoJpaEntity evento) {
        return ContextoMdc.com(Map.of(
                Correlacao.MDC_CORRELATION_ID, evento.getCorrelationId(),
                Correlacao.MDC_CONTRATO_ID, evento.getAggregateId().toString(),
                Correlacao.MDC_EVENT_ID, evento.getEventId().toString(),
                Correlacao.MDC_EVENT_TYPE, evento.getEventType()));
    }

    private ProducerRecord<String, String> paraRecord(OutboxEventoJpaEntity evento) {
        var record = new ProducerRecord<String, String>(evento.getTopico(), evento.getChave(), evento.getPayload());
        header(record, "eventId", evento.getEventId().toString());
        header(record, "eventType", evento.getEventType());
        header(record, "contratoId", evento.getAggregateId().toString());
        header(record, "correlationId", evento.getCorrelationId());
        return record;
    }

    private static void header(ProducerRecord<String, String> record, String nome, String valor) {
        record.headers().add(nome, valor.getBytes(StandardCharsets.UTF_8));
    }

    private boolean adquirirLock() {
        return Boolean.TRUE.equals(jdbc.sql("select pg_try_advisory_xact_lock(?)")
                .param(CHAVE_LOCK_RELAY).query(Boolean.class).single());
    }
}
