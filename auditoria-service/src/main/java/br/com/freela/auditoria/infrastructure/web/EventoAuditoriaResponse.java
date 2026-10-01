package br.com.freela.auditoria.infrastructure.web;

import br.com.freela.auditoria.domain.EventoAuditoria;
import com.fasterxml.jackson.annotation.JsonRawValue;

import java.time.Instant;
import java.util.UUID;

public record EventoAuditoriaResponse(UUID id, Long sequencia, UUID eventId, String eventType, Integer eventVersion,
                                      Instant occurredAt, UUID contratoId, String correlationId, String producer,
                                      String topico, int particao, long offset, Instant recebidoEm,
                                      @JsonRawValue String conteudo) {
    static EventoAuditoriaResponse from(EventoAuditoria e) {
        return new EventoAuditoriaResponse(e.getId(), e.getSequencia(), e.getEventId(), e.getEventType(),
                e.getEventVersion(), e.getOccurredAt(), e.getContratoId(), e.getCorrelationId(), e.getProducer(),
                e.getTopico(), e.getParticao(), e.getKafkaOffset(), e.getRecebidoEm(), e.getConteudo());
    }
}
