package br.com.freela.contrato.infrastructure.messaging;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/** Mensagem publicada no Kafka (contrato de comunicação descrito em docs/eventos.md). */
public record ContratoEventoMensagem(
        UUID eventId,
        String eventType,
        int eventVersion,
        Instant occurredAt,
        UUID contratoId,
        String correlationId,
        String producer,
        Payload payload) {

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Payload(
            UUID contratoId,
            UUID clienteId,
            UUID freelancerId,
            String titulo,
            BigDecimal valor,
            String status,
            String statusAnterior) {}
}
