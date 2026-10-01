package br.com.freela.auditoria.domain;

import java.time.Instant;
import java.util.UUID;

/** Evento recebido da plataforma, com a mensagem original e a posição de onde foi lido. */
public record EventoRecebido(
        UUID eventId,
        String eventType,
        Integer eventVersion,
        Instant occurredAt,
        UUID contratoId,
        String correlationId,
        String producer,
        String conteudo,
        Origem origem) {

    public record Origem(String topico, int particao, long offset) {}
}
