package br.com.freela.auditoria.infrastructure.messaging;

import br.com.freela.auditoria.domain.EventoRecebido;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.time.Instant;
import java.util.UUID;

@JsonIgnoreProperties(ignoreUnknown = true)
public record ContratoEventoMensagem(UUID eventId, String eventType, Integer eventVersion, Instant occurredAt,
                                     UUID contratoId, String correlationId, String producer) {

    public EventoRecebido paraEvento(String conteudo, EventoRecebido.Origem origem) {
        return new EventoRecebido(
                exigir(eventId, "eventId"),
                exigir(eventType, "eventType"),
                eventVersion,
                exigir(occurredAt, "occurredAt"),
                exigir(contratoId, "contratoId"),
                correlationId,
                producer,
                conteudo,
                origem);
    }

    private static <T> T exigir(T valor, String campo) {
        if (valor == null) throw new EventoInvalidoException("campo obrigatório ausente: " + campo);
        return valor;
    }
}
