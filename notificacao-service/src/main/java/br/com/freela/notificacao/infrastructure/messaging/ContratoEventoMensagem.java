package br.com.freela.notificacao.infrastructure.messaging;

import br.com.freela.notificacao.domain.EventoDeContrato;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.UUID;

@JsonIgnoreProperties(ignoreUnknown = true)
public record ContratoEventoMensagem(UUID eventId, String eventType, Integer eventVersion, UUID contratoId, Payload payload) {
    public static final int VERSAO_SUPORTADA = 1;

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Payload(UUID clienteId, UUID freelancerId, String titulo) {}

    public EventoDeContrato paraEvento() {
        if (exigir(eventVersion, "eventVersion") != VERSAO_SUPORTADA) {
            throw new EventoInvalidoException("eventVersion não suportada: " + eventVersion);
        }
        var dados = exigir(payload, "payload");
        return new EventoDeContrato(
                exigir(eventId, "eventId"),
                exigir(eventType, "eventType"),
                exigir(contratoId, "contratoId"),
                exigir(dados.clienteId(), "payload.clienteId"),
                exigir(dados.freelancerId(), "payload.freelancerId"),
                exigir(dados.titulo(), "payload.titulo"));
    }

    private static <T> T exigir(T valor, String campo) {
        if (valor == null) throw new EventoInvalidoException("campo obrigatório ausente: " + campo);
        return valor;
    }
}
