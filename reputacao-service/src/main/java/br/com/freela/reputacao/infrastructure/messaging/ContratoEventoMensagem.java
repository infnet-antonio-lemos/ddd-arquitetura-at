package br.com.freela.reputacao.infrastructure.messaging;

import br.com.freela.reputacao.domain.EventoDeContrato;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.math.BigDecimal;
import java.util.UUID;

@JsonIgnoreProperties(ignoreUnknown = true)
public record ContratoEventoMensagem(UUID eventId, String eventType, Integer eventVersion, UUID contratoId, Payload payload) {
    public static final int VERSAO_SUPORTADA = 1;

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Payload(UUID freelancerId, BigDecimal valor) {}

    public EventoDeContrato paraEvento() {
        if (exigir(eventVersion, "eventVersion") != VERSAO_SUPORTADA) {
            throw new EventoInvalidoException("eventVersion não suportada: " + eventVersion);
        }
        var dados = exigir(payload, "payload");
        return new EventoDeContrato(
                exigir(eventId, "eventId"),
                exigir(eventType, "eventType"),
                exigir(contratoId, "contratoId"),
                exigir(dados.freelancerId(), "payload.freelancerId"),
                exigir(dados.valor(), "payload.valor"));
    }

    private static <T> T exigir(T valor, String campo) {
        if (valor == null) throw new EventoInvalidoException("campo obrigatório ausente: " + campo);
        return valor;
    }
}
