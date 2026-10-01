package br.com.freela.contrato.infrastructure.messaging;

import br.com.freela.contrato.domain.event.ContratoCancelado;
import br.com.freela.contrato.domain.event.ContratoEvento;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

@Component
public class ContratoEventoMapper {
    public static final int VERSAO_ENVELOPE = 1;

    private final JsonMapper json;
    private final String produtor;

    public ContratoEventoMapper(JsonMapper json, @Value("${spring.application.name}") String produtor) {
        this.json = json;
        this.produtor = produtor;
    }

    public ContratoEventoMensagem paraMensagem(ContratoEvento evento, String correlationId) {
        var statusAnterior = evento instanceof ContratoCancelado cancelado ? cancelado.statusAnterior().name() : null;
        var payload = new ContratoEventoMensagem.Payload(
                evento.contratoId().valor(),
                evento.clienteId().valor(),
                evento.freelancerId().valor(),
                evento.titulo().valor(),
                evento.valor().valor(),
                evento.status().name(),
                statusAnterior);
        return new ContratoEventoMensagem(evento.eventId(), evento.eventType(), VERSAO_ENVELOPE, evento.occurredAt(),
                evento.contratoId().valor(), correlationId, produtor, payload);
    }

    public String paraJson(ContratoEventoMensagem mensagem) {
        return json.writeValueAsString(mensagem);
    }
}
