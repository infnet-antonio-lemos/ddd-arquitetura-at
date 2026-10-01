package br.com.freela.contrato.domain.event;

import br.com.freela.contrato.domain.model.ClienteId;
import br.com.freela.contrato.domain.model.Contrato;
import br.com.freela.contrato.domain.model.ContratoId;
import br.com.freela.contrato.domain.model.Dinheiro;
import br.com.freela.contrato.domain.model.FreelancerId;
import br.com.freela.contrato.domain.model.Titulo;
import br.com.freela.contrato.domain.model.StatusContrato;
import java.time.Instant;
import java.util.UUID;

public record EntregaRegistrada(UUID eventId, Instant occurredAt, ContratoId contratoId,
                                ClienteId clienteId, FreelancerId freelancerId, Titulo titulo, Dinheiro valor)
        implements ContratoEvento {
    public static EntregaRegistrada novo(Contrato c) {
        return new EntregaRegistrada(UUID.randomUUID(), Instant.now(), c.id(), c.clienteId(), c.freelancerId(), c.titulo(), c.valor());
    }
    @Override public String eventType() { return "EntregaRegistrada"; }
    @Override public StatusContrato status() { return StatusContrato.ENTREGA_REGISTRADA; }
}
