package br.com.freela.contrato.domain.event;

import br.com.freela.contrato.domain.model.ClienteId;
import br.com.freela.contrato.domain.model.ContratoId;
import br.com.freela.contrato.domain.model.Dinheiro;
import br.com.freela.contrato.domain.model.FreelancerId;
import br.com.freela.contrato.domain.model.StatusContrato;
import br.com.freela.contrato.domain.model.Titulo;
import br.com.freela.contrato.domain.shared.DomainEvent;

/**
 * Eventos do ciclo de vida do Aggregate Contrato.
 * Cada evento carrega o retrato do contrato no momento em que ocorreu, para que os serviços
 * consumidores não precisem consultar o contrato-service.
 */
public sealed interface ContratoEvento extends DomainEvent
        permits ContratoCriado, EntregaRegistrada, ContratoConcluido, ContratoCancelado {
    ContratoId contratoId();
    ClienteId clienteId();
    FreelancerId freelancerId();
    Titulo titulo();
    Dinheiro valor();
    /** Status do contrato depois do evento. */
    StatusContrato status();
}
