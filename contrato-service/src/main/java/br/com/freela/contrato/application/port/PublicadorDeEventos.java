package br.com.freela.contrato.application.port;

import br.com.freela.contrato.domain.shared.DomainEvent;
import java.util.List;

/** Publica eventos de domínio de forma atômica com a transação que alterou o Aggregate. */
public interface PublicadorDeEventos {
    void publicar(List<DomainEvent> eventos);
}
