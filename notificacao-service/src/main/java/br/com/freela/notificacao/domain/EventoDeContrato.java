package br.com.freela.notificacao.domain;

import java.util.UUID;

/** O que aconteceu com um contrato, na visão do serviço de notificação. */
public record EventoDeContrato(
        UUID eventId,
        String eventType,
        UUID contratoId,
        UUID clienteId,
        UUID freelancerId,
        String titulo) {

    public UUID participante(Papel papel) {
        return papel == Papel.CLIENTE ? clienteId : freelancerId;
    }
}
