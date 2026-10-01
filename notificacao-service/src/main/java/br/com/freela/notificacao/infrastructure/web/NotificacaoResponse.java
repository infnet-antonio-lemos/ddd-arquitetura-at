package br.com.freela.notificacao.infrastructure.web;

import br.com.freela.notificacao.domain.Notificacao;
import br.com.freela.notificacao.domain.Papel;

import java.time.Instant;
import java.util.UUID;

public record NotificacaoResponse(UUID id, UUID eventId, String tipoEvento, UUID contratoId, UUID destinatarioId,
                                  Papel papel, String mensagem, Instant criadaEm) {
    static NotificacaoResponse from(Notificacao n) {
        return new NotificacaoResponse(n.getId(), n.getEventId(), n.getTipoEvento(), n.getContratoId(),
                n.getDestinatarioId(), n.getPapel(), n.getMensagem(), n.getCriadaEm());
    }
}
