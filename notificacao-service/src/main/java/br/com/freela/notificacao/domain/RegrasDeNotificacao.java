package br.com.freela.notificacao.domain;

import java.util.List;

/** Quem é notificado em cada evento do contrato, e com qual mensagem. */
public final class RegrasDeNotificacao {

    private RegrasDeNotificacao() {}

    public static List<Notificacao> para(EventoDeContrato evento) {
        return switch (evento.eventType()) {
            case "ContratoCriado" -> List.of(
                    notificacao(evento, Papel.CLIENTE, "Seu contrato \"%s\" foi criado e aguarda a entrega do freelancer."),
                    notificacao(evento, Papel.FREELANCER, "Você foi contratado para \"%s\"."));
            case "EntregaRegistrada" -> List.of(
                    notificacao(evento, Papel.CLIENTE, "A entrega de \"%s\" foi registrada. Revise o trabalho e conclua o contrato."));
            case "ContratoConcluido" -> List.of(
                    notificacao(evento, Papel.CLIENTE, "O contrato \"%s\" foi concluído."),
                    notificacao(evento, Papel.FREELANCER, "O contrato \"%s\" foi concluído pelo cliente."));
            case "ContratoCancelado" -> List.of(
                    notificacao(evento, Papel.CLIENTE, "O contrato \"%s\" foi cancelado."),
                    notificacao(evento, Papel.FREELANCER, "O contrato \"%s\" foi cancelado."));
            default -> List.of();
        };
    }

    private static Notificacao notificacao(EventoDeContrato evento, Papel papel, String modelo) {
        return new Notificacao(evento, papel, modelo.formatted(evento.titulo()));
    }
}
