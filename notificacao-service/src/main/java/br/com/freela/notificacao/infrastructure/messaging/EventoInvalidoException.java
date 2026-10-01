package br.com.freela.notificacao.infrastructure.messaging;

/** Mensagem que nunca poderá ser processada (JSON inválido ou campo obrigatório ausente). */
public class EventoInvalidoException extends RuntimeException {
    public EventoInvalidoException(String mensagem) {
        super(mensagem);
    }

    public EventoInvalidoException(String mensagem, Throwable causa) {
        super(mensagem, causa);
    }
}
