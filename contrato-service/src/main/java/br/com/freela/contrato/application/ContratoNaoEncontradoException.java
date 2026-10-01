package br.com.freela.contrato.application;

import java.util.UUID;

public class ContratoNaoEncontradoException extends RuntimeException {
    private final UUID contratoId;

    public ContratoNaoEncontradoException(UUID contratoId) {
        super("Contrato não encontrado: " + contratoId);
        this.contratoId = contratoId;
    }

    public UUID getContratoId() { return contratoId; }
}
