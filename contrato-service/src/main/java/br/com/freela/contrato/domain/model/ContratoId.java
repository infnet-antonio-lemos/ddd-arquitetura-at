package br.com.freela.contrato.domain.model;

import br.com.freela.contrato.domain.shared.RegraDeNegocioException;
import java.util.UUID;

public record ContratoId(UUID valor) {
    public ContratoId {
        if (valor == null) throw new RegraDeNegocioException("O id do contrato é obrigatório");
    }
    public static ContratoId novo() { return new ContratoId(UUID.randomUUID()); }
    @Override public String toString() { return valor.toString(); }
}
