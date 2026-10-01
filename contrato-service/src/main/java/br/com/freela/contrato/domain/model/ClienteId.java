package br.com.freela.contrato.domain.model;

import br.com.freela.contrato.domain.shared.RegraDeNegocioException;
import java.util.UUID;

public record ClienteId(UUID valor) {
    public ClienteId {
        if (valor == null) throw new RegraDeNegocioException("O cliente é obrigatório");
    }
    @Override public String toString() { return valor.toString(); }
}
