package br.com.freela.contrato.domain.model;

import br.com.freela.contrato.domain.shared.RegraDeNegocioException;
import java.util.UUID;

public record FreelancerId(UUID valor) {
    public FreelancerId {
        if (valor == null) throw new RegraDeNegocioException("O freelancer é obrigatório");
    }
    @Override public String toString() { return valor.toString(); }
}
