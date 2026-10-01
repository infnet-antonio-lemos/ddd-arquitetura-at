package br.com.freela.contrato.domain.model;

import br.com.freela.contrato.domain.shared.RegraDeNegocioException;
import java.math.BigDecimal;
import java.math.RoundingMode;

/** Valor monetário positivo, sempre com 2 casas decimais (mesma escala da coluna numeric(19,2)). */
public record Dinheiro(BigDecimal valor) {
    public Dinheiro {
        if (valor == null) throw new RegraDeNegocioException("O valor é obrigatório");
        valor = valor.setScale(2, RoundingMode.HALF_UP);
        if (valor.signum() <= 0) throw new RegraDeNegocioException("Valor deve ser positivo");
    }
    public static Dinheiro de(String valor) { return new Dinheiro(new BigDecimal(valor)); }
}
