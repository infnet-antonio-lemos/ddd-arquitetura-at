package br.com.freela.contrato.domain.model;

import br.com.freela.contrato.domain.shared.RegraDeNegocioException;

public record Titulo(String valor) {
    public static final int TAMANHO_MAXIMO = 160;

    public Titulo {
        if (valor == null || valor.isBlank()) throw new RegraDeNegocioException("Título é obrigatório");
        valor = valor.trim();
        if (valor.length() > TAMANHO_MAXIMO)
            throw new RegraDeNegocioException("Título deve ter no máximo " + TAMANHO_MAXIMO + " caracteres");
    }
}
