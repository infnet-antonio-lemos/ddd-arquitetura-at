package br.com.freela.contrato.domain.shared;

/**
 * Violação de uma regra do domínio (ex.: transição de status inválida).
 * Tipo próprio para que a camada web não confunda regra de negócio com erros
 * genéricos (IllegalArgumentException) lançados pelo framework.
 */
public class RegraDeNegocioException extends RuntimeException {
    public RegraDeNegocioException(String mensagem) { super(mensagem); }
}
