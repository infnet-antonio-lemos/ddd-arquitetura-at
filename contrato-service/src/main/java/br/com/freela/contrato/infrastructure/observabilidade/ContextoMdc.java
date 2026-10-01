package br.com.freela.contrato.infrastructure.observabilidade;

import org.slf4j.MDC;

import java.util.HashMap;
import java.util.Map;

/** Coloca várias chaves no MDC de uma vez e restaura os valores anteriores ao fechar. */
public final class ContextoMdc implements AutoCloseable {
    private final Map<String, String> anteriores = new HashMap<>();

    private ContextoMdc(Map<String, String> valores) {
        valores.forEach((chave, valor) -> {
            anteriores.put(chave, MDC.get(chave));
            MDC.put(chave, valor);
        });
    }

    public static ContextoMdc com(Map<String, String> valores) {
        return new ContextoMdc(valores);
    }

    @Override
    public void close() {
        anteriores.forEach((chave, anterior) -> {
            if (anterior == null) MDC.remove(chave); else MDC.put(chave, anterior);
        });
    }
}
