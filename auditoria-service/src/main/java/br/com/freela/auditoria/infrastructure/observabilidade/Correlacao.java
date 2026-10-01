package br.com.freela.auditoria.infrastructure.observabilidade;

import java.util.UUID;
import java.util.regex.Pattern;

public final class Correlacao {
    public static final String HEADER = "X-Correlation-Id";
    public static final String MDC_CORRELATION_ID = "correlationId";
    public static final String MDC_CONTRATO_ID = "contratoId";
    public static final String MDC_EVENT_ID = "eventId";
    public static final String MDC_EVENT_TYPE = "eventType";

    private static final Pattern FORMATO_VALIDO = Pattern.compile("[A-Za-z0-9._:-]{1,100}");

    private Correlacao() {}

    /** Valor seguro para logs e headers (sem quebras de linha, caracteres de controle ou tamanho excessivo). */
    public static boolean valido(String valor) {
        return valor != null && FORMATO_VALIDO.matcher(valor).matches();
    }

    public static String normalizar(String recebido) {
        return valido(recebido) ? recebido : UUID.randomUUID().toString();
    }
}
