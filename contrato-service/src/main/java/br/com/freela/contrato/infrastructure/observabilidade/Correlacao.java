package br.com.freela.contrato.infrastructure.observabilidade;

import org.slf4j.MDC;

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

    /** Valor recebido do cliente, se seguro para logs e headers; senão um novo UUID. */
    public static String normalizar(String recebido) {
        return recebido != null && FORMATO_VALIDO.matcher(recebido).matches() ? recebido : UUID.randomUUID().toString();
    }

    public static String atual() {
        return MDC.get(MDC_CORRELATION_ID);
    }
}
