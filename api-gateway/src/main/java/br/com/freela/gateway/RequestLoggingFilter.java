package br.com.freela.gateway;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

import java.util.UUID;
import java.util.regex.Pattern;

@Component
@Order(Ordered.HIGHEST_PRECEDENCE + 10)
public class RequestLoggingFilter implements WebFilter {
    static final String HEADER = "X-Correlation-Id";
    private static final String MDC_CORRELATION_ID = "correlationId";
    private static final Pattern FORMATO_VALIDO = Pattern.compile("[A-Za-z0-9._:-]{1,100}");
    private static final Logger log = LoggerFactory.getLogger(RequestLoggingFilter.class);

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        if (exchange.getRequest().getURI().getPath().startsWith("/actuator")) return chain.filter(exchange);
        var correlationId = normalizar(exchange.getRequest().getHeaders().getFirst(HEADER));
        var request = exchange.getRequest().mutate().header(HEADER, correlationId).build();
        var response = exchange.getResponse();
        response.beforeCommit(() -> {
            response.getHeaders().set(HEADER, correlationId);
            return Mono.empty();
        });
        long inicio = System.nanoTime();
        logar(correlationId, () -> log.info("gateway.requisicao.inicio metodo={} caminho={}",
                request.getMethod(), request.getURI().getPath()));

        return chain.filter(exchange.mutate().request(request).build())
                .doFinally(sinal -> logar(correlationId, () -> log.info(
                        "gateway.requisicao.fim metodo={} caminho={} status={} durationMs={} sinal={}",
                        request.getMethod(), request.getURI().getPath(), response.getStatusCode(),
                        (System.nanoTime() - inicio) / 1_000_000, sinal)));
    }

    static String normalizar(String recebido) {
        return recebido != null && FORMATO_VALIDO.matcher(recebido).matches() ? recebido : UUID.randomUUID().toString();
    }

    private static void logar(String correlationId, Runnable registro) {
        MDC.put(MDC_CORRELATION_ID, correlationId);
        try {
            registro.run();
        } finally {
            MDC.remove(MDC_CORRELATION_ID);
        }
    }
}
