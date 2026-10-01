package br.com.freela.contrato.infrastructure.web;

import br.com.freela.contrato.infrastructure.observabilidade.Correlacao;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.MDC;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;

@Component
@Order(Ordered.HIGHEST_PRECEDENCE + 10)
public class CorrelationIdFilter extends OncePerRequestFilter {

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        String correlationId = Correlacao.normalizar(request.getHeader(Correlacao.HEADER));
        MDC.put(Correlacao.MDC_CORRELATION_ID, correlationId);
        response.setHeader(Correlacao.HEADER, correlationId);
        try {
            chain.doFilter(request, response);
        } finally {
            MDC.remove(Correlacao.MDC_CORRELATION_ID);
        }
    }
}
