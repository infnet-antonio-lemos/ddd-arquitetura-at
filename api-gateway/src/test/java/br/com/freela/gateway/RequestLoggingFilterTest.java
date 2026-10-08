package br.com.freela.gateway;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static br.com.freela.gateway.RequestLoggingFilter.HEADER;
import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(OutputCaptureExtension.class)
class RequestLoggingFilterTest {
    private final RequestLoggingFilter filtro = new RequestLoggingFilter();

    @Test
    void repassaEDevolveOCorrelationIdRecebido(CapturedOutput saida) {
        var exchange = MockServerWebExchange.from(MockServerHttpRequest.get("/api/contratos").header(HEADER, "pedido-123"));

        assertThat(encaminhar(exchange)).isEqualTo("pedido-123");
        assertThat(exchange.getResponse().getHeaders().getFirst(HEADER)).isEqualTo("pedido-123");
        assertThat(saida).contains("gateway.requisicao.inicio", "gateway.requisicao.fim");
    }

    @Test
    void geraUmIdQuandoOClienteNaoEnvia() {
        var exchange = MockServerWebExchange.from(MockServerHttpRequest.get("/api/contratos"));

        var encaminhado = encaminhar(exchange);

        assertThat(UUID.fromString(encaminhado)).isNotNull();
        assertThat(exchange.getResponse().getHeaders().getFirst(HEADER)).isEqualTo(encaminhado);
    }

    @Test
    void substituiValorInseguroAntesDeLogarOuEncaminhar(CapturedOutput saida) {
        var exchange = MockServerWebExchange.from(
                MockServerHttpRequest.get("/api/contratos").header(HEADER, "abc INFO linha-forjada <script>"));

        var encaminhado = encaminhar(exchange);

        assertThat(UUID.fromString(encaminhado)).isNotNull();
        assertThat(saida).doesNotContain("linha-forjada");
    }

    @Test
    void naoRegistraChamadasDoActuator(CapturedOutput saida) {
        encaminhar(MockServerWebExchange.from(MockServerHttpRequest.get("/actuator/health")));

        assertThat(saida).doesNotContain("gateway.requisicao");
    }

    private String encaminhar(MockServerWebExchange exchange) {
        var encaminhado = new AtomicReference<String>();
        filtro.filter(exchange, e -> {
            encaminhado.set(e.getRequest().getHeaders().getFirst(HEADER));
            return e.getResponse().setComplete();
        }).block();
        return encaminhado.get();
    }
}
