package br.com.freela.contrato.infrastructure.web;

import br.com.freela.contrato.support.ContainersConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("test")
@Import(ContainersConfig.class)
@Testcontainers(disabledWithoutDocker = true)
@ExtendWith(OutputCaptureExtension.class)
class LogsIntegrationTest {
    private static final Pattern ID = Pattern.compile("\"id\":\"([0-9a-f-]{36})\"");
    private static final String CLIENTE = "11111111-1111-1111-1111-111111111111";
    private static final String FREELANCER = "22222222-2222-2222-2222-222222222222";

    @Value("${local.server.port}") int porta;
    private final HttpClient http = HttpClient.newHttpClient();

    @Test
    void logsTemOContextoDaOperacaoENaoTemDadosSensiveis(CapturedOutput saida) throws Exception {
        var criado = post("/api/contratos", """
                {"clienteId":"%s","freelancerId":"%s","titulo":"Projeto Confidencial XPTO","valor":98765.43}"""
                .formatted(CLIENTE, FREELANCER), "teste-logs-001");
        var matcher = ID.matcher(criado.body());
        assertThat(matcher.find()).isTrue();
        var contratoId = matcher.group(1);
        var entrega = post("/api/contratos/" + contratoId + "/entrega", "", "teste-logs-001");
        var tituloLongo = post("/api/contratos", """
                {"clienteId":"%s","freelancerId":"%s","titulo":"%s","valor":10}"""
                .formatted(CLIENTE, FREELANCER, "SEGREDO".repeat(30)), "teste-logs-002");
        var jsonQuebrado = post("/api/contratos", "{\"titulo\": SEGREDO-FORA-DE-ASPAS", "teste-logs-003");

        assertThat(criado.statusCode()).isEqualTo(201);
        assertThat(criado.headers().firstValue("X-Correlation-Id")).hasValue("teste-logs-001");
        assertThat(entrega.statusCode()).isEqualTo(200);
        assertThat(tituloLongo.statusCode()).isEqualTo(400);
        assertThat(jsonQuebrado.statusCode()).isEqualTo(400);
        assertThat(saida)
                .containsPattern("correlationId=teste-logs-001 .*contrato\\.criacao\\.sucesso contratoId=" + contratoId)
                .containsPattern("correlationId=teste-logs-001 .*contrato\\.entrega\\.sucesso contratoId=" + contratoId)
                .doesNotContain("Confidencial", "98765", "SEGREDO");
    }

    private HttpResponse<String> post(String caminho, String corpo, String correlationId) throws Exception {
        var request = HttpRequest.newBuilder(URI.create("http://localhost:" + porta + caminho))
                .header("Content-Type", "application/json")
                .header("X-Correlation-Id", correlationId)
                .POST(HttpRequest.BodyPublishers.ofString(corpo))
                .build();
        return http.send(request, HttpResponse.BodyHandlers.ofString());
    }
}
