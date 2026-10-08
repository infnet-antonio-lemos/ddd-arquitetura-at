package br.com.freela.notificacao.infrastructure.messaging;

import br.com.freela.notificacao.application.NotificacaoService;
import br.com.freela.notificacao.domain.Notificacao;
import br.com.freela.notificacao.support.IntegrationTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;

import static br.com.freela.notificacao.infrastructure.messaging.MensagemComFalha.Status.*;
import static br.com.freela.notificacao.support.LeitorDeTopico.registrosComChave;
import static br.com.freela.notificacao.support.MensagensDeContrato.evento;
import static br.com.freela.notificacao.support.MensagensDeContrato.registro;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static org.awaitility.Awaitility.await;

@IntegrationTest
@ExtendWith(OutputCaptureExtension.class)
class FalhasIntegrationTest {
    private static final Duration ESPERA = Duration.ofSeconds(30);
    private static final String DLT = "freela.contrato.eventos.v1.notificacao-service.dlt";

    @Autowired KafkaTemplate<String, String> kafka;
    @Autowired ConsumerFactory<String, String> consumidores;
    @Autowired NotificacaoService service;
    @Autowired MensagemComFalhaRepository falhas;
    @Autowired ProcessadorDeEventos processador;
    @Autowired SimuladorDeFalhas simulador;

    private final UUID cliente = UUID.randomUUID();
    private final UUID freelancer = UUID.randomUUID();

    @Test
    void mensagemInvalidaVaiParaADltSemRetentativasESemBloquearOContrato() {
        var contratoId = UUID.randomUUID();

        kafka.send(registro(UUID.randomUUID(), "ContratoCriado", contratoId, "{nao-e-json"));
        kafka.send(evento("ContratoCriado", contratoId, cliente, freelancer));

        await().atMost(ESPERA).untilAsserted(() -> assertThat(service.porContrato(contratoId)).hasSize(2));
        assertThat(falhas.pendentesDoContrato(contratoId)).isEmpty();
        assertThat(falhas.findAll()).filteredOn(f -> contratoId.equals(f.getContratoId())).singleElement().satisfies(f -> {
            assertThat(f.getStatus()).isEqualTo(INVALIDA);
            assertThat(f.getErro()).contains("JSON inválido");
            assertThat(f.getConteudo()).isEqualTo("{nao-e-json");
        });
        assertThat(registrosComChave(consumidores, DLT, contratoId)).singleElement().satisfies(r -> {
            assertThat(r.value()).isEqualTo("{nao-e-json");
            assertThat(new String(r.headers().lastHeader("kafka_dlt-exception-cause-fqcn").value(), StandardCharsets.UTF_8))
                    .isEqualTo(EventoInvalidoException.class.getName());
        });
    }

    @Test
    void erroDeLeituraNaoExpoeOConteudoDaMensagem(CapturedOutput saida) {
        var contratoId = UUID.randomUUID();

        kafka.send(registro(UUID.randomUUID(), "ContratoCriado", contratoId, "{\"eventId\":\"SEGREDO-NO-CAMPO\"}"));

        await().atMost(ESPERA).untilAsserted(() -> assertThat(falhas.findByStatusOrderByIdAsc(INVALIDA))
                .filteredOn(f -> contratoId.equals(f.getContratoId())).singleElement()
                .satisfies(f -> assertThat(f.getErro()).contains("JSON inválido").doesNotContain("SEGREDO")));
        assertThat(saida).contains("notificacao.evento.falha-definitiva").doesNotContain("SEGREDO");
    }

    @Test
    void falhaEstacionaOsEventosSeguintesDoContratoEOReprocessamentoAplicaNaOrdem() {
        var contratoComFalha = UUID.randomUUID();
        var outroContrato = UUID.randomUUID();
        simulador.ativar(contratoComFalha);

        kafka.send(evento("ContratoCriado", contratoComFalha, cliente, freelancer));
        kafka.send(evento("EntregaRegistrada", contratoComFalha, cliente, freelancer));
        kafka.send(evento("ContratoCriado", outroContrato, cliente, freelancer));

        await().atMost(ESPERA).untilAsserted(() -> assertThat(falhas.pendentesDoContrato(contratoComFalha))
                .extracting(MensagemComFalha::getEventType, MensagemComFalha::getStatus)
                .containsExactly(tuple("ContratoCriado", FALHA),
                        tuple("EntregaRegistrada", ESTACIONADA)));
        await().atMost(ESPERA).untilAsserted(() -> assertThat(service.porContrato(outroContrato)).hasSize(2));
        assertThat(service.porContrato(contratoComFalha)).isEmpty();
        assertThat(registrosComChave(consumidores, DLT, contratoComFalha)).singleElement()
                .satisfies(r -> assertThat(r.value()).contains("\"eventType\":\"ContratoCriado\""));

        simulador.desativar(contratoComFalha);
        var reprocessamento = processador.reprocessar(contratoComFalha);

        assertThat(reprocessamento.reprocessadas()).isEqualTo(2);
        assertThat(reprocessamento.pendentes()).isZero();
        assertThat(service.porContrato(contratoComFalha)).extracting(Notificacao::getTipoEvento)
                .containsExactlyInAnyOrder("ContratoCriado", "ContratoCriado", "EntregaRegistrada");
        assertThat(falhas.pendentesDoContrato(contratoComFalha)).isEmpty();

        kafka.send(evento("ContratoConcluido", contratoComFalha, cliente, freelancer));
        await().atMost(ESPERA).untilAsserted(() -> assertThat(service.porContrato(contratoComFalha)).hasSize(5));
        assertThat(processador.reprocessar(contratoComFalha).reprocessadas()).isZero();
    }
}
