package br.com.freela.reputacao.infrastructure.messaging;

import br.com.freela.reputacao.application.ReputacaoService;
import br.com.freela.reputacao.support.IntegrationTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Duration;
import java.util.UUID;

import static br.com.freela.reputacao.infrastructure.messaging.MensagemComFalha.Status.ESTACIONADA;
import static br.com.freela.reputacao.infrastructure.messaging.MensagemComFalha.Status.FALHA;
import static br.com.freela.reputacao.support.LeitorDeTopico.registrosComChave;
import static br.com.freela.reputacao.support.MensagensDeContrato.evento;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static org.awaitility.Awaitility.await;

@IntegrationTest
class FalhasIntegrationTest {
    private static final Duration ESPERA = Duration.ofSeconds(30);
    private static final String DLT = "freela.contrato.eventos.v1.reputacao-service.dlt";

    @Autowired KafkaTemplate<String, String> kafka;
    @Autowired ConsumerFactory<String, String> consumidores;
    @Autowired ReputacaoService service;
    @Autowired MensagemComFalhaRepository falhas;
    @Autowired ProcessadorDeEventos processador;
    @Autowired SimuladorDeFalhas simulador;

    @Test
    void falhaEstacionaOsEventosSeguintesDoContratoEOReprocessamentoAplicaNaOrdem() {
        var cliente = UUID.randomUUID();
        var freelancerComFalha = UUID.randomUUID();
        var outroFreelancer = UUID.randomUUID();
        var contratoComFalha = UUID.randomUUID();
        simulador.ativar(contratoComFalha);

        kafka.send(evento("ContratoCriado", contratoComFalha, cliente, freelancerComFalha));
        kafka.send(evento("ContratoConcluido", contratoComFalha, cliente, freelancerComFalha));
        kafka.send(evento("ContratoCriado", UUID.randomUUID(), cliente, outroFreelancer));

        await().atMost(ESPERA).untilAsserted(() -> assertThat(falhas.pendentesDoContrato(contratoComFalha))
                .extracting(MensagemComFalha::getEventType, MensagemComFalha::getStatus)
                .containsExactly(tuple("ContratoCriado", FALHA), tuple("ContratoConcluido", ESTACIONADA)));
        await().atMost(ESPERA).until(() -> service.buscar(outroFreelancer).isPresent());
        assertThat(service.buscar(freelancerComFalha)).isEmpty();
        assertThat(registrosComChave(consumidores, DLT, contratoComFalha)).hasSize(1);

        simulador.desativar(contratoComFalha);

        assertThat(processador.reprocessar(contratoComFalha).reprocessadas()).isEqualTo(2);
        assertThat(service.buscar(freelancerComFalha)).hasValueSatisfying(r -> {
            assertThat(r.getContratosRecebidos()).isEqualTo(1);
            assertThat(r.getContratosConcluidos()).isEqualTo(1);
            assertThat(r.getValorTotalConcluido()).isEqualByComparingTo("800.00");
        });
        assertThat(processador.reprocessar(contratoComFalha).reprocessadas()).isZero();
    }
}
