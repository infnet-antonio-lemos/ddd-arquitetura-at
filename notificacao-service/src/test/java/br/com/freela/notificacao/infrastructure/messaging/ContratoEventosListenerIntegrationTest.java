package br.com.freela.notificacao.infrastructure.messaging;

import br.com.freela.notificacao.application.NotificacaoService;
import br.com.freela.notificacao.domain.Notificacao;
import br.com.freela.notificacao.domain.Papel;
import br.com.freela.notificacao.support.IntegrationTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Duration;
import java.util.UUID;

import static br.com.freela.notificacao.support.MensagensDeContrato.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static org.awaitility.Awaitility.await;

@IntegrationTest
class ContratoEventosListenerIntegrationTest {
    private static final Duration ESPERA = Duration.ofSeconds(30);

    @Autowired KafkaTemplate<String, String> kafka;
    @Autowired NotificacaoService service;

    private final UUID contratoId = UUID.randomUUID();
    private final UUID cliente = UUID.randomUUID();
    private final UUID freelancer = UUID.randomUUID();

    @Test
    void cicloDeVidaDoContratoGeraUmaNotificacaoPorDestinatario() {
        kafka.send(evento("ContratoCriado", contratoId, cliente, freelancer));
        kafka.send(evento("EntregaRegistrada", contratoId, cliente, freelancer));
        kafka.send(evento("ContratoConcluido", contratoId, cliente, freelancer));

        await().atMost(ESPERA).untilAsserted(() -> assertThat(service.porContrato(contratoId))
                .extracting(Notificacao::getTipoEvento, Notificacao::getPapel, Notificacao::getDestinatarioId)
                .containsExactlyInAnyOrder(
                        tuple("ContratoCriado", Papel.CLIENTE, cliente),
                        tuple("ContratoCriado", Papel.FREELANCER, freelancer),
                        tuple("EntregaRegistrada", Papel.CLIENTE, cliente),
                        tuple("ContratoConcluido", Papel.CLIENTE, cliente),
                        tuple("ContratoConcluido", Papel.FREELANCER, freelancer)));
        assertThat(service.porDestinatario(freelancer)).hasSize(2);
    }

    @Test
    void ignoraCamposDesconhecidosNaMensagem() {
        var eventId = UUID.randomUUID();
        var comCamposNovos = json(eventId, "ContratoCriado", 1, contratoId, cliente, freelancer)
                .replace("\"producer\"", "\"campoNovo\":\"x\",\"producer\"")
                .replace("\"status\"", "\"prazo\":\"2026-12-01\",\"status\"");

        kafka.send(registro(eventId, "ContratoCriado", contratoId, comCamposNovos));

        await().atMost(ESPERA).untilAsserted(() -> assertThat(service.porContrato(contratoId)).hasSize(2)
                .allSatisfy(n -> assertThat(n.getEventId()).isEqualTo(eventId)));
    }

    @Test
    void tipoDeEventoDesconhecidoEIgnoradoSemBloquearOsSeguintes() {
        kafka.send(evento("ContratoReaberto", contratoId, cliente, freelancer));
        kafka.send(evento("ContratoCriado", contratoId, cliente, freelancer));

        await().atMost(ESPERA).untilAsserted(() -> assertThat(service.porContrato(contratoId))
                .extracting(Notificacao::getTipoEvento).containsOnly("ContratoCriado").hasSize(2));
    }

    @Test
    void mensagemInvalidaOuDeVersaoNaoSuportadaNaoBloqueiaAsSeguintes() {
        kafka.send(registro(UUID.randomUUID(), "ContratoCriado", contratoId, "{nao-e-json"));
        var v2 = UUID.randomUUID();
        kafka.send(registro(v2, "ContratoCriado", contratoId, json(v2, "ContratoCriado", 2, contratoId, cliente, freelancer)));
        kafka.send(evento("EntregaRegistrada", contratoId, cliente, freelancer));

        await().atMost(ESPERA).untilAsserted(() -> assertThat(service.porContrato(contratoId))
                .extracting(Notificacao::getTipoEvento).containsExactly("EntregaRegistrada"));
    }
}
