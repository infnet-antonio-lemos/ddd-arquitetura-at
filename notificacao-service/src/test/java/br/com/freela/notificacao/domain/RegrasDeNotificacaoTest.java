package br.com.freela.notificacao.domain;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

class RegrasDeNotificacaoTest {
    private final UUID cliente = UUID.randomUUID();
    private final UUID freelancer = UUID.randomUUID();

    @Test
    void contratoCriadoNotificaClienteEFreelancer() {
        var evento = evento("ContratoCriado");

        assertThat(RegrasDeNotificacao.para(evento))
                .extracting(Notificacao::getPapel, Notificacao::getDestinatarioId, Notificacao::getEventId, Notificacao::getContratoId)
                .containsExactly(
                        tuple(Papel.CLIENTE, cliente, evento.eventId(), evento.contratoId()),
                        tuple(Papel.FREELANCER, freelancer, evento.eventId(), evento.contratoId()));
    }

    @Test
    void entregaRegistradaNotificaSomenteOCliente() {
        assertThat(RegrasDeNotificacao.para(evento("EntregaRegistrada")))
                .extracting(Notificacao::getPapel, Notificacao::getDestinatarioId)
                .containsExactly(tuple(Papel.CLIENTE, cliente));
    }

    @Test
    void conclusaoECancelamentoNotificamAsDuasPartes() {
        assertThat(RegrasDeNotificacao.para(evento("ContratoConcluido"))).extracting(Notificacao::getPapel)
                .containsExactly(Papel.CLIENTE, Papel.FREELANCER);
        assertThat(RegrasDeNotificacao.para(evento("ContratoCancelado"))).extracting(Notificacao::getPapel)
                .containsExactly(Papel.CLIENTE, Papel.FREELANCER);
    }

    @Test
    void mensagemCitaOTituloDoContrato() {
        assertThat(RegrasDeNotificacao.para(evento("ContratoConcluido")))
                .allSatisfy(n -> assertThat(n.getMensagem()).contains("\"Landing page\"").hasSizeLessThanOrEqualTo(500));
    }

    @Test
    void eventoDesconhecidoNaoGeraNotificacao() {
        assertThat(RegrasDeNotificacao.para(evento("ContratoReaberto"))).isEmpty();
    }

    private EventoDeContrato evento(String tipo) {
        return new EventoDeContrato(UUID.randomUUID(), tipo, UUID.randomUUID(), cliente, freelancer, "Landing page");
    }
}
