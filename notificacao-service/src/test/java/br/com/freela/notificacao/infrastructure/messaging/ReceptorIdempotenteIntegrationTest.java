package br.com.freela.notificacao.infrastructure.messaging;

import br.com.freela.notificacao.domain.EventoDeContrato;
import br.com.freela.notificacao.domain.NotificacaoRepository;
import br.com.freela.notificacao.domain.RegrasDeNotificacao;
import br.com.freela.notificacao.support.IntegrationTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@IntegrationTest
class ReceptorIdempotenteIntegrationTest {
    @Autowired ReceptorIdempotente receptor;
    @Autowired EventoProcessadoRepository processados;
    @Autowired NotificacaoRepository notificacoes;

    private final UUID eventId = UUID.randomUUID();
    private final UUID contratoId = UUID.randomUUID();

    @Test
    void falhaNoProcessamentoDesfazORegistroEAReentregaProcessa() {
        assertThatThrownBy(() -> receptor.processarUmaVez(eventId, "ContratoCriado", contratoId, () -> {
            throw new IllegalStateException("falha simulada");
        })).isInstanceOf(IllegalStateException.class);
        assertThat(processados.existsById(eventId)).isFalse();

        assertThat(receptor.processarUmaVez(eventId, "ContratoCriado", contratoId, () -> "processado")).contains("processado");
        assertThat(receptor.processarUmaVez(eventId, "ContratoCriado", contratoId, () -> "de novo")).isEmpty();
    }

    @Test
    void bancoRecusaNotificacaoRepetidaParaOMesmoEventoEDestinatario() {
        var evento = new EventoDeContrato(eventId, "ContratoCriado", contratoId, UUID.randomUUID(), UUID.randomUUID(), "Landing page");
        notificacoes.saveAllAndFlush(RegrasDeNotificacao.para(evento));

        assertThatThrownBy(() -> notificacoes.saveAllAndFlush(RegrasDeNotificacao.para(evento)))
                .isInstanceOf(DataIntegrityViolationException.class)
                .hasMessageContaining("uk_notificacoes_evento_destinatario");
    }
}
