package br.com.freela.reputacao.domain;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class ReputacaoFreelancerTest {
    private final UUID freelancer = UUID.randomUUID();
    private final ReputacaoFreelancer reputacao = ReputacaoFreelancer.nova(freelancer);

    @Test
    void novaReputacaoComecaZerada() {
        assertThat(reputacao.getContratosRecebidos()).isZero();
        assertThat(reputacao.getContratosConcluidos()).isZero();
        assertThat(reputacao.getContratosCancelados()).isZero();
        assertThat(reputacao.getValorTotalConcluido()).isEqualByComparingTo("0");
    }

    @Test
    void contaContratosRecebidosConcluidosECancelados() {
        assertThat(reputacao.aplicar(evento("ContratoCriado", "800.00"))).isTrue();
        assertThat(reputacao.aplicar(evento("ContratoCriado", "1200.00"))).isTrue();
        assertThat(reputacao.aplicar(evento("ContratoConcluido", "800.00"))).isTrue();
        assertThat(reputacao.aplicar(evento("ContratoCancelado", "1200.00"))).isTrue();

        assertThat(reputacao.getContratosRecebidos()).isEqualTo(2);
        assertThat(reputacao.getContratosConcluidos()).isEqualTo(1);
        assertThat(reputacao.getContratosCancelados()).isEqualTo(1);
        assertThat(reputacao.getValorTotalConcluido()).isEqualByComparingTo("800.00");
    }

    @Test
    void entregaRegistradaEEventosDesconhecidosNaoAlteramAReputacao() {
        var atualizadoEm = reputacao.getAtualizadoEm();

        assertThat(reputacao.aplicar(evento("EntregaRegistrada", "800.00"))).isFalse();
        assertThat(reputacao.aplicar(evento("ContratoReaberto", "800.00"))).isFalse();

        assertThat(reputacao.getContratosRecebidos() + reputacao.getContratosConcluidos() + reputacao.getContratosCancelados()).isZero();
        assertThat(reputacao.getAtualizadoEm()).isEqualTo(atualizadoEm);
    }

    private EventoDeContrato evento(String tipo, String valor) {
        return new EventoDeContrato(UUID.randomUUID(), tipo, UUID.randomUUID(), freelancer, new BigDecimal(valor));
    }
}
