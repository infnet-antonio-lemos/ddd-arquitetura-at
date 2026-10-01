package br.com.freela.reputacao.domain;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
        reputacao.aplicar(evento("ContratoCriado", "800.00"));
        reputacao.aplicar(evento("ContratoCriado", "1200.00"));
        reputacao.aplicar(evento("ContratoConcluido", "800.00"));
        reputacao.aplicar(evento("ContratoCancelado", "1200.00"));

        assertThat(reputacao.getContratosRecebidos()).isEqualTo(2);
        assertThat(reputacao.getContratosConcluidos()).isEqualTo(1);
        assertThat(reputacao.getContratosCancelados()).isEqualTo(1);
        assertThat(reputacao.getValorTotalConcluido()).isEqualByComparingTo("800.00");
    }

    @Test
    void somenteCriacaoConclusaoECancelamentoAfetamAReputacao() {
        assertThat(ReputacaoFreelancer.afetadaPor(evento("ContratoCriado", "1"))).isTrue();
        assertThat(ReputacaoFreelancer.afetadaPor(evento("ContratoConcluido", "1"))).isTrue();
        assertThat(ReputacaoFreelancer.afetadaPor(evento("ContratoCancelado", "1"))).isTrue();
        assertThat(ReputacaoFreelancer.afetadaPor(evento("EntregaRegistrada", "1"))).isFalse();
        assertThat(ReputacaoFreelancer.afetadaPor(evento("ContratoReaberto", "1"))).isFalse();
    }

    @Test
    void eventoSemImpactoNaoPodeSerAplicado() {
        assertThatThrownBy(() -> reputacao.aplicar(evento("EntregaRegistrada", "800.00")))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(reputacao.getContratosRecebidos() + reputacao.getContratosConcluidos() + reputacao.getContratosCancelados()).isZero();
    }

    private EventoDeContrato evento(String tipo, String valor) {
        return new EventoDeContrato(UUID.randomUUID(), tipo, UUID.randomUUID(), freelancer, new BigDecimal(valor));
    }
}
