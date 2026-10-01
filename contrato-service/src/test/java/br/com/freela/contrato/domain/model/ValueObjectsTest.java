package br.com.freela.contrato.domain.model;

import br.com.freela.contrato.domain.event.ContratoCriado;
import br.com.freela.contrato.domain.shared.RegraDeNegocioException;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ValueObjectsTest {

    @Test
    void dinheiroDeveNormalizarParaDuasCasasDecimais() {
        assertThat(Dinheiro.de("10.999").valor()).isEqualTo(new BigDecimal("11.00"));
        assertThat(Dinheiro.de("3500").valor()).isEqualTo(new BigDecimal("3500.00"));
    }

    @Test
    void dinheiroComMesmoValorDeveSerIgualIndependenteDaEscala() {
        assertThat(Dinheiro.de("3500")).isEqualTo(Dinheiro.de("3500.00"));
    }

    @Test
    void dinheiroDeveSerPositivo() {
        assertThatThrownBy(() -> Dinheiro.de("0")).isInstanceOf(RegraDeNegocioException.class);
        assertThatThrownBy(() -> Dinheiro.de("-1")).isInstanceOf(RegraDeNegocioException.class);
        assertThatThrownBy(() -> Dinheiro.de("0.004")).isInstanceOf(RegraDeNegocioException.class);
        assertThatThrownBy(() -> new Dinheiro(null)).isInstanceOf(RegraDeNegocioException.class);
    }

    @Test
    void tituloDeveSerAparadoEObrigatorio() {
        assertThat(new Titulo("  Landing page  ").valor()).isEqualTo("Landing page");
        assertThatThrownBy(() -> new Titulo("   ")).isInstanceOf(RegraDeNegocioException.class);
        assertThatThrownBy(() -> new Titulo(null)).isInstanceOf(RegraDeNegocioException.class);
    }

    @Test
    void tituloDeveRespeitarTamanhoMaximo() {
        assertThat(new Titulo("a".repeat(Titulo.TAMANHO_MAXIMO)).valor()).hasSize(160);
        assertThatThrownBy(() -> new Titulo("a".repeat(Titulo.TAMANHO_MAXIMO + 1))).isInstanceOf(RegraDeNegocioException.class);
    }

    @Test
    void identificadoresSaoObrigatoriosEImprimemSoOUuid() {
        var uuid = UUID.randomUUID();
        assertThat(new ContratoId(uuid)).hasToString(uuid.toString());
        assertThatThrownBy(() -> new ClienteId(null)).isInstanceOf(RegraDeNegocioException.class);
        assertThatThrownBy(() -> new FreelancerId(null)).isInstanceOf(RegraDeNegocioException.class);
    }

    @Test
    void eventoDeveCarregarOValorNormalizado() {
        var contrato = Contrato.criar(new ClienteId(UUID.randomUUID()), new FreelancerId(UUID.randomUUID()),
                new Titulo("Teste de escala"), Dinheiro.de("10.999"));

        var criado = (ContratoCriado) contrato.pullDomainEvents().getFirst();

        assertThat(criado.valor().valor()).isEqualTo(new BigDecimal("11.00"));
    }
}
