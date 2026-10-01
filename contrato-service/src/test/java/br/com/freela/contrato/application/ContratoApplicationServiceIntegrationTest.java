package br.com.freela.contrato.application;

import br.com.freela.contrato.domain.shared.RegraDeNegocioException;
import br.com.freela.contrato.support.IntegrationTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.simple.JdbcClient;

import java.math.BigDecimal;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@IntegrationTest
class ContratoApplicationServiceIntegrationTest {

    @Autowired
    ContratoApplicationService service;

    @Autowired
    JdbcClient jdbc;

    @Test
    void valorDevolvidoDeveSerOMesmoGravadoNoBanco() {
        var criado = service.criar(new CriarContratoCommand(UUID.randomUUID(), UUID.randomUUID(), "Teste de escala", new BigDecimal("10.999")));

        var gravado = jdbc.sql("select valor from contratos where id = ?").param(criado.id().valor()).query(BigDecimal.class).single();

        assertThat(criado.valor().valor()).isEqualTo(new BigDecimal("11.00")).isEqualTo(gravado);
        assertThat(service.buscar(criado.id().valor()).valor()).isEqualTo(criado.valor());
    }

    @Test
    void tituloAcimaDoLimiteDeveSerRegraDeNegocioENaoErroDeBanco() {
        var comando = new CriarContratoCommand(UUID.randomUUID(), UUID.randomUUID(), "a".repeat(161), BigDecimal.TEN);

        assertThatThrownBy(() -> service.criar(comando)).isInstanceOf(RegraDeNegocioException.class);
    }
}
