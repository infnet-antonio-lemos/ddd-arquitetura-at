package br.com.freela.contrato.infrastructure.persistence;

import br.com.freela.contrato.domain.model.ClienteId;
import br.com.freela.contrato.domain.model.Contrato;
import br.com.freela.contrato.domain.model.Dinheiro;
import br.com.freela.contrato.domain.model.FreelancerId;
import br.com.freela.contrato.domain.model.StatusContrato;
import br.com.freela.contrato.domain.model.Titulo;
import br.com.freela.contrato.domain.repository.ContratoRepository;
import br.com.freela.contrato.support.IntegrationTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.orm.ObjectOptimisticLockingFailureException;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@IntegrationTest
class ContratoConcorrenciaIntegrationTest {

    @Autowired
    ContratoRepository repository;

    private Contrato novoContrato(String titulo) {
        return Contrato.criar(new ClienteId(UUID.randomUUID()), new FreelancerId(UUID.randomUUID()), new Titulo(titulo), Dinheiro.de("10"));
    }

    @Test
    void deveIncrementarVersaoACadaAlteracao() {
        var salvo = repository.salvar(novoContrato("Landing page"));
        assertThat(salvo.versao()).isZero();

        var carregado = repository.buscarPorId(salvo.id()).orElseThrow();
        carregado.registrarEntrega();
        var atualizado = repository.salvar(carregado);

        assertThat(atualizado.versao()).isEqualTo(1L);
        assertThat(atualizado.status()).isEqualTo(StatusContrato.ENTREGA_REGISTRADA);
    }

    @Test
    void deveRejeitarAlteracaoBaseadaEmVersaoDesatualizada() {
        var salvo = repository.salvar(novoContrato("App mobile"));

        // duas operações leem o mesmo contrato ao mesmo tempo
        var leituraA = repository.buscarPorId(salvo.id()).orElseThrow();
        var leituraB = repository.buscarPorId(salvo.id()).orElseThrow();

        leituraA.registrarEntrega();
        repository.salvar(leituraA);

        // a segunda tenta gravar a partir de uma versão que já não é a atual
        leituraB.cancelar();
        assertThatThrownBy(() -> repository.salvar(leituraB))
                .isInstanceOf(ObjectOptimisticLockingFailureException.class);

        assertThat(repository.buscarPorId(salvo.id()).orElseThrow().status())
                .isEqualTo(StatusContrato.ENTREGA_REGISTRADA);
    }
}
