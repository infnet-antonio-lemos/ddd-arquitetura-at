package br.com.freela.contrato.domain.model;

import br.com.freela.contrato.domain.event.ContratoCancelado;
import br.com.freela.contrato.domain.event.ContratoConcluido;
import br.com.freela.contrato.domain.event.ContratoCriado;
import br.com.freela.contrato.domain.event.EntregaRegistrada;
import br.com.freela.contrato.domain.shared.DomainEvent;
import br.com.freela.contrato.domain.shared.RegraDeNegocioException;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ContratoTest {
    private final ClienteId clienteId = new ClienteId(UUID.randomUUID());
    private final FreelancerId freelancerId = new FreelancerId(UUID.randomUUID());

    private Contrato novoContrato() {
        return Contrato.criar(clienteId, freelancerId, new Titulo("API de pagamentos"), Dinheiro.de("3500.00"));
    }

    @Test
    void devePercorrerCicloDeVidaEmitindoUmEventoPorTransicao() {
        var contrato = novoContrato();
        assertThat(contrato.pullDomainEvents()).singleElement().isInstanceOf(ContratoCriado.class);

        contrato.registrarEntrega();
        assertThat(contrato.status()).isEqualTo(StatusContrato.ENTREGA_REGISTRADA);
        assertThat(contrato.pullDomainEvents()).singleElement().isInstanceOf(EntregaRegistrada.class);

        contrato.concluir();
        assertThat(contrato.status()).isEqualTo(StatusContrato.CONCLUIDO);
        var eventos = contrato.pullDomainEvents();
        assertThat(eventos).singleElement().isInstanceOf(ContratoConcluido.class);

        var concluido = (ContratoConcluido) eventos.getFirst();
        assertThat(concluido.contratoId()).isEqualTo(contrato.id());
        assertThat(concluido.freelancerId()).isEqualTo(freelancerId);
        assertThat(concluido.valor()).isEqualTo(Dinheiro.de("3500"));
        assertThat(concluido.status()).isEqualTo(StatusContrato.CONCLUIDO);
    }

    @Test
    void eventosDevemTerIdentificadoresUnicos() {
        var contrato = novoContrato();
        contrato.registrarEntrega();
        contrato.concluir();
        assertThat(contrato.pullDomainEvents()).extracting(DomainEvent::eventId).doesNotHaveDuplicates().hasSize(3);
    }

    @Test
    void naoDeveConcluirSemEntregaRegistrada() {
        var contrato = novoContrato();
        assertThatThrownBy(contrato::concluir).isInstanceOf(RegraDeNegocioException.class);
        assertThat(contrato.status()).isEqualTo(StatusContrato.ATIVO);
    }

    @Test
    void naoDeveRegistrarEntregaDeContratoCancelado() {
        var contrato = novoContrato();
        contrato.cancelar();
        assertThatThrownBy(contrato::registrarEntrega).isInstanceOf(RegraDeNegocioException.class);
    }

    @Test
    void cancelamentoDeveSerIdempotente() {
        var contrato = novoContrato();
        contrato.pullDomainEvents();

        contrato.cancelar();
        contrato.cancelar();

        var eventos = contrato.pullDomainEvents();
        assertThat(eventos).singleElement().isInstanceOf(ContratoCancelado.class);
        assertThat(((ContratoCancelado) eventos.getFirst()).statusAnterior()).isEqualTo(StatusContrato.ATIVO);
        assertThat(contrato.status()).isEqualTo(StatusContrato.CANCELADO);
    }

    @Test
    void naoDeveCancelarContratoConcluido() {
        var contrato = novoContrato();
        contrato.registrarEntrega();
        contrato.concluir();
        assertThatThrownBy(contrato::cancelar).isInstanceOf(RegraDeNegocioException.class);
    }

    @Test
    void naoDeveCriarContratoComClienteIgualAoFreelancer() {
        var mesmaPessoa = new FreelancerId(clienteId.valor());
        assertThatThrownBy(() -> Contrato.criar(clienteId, mesmaPessoa, new Titulo("Auto contratação"), Dinheiro.de("10")))
                .isInstanceOf(RegraDeNegocioException.class);
    }
}
