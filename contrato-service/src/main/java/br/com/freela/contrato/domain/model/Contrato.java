package br.com.freela.contrato.domain.model;

import br.com.freela.contrato.domain.event.ContratoCancelado;
import br.com.freela.contrato.domain.event.ContratoConcluido;
import br.com.freela.contrato.domain.event.ContratoCriado;
import br.com.freela.contrato.domain.event.EntregaRegistrada;
import br.com.freela.contrato.domain.shared.DomainEvent;
import br.com.freela.contrato.domain.shared.RegraDeNegocioException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Contrato {
    private final ContratoId id;
    private final ClienteId clienteId;
    private final FreelancerId freelancerId;
    private final Titulo titulo;
    private final Dinheiro valor;
    private StatusContrato status;
    private final Instant criadoEm;
    /** Versão de persistência (controle de concorrência otimista). Nula enquanto o contrato não foi salvo. */
    private final Long versao;
    private final List<DomainEvent> domainEvents = new ArrayList<>();

    private Contrato(ContratoId id, ClienteId clienteId, FreelancerId freelancerId, Titulo titulo,
                     Dinheiro valor, StatusContrato status, Instant criadoEm, Long versao) {
        this.id = id; this.clienteId = clienteId; this.freelancerId = freelancerId;
        this.titulo = titulo; this.valor = valor; this.status = status; this.criadoEm = criadoEm; this.versao = versao;
    }

    public static Contrato criar(ClienteId clienteId, FreelancerId freelancerId, Titulo titulo, Dinheiro valor) {
        if (clienteId == null || freelancerId == null) throw new RegraDeNegocioException("Cliente e freelancer são obrigatórios");
        if (clienteId.valor().equals(freelancerId.valor())) throw new RegraDeNegocioException("Cliente e freelancer devem ser pessoas diferentes");
        if (titulo == null) throw new RegraDeNegocioException("Título é obrigatório");
        if (valor == null) throw new RegraDeNegocioException("O valor é obrigatório");
        var contrato = new Contrato(ContratoId.novo(), clienteId, freelancerId, titulo, valor,
                StatusContrato.ATIVO, Instant.now(), null);
        contrato.domainEvents.add(ContratoCriado.novo(contrato));
        return contrato;
    }

    public static Contrato restaurar(ContratoId id, ClienteId clienteId, FreelancerId freelancerId, Titulo titulo,
                                     Dinheiro valor, StatusContrato status, Instant criadoEm, Long versao) {
        return new Contrato(id, clienteId, freelancerId, titulo, valor, status, criadoEm, versao);
    }

    public void registrarEntrega() {
        if (status != StatusContrato.ATIVO) throw new RegraDeNegocioException("Somente contratos ativos recebem entrega");
        status = StatusContrato.ENTREGA_REGISTRADA;
        domainEvents.add(EntregaRegistrada.novo(this));
    }
    public void concluir() {
        if (status != StatusContrato.ENTREGA_REGISTRADA) throw new RegraDeNegocioException("A entrega precisa estar registrada");
        status = StatusContrato.CONCLUIDO;
        domainEvents.add(ContratoConcluido.novo(this));
    }
    /** Idempotente: cancelar um contrato já cancelado não altera nada nem gera novo evento. */
    public void cancelar() {
        if (status == StatusContrato.CANCELADO) return;
        if (status == StatusContrato.CONCLUIDO) throw new RegraDeNegocioException("Contrato concluído não pode ser cancelado");
        var statusAnterior = status;
        status = StatusContrato.CANCELADO;
        domainEvents.add(ContratoCancelado.novo(this, statusAnterior));
    }
    public List<DomainEvent> pullDomainEvents() {
        var copy = List.copyOf(domainEvents); domainEvents.clear(); return copy;
    }
    public List<DomainEvent> domainEvents() { return Collections.unmodifiableList(domainEvents); }
    public ContratoId id(){return id;} public ClienteId clienteId(){return clienteId;} public FreelancerId freelancerId(){return freelancerId;}
    public Titulo titulo(){return titulo;} public Dinheiro valor(){return valor;} public StatusContrato status(){return status;}
    public Instant criadoEm(){return criadoEm;} public Long versao(){return versao;}
}
