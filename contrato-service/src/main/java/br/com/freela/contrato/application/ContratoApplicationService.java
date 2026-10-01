package br.com.freela.contrato.application;

import br.com.freela.contrato.domain.model.ClienteId;
import br.com.freela.contrato.domain.model.Contrato;
import br.com.freela.contrato.domain.model.ContratoId;
import br.com.freela.contrato.domain.model.Dinheiro;
import br.com.freela.contrato.domain.model.FreelancerId;
import br.com.freela.contrato.domain.model.Titulo;
import br.com.freela.contrato.domain.repository.ContratoRepository;
import br.com.freela.contrato.domain.shared.DomainEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;

@Service
public class ContratoApplicationService {
    private static final Logger log = LoggerFactory.getLogger(ContratoApplicationService.class);
    private final ContratoRepository repository;

    public ContratoApplicationService(ContratoRepository repository) { this.repository = repository; }

    @Transactional
    public Contrato criar(CriarContratoCommand cmd) {
        log.info("contrato.criacao.inicio clienteId={} freelancerId={}", cmd.clienteId(), cmd.freelancerId());
        Contrato contrato = Contrato.criar(new ClienteId(cmd.clienteId()), new FreelancerId(cmd.freelancerId()),
                new Titulo(cmd.titulo()), new Dinheiro(cmd.valor()));
        log.info("contrato.dominio.criado contratoId={} status={} domainEvents={}",
                contrato.id(), contrato.status(), contrato.domainEvents().size());
        Contrato salvo = repository.salvar(contrato);
        registrarEventos(contrato);
        log.info("contrato.criacao.sucesso contratoId={} clienteId={} freelancerId={} status={}",
                salvo.id(), salvo.clienteId(), salvo.freelancerId(), salvo.status());
        return salvo;
    }

    @Transactional
    public Contrato registrarEntrega(UUID id) { return executarTransicao(id, "entrega", Contrato::registrarEntrega); }

    @Transactional
    public Contrato concluir(UUID id) { return executarTransicao(id, "conclusao", Contrato::concluir); }

    @Transactional
    public Contrato cancelar(UUID id) { return executarTransicao(id, "cancelamento", Contrato::cancelar); }

    @Transactional(readOnly = true)
    public Contrato buscar(UUID id) {
        log.info("contrato.busca.inicio contratoId={}", id);
        var contrato = carregar(id);
        log.info("contrato.busca.sucesso contratoId={} status={}", id, contrato.status());
        return contrato;
    }

    @Transactional(readOnly = true)
    public List<Contrato> listar() {
        log.info("contrato.listagem.inicio");
        var contratos = repository.listar();
        log.info("contrato.listagem.sucesso quantidade={}", contratos.size());
        return contratos;
    }

    private Contrato executarTransicao(UUID id, String operacao, Consumer<Contrato> transicao) {
        log.info("contrato.{}.inicio contratoId={}", operacao, id);
        var contrato = carregar(id);
        var statusAnterior = contrato.status();
        transicao.accept(contrato);
        var salvo = repository.salvar(contrato);
        // os eventos são puxados da instância alterada (a devolvida por salvar é reconstruída sem eventos)
        registrarEventos(contrato);
        log.info("contrato.{}.sucesso contratoId={} statusAnterior={} status={}", operacao, id, statusAnterior, salvo.status());
        return salvo;
    }

    private Contrato carregar(UUID id) {
        return repository.buscarPorId(new ContratoId(id)).orElseThrow(() -> new ContratoNaoEncontradoException(id));
    }

    private void registrarEventos(Contrato contrato) {
        // PONTO DO ASSESSMENT:
        // Os eventos existem no Aggregate, mas ainda NÃO são publicados no Kafka.
        // A publicação transacional (Outbox) é implementada na próxima etapa.
        for (DomainEvent event : contrato.pullDomainEvents()) {
            log.info("contrato.evento.pendente contratoId={} eventId={} eventType={} occurredAt={}",
                    contrato.id(), event.eventId(), event.eventType(), event.occurredAt());
        }
    }
}
