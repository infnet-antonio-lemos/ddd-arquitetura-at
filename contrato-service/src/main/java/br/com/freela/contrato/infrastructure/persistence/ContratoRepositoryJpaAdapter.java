package br.com.freela.contrato.infrastructure.persistence;

import br.com.freela.contrato.domain.model.ClienteId;
import br.com.freela.contrato.domain.model.Contrato;
import br.com.freela.contrato.domain.model.ContratoId;
import br.com.freela.contrato.domain.model.Dinheiro;
import br.com.freela.contrato.domain.model.FreelancerId;
import br.com.freela.contrato.domain.model.Titulo;
import br.com.freela.contrato.domain.repository.ContratoRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;
import java.util.List;
import java.util.Optional;

@Repository
public class ContratoRepositoryJpaAdapter implements ContratoRepository {
    private static final Logger log = LoggerFactory.getLogger(ContratoRepositoryJpaAdapter.class);
    private final SpringDataContratoRepository jpa;
    public ContratoRepositoryJpaAdapter(SpringDataContratoRepository jpa) { this.jpa = jpa; }

    @Override public Contrato salvar(Contrato c) {
        log.info("contrato.persistence.save.inicio contratoId={} status={} versao={}", c.id(), c.status(), c.versao());
        var entity = new ContratoJpaEntity(c.id().valor(), c.clienteId().valor(), c.freelancerId().valor(), c.titulo().valor(),
                c.valor().valor(), c.status(), c.criadoEm(), c.versao());
        var saved = jpa.saveAndFlush(entity);
        log.info("contrato.persistence.save.sucesso contratoId={} status={} versao={}", saved.getId(), saved.getStatus(), saved.getVersao());
        return toDomain(saved);
    }
    @Override public Optional<Contrato> buscarPorId(ContratoId id) {
        log.info("contrato.persistence.findById contratoId={}", id);
        return jpa.findById(id.valor()).map(this::toDomain);
    }
    @Override public List<Contrato> listar() {
        log.info("contrato.persistence.findAll");
        return jpa.findAll().stream().map(this::toDomain).toList();
    }
    private Contrato toDomain(ContratoJpaEntity e) {
        return Contrato.restaurar(new ContratoId(e.getId()), new ClienteId(e.getClienteId()), new FreelancerId(e.getFreelancerId()),
                new Titulo(e.getTitulo()), new Dinheiro(e.getValor()), e.getStatus(), e.getCriadoEm(), e.getVersao());
    }
}
