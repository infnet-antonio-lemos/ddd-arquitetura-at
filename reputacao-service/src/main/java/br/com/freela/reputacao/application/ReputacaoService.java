package br.com.freela.reputacao.application;

import br.com.freela.reputacao.domain.EventoDeContrato;
import br.com.freela.reputacao.domain.ReputacaoFreelancer;
import br.com.freela.reputacao.domain.ReputacaoRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Service
public class ReputacaoService {
    private static final Logger log = LoggerFactory.getLogger(ReputacaoService.class);
    private final ReputacaoRepository repository;

    public ReputacaoService(ReputacaoRepository repository) {
        this.repository = repository;
    }

    @Transactional
    public boolean processar(EventoDeContrato evento) {
        if (!ReputacaoFreelancer.afetadaPor(evento)) {
            log.info("reputacao.evento.ignorado eventType={} freelancerId={} motivo=sem-impacto-na-reputacao",
                    evento.eventType(), evento.freelancerId());
            return false;
        }
        repository.garantirExistencia(evento.freelancerId());
        var reputacao = repository.buscarParaAtualizar(evento.freelancerId());
        reputacao.aplicar(evento);
        log.info("reputacao.atualizada eventType={} freelancerId={} contratosRecebidos={} contratosConcluidos={} contratosCancelados={}",
                evento.eventType(), reputacao.getFreelancerId(), reputacao.getContratosRecebidos(),
                reputacao.getContratosConcluidos(), reputacao.getContratosCancelados());
        return true;
    }

    @Transactional(readOnly = true)
    public List<ReputacaoFreelancer> listar() {
        return repository.findAllByOrderByFreelancerIdAsc();
    }

    @Transactional(readOnly = true)
    public Optional<ReputacaoFreelancer> buscar(UUID freelancerId) {
        return repository.findById(freelancerId);
    }
}
