package br.com.freela.notificacao.application;

import br.com.freela.notificacao.domain.EventoDeContrato;
import br.com.freela.notificacao.domain.Notificacao;
import br.com.freela.notificacao.domain.NotificacaoRepository;
import br.com.freela.notificacao.domain.RegrasDeNotificacao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

@Service
public class NotificacaoService {
    private static final Logger log = LoggerFactory.getLogger(NotificacaoService.class);
    private final NotificacaoRepository repository;

    public NotificacaoService(NotificacaoRepository repository) {
        this.repository = repository;
    }

    @Transactional
    public List<Notificacao> notificar(EventoDeContrato evento) {
        var notificacoes = RegrasDeNotificacao.para(evento);
        if (notificacoes.isEmpty()) {
            log.info("notificacao.evento.ignorado eventType={} motivo=evento-sem-destinatarios", evento.eventType());
            return notificacoes;
        }
        repository.saveAll(notificacoes);
        notificacoes.forEach(n -> log.info("notificacao.registrada notificacaoId={} destinatarioId={} papel={} eventType={}",
                n.getId(), n.getDestinatarioId(), n.getPapel(), n.getTipoEvento()));
        return notificacoes;
    }

    @Transactional(readOnly = true)
    public List<Notificacao> listar() {
        return repository.findAllByOrderByCriadaEmAsc();
    }

    @Transactional(readOnly = true)
    public List<Notificacao> porContrato(UUID contratoId) {
        return repository.findByContratoIdOrderByCriadaEmAsc(contratoId);
    }

    @Transactional(readOnly = true)
    public List<Notificacao> porDestinatario(UUID destinatarioId) {
        return repository.findByDestinatarioIdOrderByCriadaEmAsc(destinatarioId);
    }
}
