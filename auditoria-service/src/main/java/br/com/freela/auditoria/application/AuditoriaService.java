package br.com.freela.auditoria.application;

import br.com.freela.auditoria.domain.EventoAuditoria;
import br.com.freela.auditoria.domain.EventoAuditoriaRepository;
import br.com.freela.auditoria.domain.EventoRecebido;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Service
public class AuditoriaService {
    private final EventoAuditoriaRepository repository;

    public AuditoriaService(EventoAuditoriaRepository repository) {
        this.repository = repository;
    }

    @Transactional
    public EventoAuditoria registrar(EventoRecebido evento) {
        return repository.save(EventoAuditoria.registrar(evento));
    }

    @Transactional(readOnly = true)
    public List<EventoAuditoria> listar() {
        return repository.findAllByOrderBySequenciaAsc();
    }

    @Transactional(readOnly = true)
    public List<EventoAuditoria> porContrato(UUID contratoId) {
        return repository.findByContratoIdOrderBySequenciaAsc(contratoId);
    }

    @Transactional(readOnly = true)
    public List<EventoAuditoria> porCorrelacao(String correlationId) {
        return repository.findByCorrelationIdOrderBySequenciaAsc(correlationId);
    }

    @Transactional(readOnly = true)
    public Optional<EventoAuditoria> porEvento(UUID eventId) {
        return repository.findByEventId(eventId);
    }
}
