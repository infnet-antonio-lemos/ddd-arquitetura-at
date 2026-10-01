package br.com.freela.auditoria.domain;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface EventoAuditoriaRepository extends JpaRepository<EventoAuditoria, UUID> {
    List<EventoAuditoria> findAllByOrderBySequenciaAsc();
    List<EventoAuditoria> findByContratoIdOrderBySequenciaAsc(UUID contratoId);
    List<EventoAuditoria> findByCorrelationIdOrderBySequenciaAsc(String correlationId);
    Optional<EventoAuditoria> findByEventId(UUID eventId);
}
