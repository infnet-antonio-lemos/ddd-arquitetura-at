package br.com.freela.auditoria.domain;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface EventoAuditoriaRepository extends JpaRepository<EventoAuditoria, UUID> {
    List<EventoAuditoria> findAllByOrderByRecebidoEmAsc();
    List<EventoAuditoria> findByContratoIdOrderByParticaoAscKafkaOffsetAsc(UUID contratoId);
    List<EventoAuditoria> findByCorrelationIdOrderByRecebidoEmAsc(String correlationId);
    Optional<EventoAuditoria> findFirstByEventIdOrderByRecebidoEmAsc(UUID eventId);
}
