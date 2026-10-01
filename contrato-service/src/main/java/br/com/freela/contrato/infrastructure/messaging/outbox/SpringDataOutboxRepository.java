package br.com.freela.contrato.infrastructure.messaging.outbox;

import org.springframework.data.domain.Limit;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface SpringDataOutboxRepository extends JpaRepository<OutboxEventoJpaEntity, Long> {
    List<OutboxEventoJpaEntity> findByStatusOrderByIdAsc(StatusOutbox status, Limit limite);
    List<OutboxEventoJpaEntity> findByAggregateIdOrderByIdAsc(UUID aggregateId);
    Optional<OutboxEventoJpaEntity> findByEventId(UUID eventId);
}
