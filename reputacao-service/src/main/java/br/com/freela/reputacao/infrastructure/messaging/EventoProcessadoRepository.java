package br.com.freela.reputacao.infrastructure.messaging;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;

import java.util.UUID;

public interface EventoProcessadoRepository extends JpaRepository<EventoProcessado, UUID> {

    @Modifying
    @Query(value = """
            insert into eventos_processados (event_id, event_type, contrato_id, processado_em)
            values (:eventId, :eventType, :contratoId, now())
            on conflict (event_id) do nothing""", nativeQuery = true)
    int registrar(UUID eventId, String eventType, UUID contratoId);
}
