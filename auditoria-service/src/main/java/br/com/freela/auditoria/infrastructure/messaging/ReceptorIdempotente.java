package br.com.freela.auditoria.infrastructure.messaging;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;

@Component
public class ReceptorIdempotente {
    private final EventoProcessadoRepository processados;

    public ReceptorIdempotente(EventoProcessadoRepository processados) {
        this.processados = processados;
    }

    @Transactional
    public <T> Optional<T> processarUmaVez(UUID eventId, String eventType, UUID contratoId, Supplier<T> processamento) {
        if (processados.registrar(eventId, eventType, contratoId) == 0) {
            return Optional.empty();
        }
        return Optional.of(processamento.get());
    }
}
