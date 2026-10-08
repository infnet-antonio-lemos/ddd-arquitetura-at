package br.com.freela.contrato.infrastructure.messaging.outbox;

import br.com.freela.contrato.application.port.PublicadorDeEventos;
import br.com.freela.contrato.domain.event.ContratoEvento;
import br.com.freela.contrato.domain.shared.DomainEvent;
import br.com.freela.contrato.infrastructure.messaging.ContratoEventoMapper;
import br.com.freela.contrato.infrastructure.observabilidade.Correlacao;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Component
public class OutboxPublicadorDeEventos implements PublicadorDeEventos {
    private static final Logger log = LoggerFactory.getLogger(OutboxPublicadorDeEventos.class);

    private final SpringDataOutboxRepository outbox;
    private final ContratoEventoMapper mapper;
    private final Tracer tracer;
    private final Propagator propagator;
    private final String topico;

    public OutboxPublicadorDeEventos(SpringDataOutboxRepository outbox, ContratoEventoMapper mapper, Tracer tracer,
                                     Propagator propagator, @Value("${freela.kafka.topico-contratos}") String topico) {
        this.outbox = outbox;
        this.mapper = mapper;
        this.tracer = tracer;
        this.propagator = propagator;
        this.topico = topico;
    }

    @Override
    @Transactional(propagation = Propagation.MANDATORY)
    public void publicar(List<DomainEvent> eventos) {
        var correlationId = correlationIdAtual();
        var traceparent = traceparentAtual();
        for (DomainEvent evento : eventos) {
            if (!(evento instanceof ContratoEvento contratoEvento)) {
                throw new IllegalArgumentException("Evento sem mapeamento para mensagem: " + evento.eventType());
            }
            var mensagem = mapper.paraMensagem(contratoEvento, correlationId);
            var chave = mensagem.contratoId().toString();
            outbox.save(new OutboxEventoJpaEntity(mensagem.eventId(), mensagem.contratoId(), mensagem.eventType(),
                    topico, chave, mapper.paraJson(mensagem), correlationId, traceparent));
            log.info("outbox.evento.registrado contratoId={} eventId={} eventType={} topico={}",
                    mensagem.contratoId(), mensagem.eventId(), mensagem.eventType(), topico);
        }
    }

    private static String correlationIdAtual() {
        var atual = Correlacao.atual();
        return atual != null ? atual : UUID.randomUUID().toString();
    }

    private String traceparentAtual() {
        var contexto = tracer.currentTraceContext().context();
        if (contexto == null) return null;
        Map<String, String> headers = new HashMap<>();
        propagator.inject(contexto, headers, Map::put);
        return headers.get("traceparent");
    }
}
