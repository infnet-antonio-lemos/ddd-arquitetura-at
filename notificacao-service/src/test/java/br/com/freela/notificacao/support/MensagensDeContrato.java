package br.com.freela.notificacao.support;

import org.apache.kafka.clients.producer.ProducerRecord;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;

public final class MensagensDeContrato {
    public static final String TOPICO = "freela.contrato.eventos.v1";
    public static final String CORRELATION_ID = "teste-consumidor";

    private MensagensDeContrato() {}

    public static ProducerRecord<String, String> evento(String eventType, UUID contratoId, UUID clienteId, UUID freelancerId) {
        var eventId = UUID.randomUUID();
        return registro(eventId, eventType, contratoId, json(eventId, eventType, 1, contratoId, clienteId, freelancerId));
    }

    public static String json(UUID eventId, String eventType, int eventVersion, UUID contratoId, UUID clienteId, UUID freelancerId) {
        return """
                {"eventId":"%s","eventType":"%s","eventVersion":%d,"occurredAt":"%s","contratoId":"%s",\
                "correlationId":"%s","producer":"contrato-service","payload":{"contratoId":"%s","clienteId":"%s",\
                "freelancerId":"%s","titulo":"Landing page","valor":800.00,"status":"ATIVO"}}"""
                .formatted(eventId, eventType, eventVersion, Instant.now(), contratoId, CORRELATION_ID, contratoId,
                        clienteId, freelancerId);
    }

    public static ProducerRecord<String, String> registro(UUID eventId, String eventType, UUID contratoId, String valor) {
        var registro = new ProducerRecord<>(TOPICO, contratoId.toString(), valor);
        registro.headers()
                .add("eventId", bytes(eventId.toString()))
                .add("eventType", bytes(eventType))
                .add("contratoId", bytes(contratoId.toString()))
                .add("correlationId", bytes(CORRELATION_ID));
        return registro;
    }

    private static byte[] bytes(String valor) {
        return valor.getBytes(StandardCharsets.UTF_8);
    }
}
