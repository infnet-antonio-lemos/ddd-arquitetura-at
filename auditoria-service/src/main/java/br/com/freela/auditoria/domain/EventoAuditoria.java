package br.com.freela.auditoria.domain;

import jakarta.persistence.*;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "auditoria_eventos", indexes = {
        @Index(name = "idx_auditoria_event_id", columnList = "eventId"),
        @Index(name = "idx_auditoria_contrato", columnList = "contratoId"),
        @Index(name = "idx_auditoria_correlation", columnList = "correlationId")})
public class EventoAuditoria {
    @Id private UUID id;
    @Column(nullable=false) private UUID eventId;
    @Column(nullable=false, length=80) private String eventType;
    private Integer eventVersion;
    @Column(nullable=false) private Instant occurredAt;
    @Column(nullable=false) private UUID contratoId;
    @Column(length=100) private String correlationId;
    @Column(length=100) private String producer;
    @Column(nullable=false, columnDefinition="text") private String conteudo;
    @Column(nullable=false, length=200) private String topico;
    @Column(nullable=false) private int particao;
    @Column(nullable=false) private long kafkaOffset;
    @Column(nullable=false) private Instant recebidoEm;

    protected EventoAuditoria() {}

    public static EventoAuditoria registrar(EventoRecebido evento) {
        var registro = new EventoAuditoria();
        registro.id = UUID.randomUUID();
        registro.eventId = evento.eventId();
        registro.eventType = evento.eventType();
        registro.eventVersion = evento.eventVersion();
        registro.occurredAt = evento.occurredAt();
        registro.contratoId = evento.contratoId();
        registro.correlationId = evento.correlationId();
        registro.producer = evento.producer();
        registro.conteudo = evento.conteudo();
        registro.topico = evento.origem().topico();
        registro.particao = evento.origem().particao();
        registro.kafkaOffset = evento.origem().offset();
        registro.recebidoEm = Instant.now();
        return registro;
    }

    public UUID getId(){return id;} public UUID getEventId(){return eventId;} public String getEventType(){return eventType;}
    public Integer getEventVersion(){return eventVersion;} public Instant getOccurredAt(){return occurredAt;}
    public UUID getContratoId(){return contratoId;} public String getCorrelationId(){return correlationId;}
    public String getProducer(){return producer;} public String getConteudo(){return conteudo;} public String getTopico(){return topico;}
    public int getParticao(){return particao;} public long getKafkaOffset(){return kafkaOffset;} public Instant getRecebidoEm(){return recebidoEm;}
}
