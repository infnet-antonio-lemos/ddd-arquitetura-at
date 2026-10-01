package br.com.freela.contrato.infrastructure.messaging.outbox;

import jakarta.persistence.*;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "outbox_eventos", indexes = @Index(name = "idx_outbox_status_id", columnList = "status, id"))
public class OutboxEventoJpaEntity {
    private static final int TAMANHO_MAXIMO_ERRO = 1000;

    @Id @GeneratedValue(strategy = GenerationType.IDENTITY) private Long id;
    @Column(nullable=false, unique=true) private UUID eventId;
    @Column(nullable=false) private UUID aggregateId;
    @Column(nullable=false, length=80) private String eventType;
    @Column(nullable=false, length=200) private String topico;
    @Column(nullable=false, length=100) private String chave;
    @Column(nullable=false, columnDefinition="text") private String payload;
    @Column(nullable=false, length=100) private String correlationId;
    @Enumerated(EnumType.STRING) @Column(nullable=false, length=20) private StatusOutbox status;
    @Column(nullable=false) private int tentativas;
    @Column(length=TAMANHO_MAXIMO_ERRO) private String ultimoErro;
    @Column(nullable=false) private Instant criadoEm;
    private Instant publicadoEm;

    protected OutboxEventoJpaEntity() {}

    public OutboxEventoJpaEntity(UUID eventId, UUID aggregateId, String eventType, String topico, String chave,
                                 String payload, String correlationId) {
        this.eventId = eventId; this.aggregateId = aggregateId; this.eventType = eventType; this.topico = topico;
        this.chave = chave; this.payload = payload; this.correlationId = correlationId;
        this.status = StatusOutbox.PENDENTE; this.criadoEm = Instant.now();
    }

    public void marcarPublicado() {
        status = StatusOutbox.PUBLICADO;
        publicadoEm = Instant.now();
        tentativas++;
        ultimoErro = null;
    }

    public void registrarFalha(String erro) {
        tentativas++;
        ultimoErro = erro == null ? null : erro.substring(0, Math.min(erro.length(), TAMANHO_MAXIMO_ERRO));
    }

    public Long getId(){return id;} public UUID getEventId(){return eventId;} public UUID getAggregateId(){return aggregateId;}
    public String getEventType(){return eventType;} public String getTopico(){return topico;} public String getChave(){return chave;}
    public String getPayload(){return payload;} public String getCorrelationId(){return correlationId;} public StatusOutbox getStatus(){return status;}
    public int getTentativas(){return tentativas;} public String getUltimoErro(){return ultimoErro;}
    public Instant getCriadoEm(){return criadoEm;} public Instant getPublicadoEm(){return publicadoEm;}
}
