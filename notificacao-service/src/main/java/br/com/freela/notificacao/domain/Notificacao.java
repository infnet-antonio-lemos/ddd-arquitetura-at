package br.com.freela.notificacao.domain;

import jakarta.persistence.*;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "notificacoes", indexes = {
        @Index(name = "idx_notificacoes_contrato", columnList = "contratoId"),
        @Index(name = "idx_notificacoes_destinatario", columnList = "destinatarioId")})
public class Notificacao {
    @Id private UUID id;
    @Column(nullable=false) private UUID eventId;
    @Column(nullable=false, length=80) private String tipoEvento;
    @Column(nullable=false) private UUID contratoId;
    @Column(nullable=false) private UUID destinatarioId;
    @Enumerated(EnumType.STRING) @Column(nullable=false, length=20) private Papel papel;
    @Column(nullable=false, length=500) private String mensagem;
    @Column(nullable=false) private Instant criadaEm;

    protected Notificacao() {}

    Notificacao(EventoDeContrato evento, Papel papel, String mensagem) {
        this.id = UUID.randomUUID();
        this.eventId = evento.eventId();
        this.tipoEvento = evento.eventType();
        this.contratoId = evento.contratoId();
        this.destinatarioId = evento.participante(papel);
        this.papel = papel;
        this.mensagem = mensagem;
        this.criadaEm = Instant.now();
    }

    public UUID getId(){return id;} public UUID getEventId(){return eventId;} public String getTipoEvento(){return tipoEvento;}
    public UUID getContratoId(){return contratoId;} public UUID getDestinatarioId(){return destinatarioId;}
    public Papel getPapel(){return papel;} public String getMensagem(){return mensagem;} public Instant getCriadaEm(){return criadaEm;}
}
