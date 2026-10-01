package br.com.freela.reputacao.domain;

import jakarta.persistence.*;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "reputacoes")
public class ReputacaoFreelancer {
    @Id private UUID freelancerId;
    @Column(nullable=false) private int contratosRecebidos;
    @Column(nullable=false) private int contratosConcluidos;
    @Column(nullable=false) private int contratosCancelados;
    @Column(nullable=false, precision=19, scale=2) private BigDecimal valorTotalConcluido;
    @Column(nullable=false) private Instant atualizadoEm;

    protected ReputacaoFreelancer() {}

    public static ReputacaoFreelancer nova(UUID freelancerId) {
        var reputacao = new ReputacaoFreelancer();
        reputacao.freelancerId = freelancerId;
        reputacao.valorTotalConcluido = BigDecimal.ZERO;
        reputacao.atualizadoEm = Instant.now();
        return reputacao;
    }

    /** Aplica o efeito do evento; devolve false quando o evento não afeta a reputação. */
    public boolean aplicar(EventoDeContrato evento) {
        switch (evento.eventType()) {
            case "ContratoCriado" -> contratosRecebidos++;
            case "ContratoConcluido" -> {
                contratosConcluidos++;
                valorTotalConcluido = valorTotalConcluido.add(evento.valor());
            }
            case "ContratoCancelado" -> contratosCancelados++;
            default -> {
                return false;
            }
        }
        atualizadoEm = Instant.now();
        return true;
    }

    public UUID getFreelancerId(){return freelancerId;} public int getContratosRecebidos(){return contratosRecebidos;}
    public int getContratosConcluidos(){return contratosConcluidos;} public int getContratosCancelados(){return contratosCancelados;}
    public BigDecimal getValorTotalConcluido(){return valorTotalConcluido;} public Instant getAtualizadoEm(){return atualizadoEm;}
}
