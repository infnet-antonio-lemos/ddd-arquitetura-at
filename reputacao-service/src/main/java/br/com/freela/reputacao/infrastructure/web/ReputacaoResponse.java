package br.com.freela.reputacao.infrastructure.web;

import br.com.freela.reputacao.domain.ReputacaoFreelancer;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record ReputacaoResponse(UUID freelancerId, int contratosRecebidos, int contratosConcluidos, int contratosCancelados,
                                BigDecimal valorTotalConcluido, Instant atualizadoEm) {
    static ReputacaoResponse from(ReputacaoFreelancer r) {
        return new ReputacaoResponse(r.getFreelancerId(), r.getContratosRecebidos(), r.getContratosConcluidos(),
                r.getContratosCancelados(), r.getValorTotalConcluido(), r.getAtualizadoEm());
    }
}
