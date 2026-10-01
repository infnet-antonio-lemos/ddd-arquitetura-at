package br.com.freela.reputacao.domain;

import java.math.BigDecimal;
import java.util.UUID;

/** O que aconteceu com um contrato, na visão do serviço de reputação. */
public record EventoDeContrato(UUID eventId, String eventType, UUID contratoId, UUID freelancerId, BigDecimal valor) {}
