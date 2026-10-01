package br.com.freela.auditoria.infrastructure.web;

import br.com.freela.auditoria.application.AuditoriaService;
import br.com.freela.auditoria.domain.EventoAuditoria;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api/auditoria")
public class AuditoriaController {
    private static final Logger log = LoggerFactory.getLogger(AuditoriaController.class);
    private final AuditoriaService service;

    public AuditoriaController(AuditoriaService service) {
        this.service = service;
    }

    @GetMapping
    public List<EventoAuditoriaResponse> listar() {
        log.info("http.auditoria.listar");
        return resposta(service.listar());
    }

    @GetMapping("/contrato/{contratoId}")
    public List<EventoAuditoriaResponse> porContrato(@PathVariable UUID contratoId) {
        log.info("http.auditoria.por-contrato contratoId={}", contratoId);
        return resposta(service.porContrato(contratoId));
    }

    @GetMapping("/correlacao/{correlationId}")
    public List<EventoAuditoriaResponse> porCorrelacao(@PathVariable String correlationId) {
        log.info("http.auditoria.por-correlacao");
        return resposta(service.porCorrelacao(correlationId));
    }

    @GetMapping("/evento/{eventId}")
    public EventoAuditoriaResponse porEvento(@PathVariable UUID eventId) {
        log.info("http.auditoria.por-evento eventId={}", eventId);
        return service.porEvento(eventId).map(EventoAuditoriaResponse::from)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Evento " + eventId + " não auditado"));
    }

    private static List<EventoAuditoriaResponse> resposta(List<EventoAuditoria> eventos) {
        return eventos.stream().map(EventoAuditoriaResponse::from).toList();
    }
}
