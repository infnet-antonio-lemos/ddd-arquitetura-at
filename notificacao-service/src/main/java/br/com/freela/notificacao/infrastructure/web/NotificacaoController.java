package br.com.freela.notificacao.infrastructure.web;

import br.com.freela.notificacao.application.NotificacaoService;
import br.com.freela.notificacao.domain.Notificacao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api/notificacoes")
public class NotificacaoController {
    private static final Logger log = LoggerFactory.getLogger(NotificacaoController.class);
    private final NotificacaoService service;

    public NotificacaoController(NotificacaoService service) {
        this.service = service;
    }

    @GetMapping
    public List<NotificacaoResponse> listar() {
        log.info("http.notificacao.listar");
        return resposta(service.listar());
    }

    @GetMapping("/contrato/{contratoId}")
    public List<NotificacaoResponse> porContrato(@PathVariable UUID contratoId) {
        log.info("http.notificacao.por-contrato contratoId={}", contratoId);
        return resposta(service.porContrato(contratoId));
    }

    @GetMapping("/destinatario/{destinatarioId}")
    public List<NotificacaoResponse> porDestinatario(@PathVariable UUID destinatarioId) {
        log.info("http.notificacao.por-destinatario destinatarioId={}", destinatarioId);
        return resposta(service.porDestinatario(destinatarioId));
    }

    private static List<NotificacaoResponse> resposta(List<Notificacao> notificacoes) {
        return notificacoes.stream().map(NotificacaoResponse::from).toList();
    }
}
