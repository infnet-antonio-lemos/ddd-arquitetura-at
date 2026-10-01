package br.com.freela.reputacao.infrastructure.web;

import br.com.freela.reputacao.application.ReputacaoService;
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
@RequestMapping("/api/reputacoes")
public class ReputacaoController {
    private static final Logger log = LoggerFactory.getLogger(ReputacaoController.class);
    private final ReputacaoService service;

    public ReputacaoController(ReputacaoService service) {
        this.service = service;
    }

    @GetMapping
    public List<ReputacaoResponse> listar() {
        log.info("http.reputacao.listar");
        return service.listar().stream().map(ReputacaoResponse::from).toList();
    }

    @GetMapping("/{freelancerId}")
    public ReputacaoResponse buscar(@PathVariable UUID freelancerId) {
        log.info("http.reputacao.buscar freelancerId={}", freelancerId);
        return service.buscar(freelancerId).map(ReputacaoResponse::from)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND,
                        "Nenhum evento de contrato registrado para o freelancer " + freelancerId));
    }
}
