package br.com.freela.notificacao.infrastructure.web;

import br.com.freela.notificacao.infrastructure.messaging.MensagemComFalha;
import br.com.freela.notificacao.infrastructure.messaging.MensagemComFalhaRepository;
import br.com.freela.notificacao.infrastructure.messaging.ProcessadorDeEventos;
import br.com.freela.notificacao.infrastructure.messaging.SimuladorDeFalhas;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api/notificacoes")
public class FalhasController {
    private static final Logger log = LoggerFactory.getLogger(FalhasController.class);
    private final MensagemComFalhaRepository falhas;
    private final ProcessadorDeEventos processador;
    private final SimuladorDeFalhas simulador;

    public FalhasController(MensagemComFalhaRepository falhas, ProcessadorDeEventos processador, SimuladorDeFalhas simulador) {
        this.falhas = falhas;
        this.processador = processador;
        this.simulador = simulador;
    }

    @GetMapping("/falhas")
    public List<MensagemComFalhaResponse> listar(@RequestParam(required = false) MensagemComFalha.Status status) {
        log.info("http.falhas.listar status={}", status);
        var mensagens = status == null ? falhas.findAllByOrderByIdAsc() : falhas.findByStatusOrderByIdAsc(status);
        return mensagens.stream().map(MensagemComFalhaResponse::from).toList();
    }

    @PostMapping("/falhas/contrato/{contratoId}/reprocessamento")
    public ProcessadorDeEventos.Reprocessamento reprocessar(@PathVariable UUID contratoId) {
        log.info("http.falhas.reprocessar contratoId={}", contratoId);
        return processador.reprocessar(contratoId);
    }

    @PostMapping("/simulacoes/falhas/{contratoId}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void ativarSimulacao(@PathVariable UUID contratoId) {
        log.warn("http.simulacao-falha.ativada contratoId={}", contratoId);
        simulador.ativar(contratoId);
    }

    @DeleteMapping("/simulacoes/falhas/{contratoId}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void desativarSimulacao(@PathVariable UUID contratoId) {
        log.info("http.simulacao-falha.desativada contratoId={}", contratoId);
        simulador.desativar(contratoId);
    }

    public record MensagemComFalhaResponse(Long id, MensagemComFalha.Status status, String topico, int particao, long offset,
                                           UUID eventId, String eventType, UUID contratoId, String correlationId,
                                           String erro, int tentativasDeReprocessamento, Instant registradaEm,
                                           Instant reprocessadaEm, String conteudo) {
        static MensagemComFalhaResponse from(MensagemComFalha m) {
            return new MensagemComFalhaResponse(m.getId(), m.getStatus(), m.getTopico(), m.getParticao(), m.getKafkaOffset(),
                    m.getEventId(), m.getEventType(), m.getContratoId(), m.getCorrelationId(), m.getErro(),
                    m.getTentativasDeReprocessamento(), m.getRegistradaEm(), m.getReprocessadaEm(), m.getConteudo());
        }
    }
}
