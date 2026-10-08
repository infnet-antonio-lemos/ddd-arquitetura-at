package br.com.freela.auditoria.infrastructure.messaging;

import br.com.freela.auditoria.application.AuditoriaService;
import br.com.freela.auditoria.domain.EventoRecebido;
import br.com.freela.auditoria.infrastructure.observabilidade.ContextoMdc;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

import java.util.UUID;

@Component
public class ProcessadorDeEventos {
    private static final Logger log = LoggerFactory.getLogger(ProcessadorDeEventos.class);

    private final AuditoriaService service;
    private final ReceptorIdempotente receptor;
    private final MensagemComFalhaRepository falhas;
    private final SimuladorDeFalhas simulador;
    private final JsonMapper json;

    public ProcessadorDeEventos(AuditoriaService service, ReceptorIdempotente receptor,
                                MensagemComFalhaRepository falhas, SimuladorDeFalhas simulador, JsonMapper json) {
        this.service = service;
        this.receptor = receptor;
        this.falhas = falhas;
        this.simulador = simulador;
        this.json = json;
    }

    public void receber(ConsumerRecord<String, String> registro) {
        var evento = ler(registro.value(), new EventoRecebido.Origem(registro.topic(), registro.partition(), registro.offset()));
        log.info("auditoria.evento.recebido eventType={} particao={} offset={}",
                evento.eventType(), registro.partition(), registro.offset());
        if (falhas.contratoBloqueado(evento.contratoId())) {
            estacionar(registro, evento);
        } else {
            processar(evento);
        }
    }

    public Reprocessamento reprocessar(UUID contratoId) {
        var pendentes = falhas.pendentesDoContrato(contratoId);
        int reprocessadas = 0;
        for (var mensagem : pendentes) {
            try (var mdc = ContextoMdc.com(mensagem.contextoDeLog())) {
                if (!reprocessar(mensagem)) break;
            }
            reprocessadas++;
        }
        log.info("auditoria.reprocessamento.fim contratoId={} reprocessadas={} pendentes={}",
                contratoId, reprocessadas, pendentes.size() - reprocessadas);
        return new Reprocessamento(contratoId, reprocessadas, pendentes.size() - reprocessadas);
    }

    private boolean reprocessar(MensagemComFalha mensagem) {
        try {
            processar(ler(mensagem.getConteudo(),
                    new EventoRecebido.Origem(mensagem.getTopico(), mensagem.getParticao(), mensagem.getKafkaOffset())));
            mensagem.marcarReprocessada();
            return true;
        } catch (RuntimeException ex) {
            mensagem.registrarFalhaNoReprocessamento(ex.toString());
            log.warn("auditoria.reprocessamento.falha mensagemId={} offset={} erro={}",
                    mensagem.getId(), mensagem.getKafkaOffset(), ex.toString());
            return false;
        } finally {
            falhas.save(mensagem);
        }
    }

    private void processar(EventoRecebido evento) {
        long inicio = System.nanoTime();
        var auditado = receptor.processarUmaVez(evento.eventId(), evento.eventType(), evento.contratoId(), () -> {
            simulador.verificar(evento.contratoId());
            return service.registrar(evento);
        });
        if (auditado.isPresent()) {
            log.info("auditoria.evento.processado eventType={} auditoriaId={} durationMs={}",
                    evento.eventType(), auditado.get().getId(), duracaoMs(inicio));
        } else {
            log.info("auditoria.evento.duplicado eventType={} durationMs={}", evento.eventType(), duracaoMs(inicio));
        }
    }

    private void estacionar(ConsumerRecord<String, String> registro, EventoRecebido evento) {
        if (!falhas.jaRegistrada(registro)) {
            falhas.save(MensagemComFalha.de(registro, MensagemComFalha.Status.ESTACIONADA, "contrato com falha pendente"));
        }
        log.warn("auditoria.evento.estacionado eventType={} motivo=contrato-com-falha-pendente", evento.eventType());
    }

    private EventoRecebido ler(String valor, EventoRecebido.Origem origem) {
        if (valor == null) throw new EventoInvalidoException("mensagem sem conteúdo");
        try {
            return json.readValue(valor, ContratoEventoMensagem.class).paraEvento(valor, origem);
        } catch (JacksonException ex) {
            throw new EventoInvalidoException("JSON inválido (" + ex.getClass().getSimpleName() + ")", ex);
        }
    }

    private static long duracaoMs(long inicioNanos) {
        return (System.nanoTime() - inicioNanos) / 1_000_000;
    }

    public record Reprocessamento(UUID contratoId, int reprocessadas, int pendentes) {}
}
