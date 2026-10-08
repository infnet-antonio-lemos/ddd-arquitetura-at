package br.com.freela.reputacao.infrastructure.messaging;

import br.com.freela.reputacao.application.ReputacaoService;
import br.com.freela.reputacao.domain.EventoDeContrato;
import br.com.freela.reputacao.infrastructure.observabilidade.ContextoMdc;
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

    private final ReputacaoService service;
    private final ReceptorIdempotente receptor;
    private final MensagemComFalhaRepository falhas;
    private final SimuladorDeFalhas simulador;
    private final JsonMapper json;

    public ProcessadorDeEventos(ReputacaoService service, ReceptorIdempotente receptor,
                                MensagemComFalhaRepository falhas, SimuladorDeFalhas simulador, JsonMapper json) {
        this.service = service;
        this.receptor = receptor;
        this.falhas = falhas;
        this.simulador = simulador;
        this.json = json;
    }

    public void receber(ConsumerRecord<String, String> registro) {
        var evento = ler(registro.value());
        log.info("reputacao.evento.recebido eventType={} particao={} offset={}",
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
        log.info("reputacao.reprocessamento.fim contratoId={} reprocessadas={} pendentes={}",
                contratoId, reprocessadas, pendentes.size() - reprocessadas);
        return new Reprocessamento(contratoId, reprocessadas, pendentes.size() - reprocessadas);
    }

    private boolean reprocessar(MensagemComFalha mensagem) {
        try {
            processar(ler(mensagem.getConteudo()));
            mensagem.marcarReprocessada();
            return true;
        } catch (RuntimeException ex) {
            mensagem.registrarFalhaNoReprocessamento(ex.toString());
            log.warn("reputacao.reprocessamento.falha mensagemId={} offset={} erro={}",
                    mensagem.getId(), mensagem.getKafkaOffset(), ex.toString());
            return false;
        } finally {
            falhas.save(mensagem);
        }
    }

    private void processar(EventoDeContrato evento) {
        long inicio = System.nanoTime();
        var aplicado = receptor.processarUmaVez(evento.eventId(), evento.eventType(), evento.contratoId(), () -> {
            simulador.verificar(evento.contratoId());
            return service.processar(evento);
        });
        if (aplicado.isPresent()) {
            log.info("reputacao.evento.processado eventType={} aplicado={} durationMs={}",
                    evento.eventType(), aplicado.get(), duracaoMs(inicio));
        } else {
            log.info("reputacao.evento.duplicado eventType={} durationMs={}", evento.eventType(), duracaoMs(inicio));
        }
    }

    private void estacionar(ConsumerRecord<String, String> registro, EventoDeContrato evento) {
        if (!falhas.jaRegistrada(registro)) {
            falhas.save(MensagemComFalha.de(registro, MensagemComFalha.Status.ESTACIONADA, "contrato com falha pendente"));
        }
        log.warn("reputacao.evento.estacionado eventType={} motivo=contrato-com-falha-pendente", evento.eventType());
    }

    private EventoDeContrato ler(String valor) {
        if (valor == null) throw new EventoInvalidoException("mensagem sem conteúdo");
        try {
            return json.readValue(valor, ContratoEventoMensagem.class).paraEvento();
        } catch (JacksonException ex) {
            throw new EventoInvalidoException("JSON inválido (" + ex.getClass().getSimpleName() + ")", ex);
        }
    }

    private static long duracaoMs(long inicioNanos) {
        return (System.nanoTime() - inicioNanos) / 1_000_000;
    }

    public record Reprocessamento(UUID contratoId, int reprocessadas, int pendentes) {}
}
