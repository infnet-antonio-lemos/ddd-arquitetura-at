package br.com.freela.auditoria.infrastructure.messaging;

import br.com.freela.auditoria.application.AuditoriaService;
import br.com.freela.auditoria.domain.EventoRecebido;
import br.com.freela.auditoria.infrastructure.observabilidade.ContextoMdc;
import br.com.freela.auditoria.infrastructure.observabilidade.Correlacao;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
public class ContratoEventosListener {
    private static final Logger log = LoggerFactory.getLogger(ContratoEventosListener.class);
    private static final List<String> HEADERS_DE_CONTEXTO = List.of(Correlacao.MDC_CORRELATION_ID,
            Correlacao.MDC_CONTRATO_ID, Correlacao.MDC_EVENT_ID, Correlacao.MDC_EVENT_TYPE);

    private final AuditoriaService service;
    private final ReceptorIdempotente receptor;
    private final JsonMapper json;

    public ContratoEventosListener(AuditoriaService service, ReceptorIdempotente receptor, JsonMapper json) {
        this.service = service;
        this.receptor = receptor;
        this.json = json;
    }

    @KafkaListener(id = "auditoria-contrato-eventos", topics = "${freela.kafka.topico-contratos}", idIsGroup = false)
    public void receber(ConsumerRecord<String, String> registro) {
        try (var mdc = ContextoMdc.com(contextoDeLog(registro))) {
            processar(registro);
        }
    }

    private void processar(ConsumerRecord<String, String> registro) {
        long inicio = System.nanoTime();
        try {
            var evento = ler(registro);
            log.info("auditoria.evento.recebido eventType={} particao={} offset={}",
                    evento.eventType(), registro.partition(), registro.offset());
            var auditado = receptor.processarUmaVez(evento.eventId(), evento.eventType(), evento.contratoId(),
                    () -> service.registrar(evento));
            if (auditado.isPresent()) {
                log.info("auditoria.evento.processado eventType={} auditoriaId={} durationMs={}",
                        evento.eventType(), auditado.get().getId(), duracaoMs(inicio));
            } else {
                log.info("auditoria.evento.duplicado eventType={} durationMs={}", evento.eventType(), duracaoMs(inicio));
            }
        } catch (RuntimeException ex) {
            log.warn("auditoria.evento.falha particao={} offset={} durationMs={} erro={}",
                    registro.partition(), registro.offset(), duracaoMs(inicio), ex.toString());
            throw ex;
        }
    }

    private EventoRecebido ler(ConsumerRecord<String, String> registro) {
        var valor = registro.value();
        if (valor == null) throw new EventoInvalidoException("mensagem sem conteúdo");
        var origem = new EventoRecebido.Origem(registro.topic(), registro.partition(), registro.offset());
        try {
            return json.readValue(valor, ContratoEventoMensagem.class).paraEvento(valor, origem);
        } catch (JacksonException ex) {
            throw new EventoInvalidoException("JSON inválido: " + ex.getOriginalMessage(), ex);
        }
    }

    private static Map<String, String> contextoDeLog(ConsumerRecord<String, String> registro) {
        var contexto = new HashMap<String, String>();
        for (var chave : HEADERS_DE_CONTEXTO) {
            var header = registro.headers().lastHeader(chave);
            var valor = header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
            if (Correlacao.valido(valor)) contexto.put(chave, valor);
        }
        return contexto;
    }

    private static long duracaoMs(long inicioNanos) {
        return (System.nanoTime() - inicioNanos) / 1_000_000;
    }
}
