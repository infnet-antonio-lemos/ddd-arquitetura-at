package br.com.freela.notificacao.infrastructure.messaging;

import br.com.freela.notificacao.application.NotificacaoService;
import br.com.freela.notificacao.domain.EventoDeContrato;
import br.com.freela.notificacao.infrastructure.observabilidade.ContextoMdc;
import br.com.freela.notificacao.infrastructure.observabilidade.Correlacao;
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

    private final NotificacaoService service;
    private final ReceptorIdempotente receptor;
    private final JsonMapper json;

    public ContratoEventosListener(NotificacaoService service, ReceptorIdempotente receptor, JsonMapper json) {
        this.service = service;
        this.receptor = receptor;
        this.json = json;
    }

    @KafkaListener(id = "notificacao-contrato-eventos", topics = "${freela.kafka.topico-contratos}", idIsGroup = false)
    public void receber(ConsumerRecord<String, String> registro) {
        try (var mdc = ContextoMdc.com(contextoDeLog(registro))) {
            processar(registro);
        }
    }

    private void processar(ConsumerRecord<String, String> registro) {
        long inicio = System.nanoTime();
        try {
            var evento = ler(registro.value());
            log.info("notificacao.evento.recebido eventType={} particao={} offset={}",
                    evento.eventType(), registro.partition(), registro.offset());
            var notificacoes = receptor.processarUmaVez(evento.eventId(), evento.eventType(), evento.contratoId(),
                    () -> service.notificar(evento));
            if (notificacoes.isPresent()) {
                log.info("notificacao.evento.processado eventType={} notificacoes={} durationMs={}",
                        evento.eventType(), notificacoes.get().size(), duracaoMs(inicio));
            } else {
                log.info("notificacao.evento.duplicado eventType={} durationMs={}", evento.eventType(), duracaoMs(inicio));
            }
        } catch (RuntimeException ex) {
            log.warn("notificacao.evento.falha particao={} offset={} durationMs={} erro={}",
                    registro.partition(), registro.offset(), duracaoMs(inicio), ex.toString());
            throw ex;
        }
    }

    private EventoDeContrato ler(String valor) {
        if (valor == null) throw new EventoInvalidoException("mensagem sem conteúdo");
        try {
            return json.readValue(valor, ContratoEventoMensagem.class).paraEvento();
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
