package br.com.freela.reputacao.infrastructure.messaging;

import br.com.freela.reputacao.application.ReputacaoService;
import br.com.freela.reputacao.domain.EventoDeContrato;
import br.com.freela.reputacao.infrastructure.observabilidade.ContextoMdc;
import br.com.freela.reputacao.infrastructure.observabilidade.Correlacao;
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

    private final ReputacaoService service;
    private final JsonMapper json;

    public ContratoEventosListener(ReputacaoService service, JsonMapper json) {
        this.service = service;
        this.json = json;
    }

    @KafkaListener(id = "reputacao-contrato-eventos", topics = "${freela.kafka.topico-contratos}", idIsGroup = false)
    public void receber(ConsumerRecord<String, String> registro) {
        try (var mdc = ContextoMdc.com(contextoDeLog(registro))) {
            processar(registro);
        }
    }

    private void processar(ConsumerRecord<String, String> registro) {
        long inicio = System.nanoTime();
        try {
            var evento = ler(registro.value());
            log.info("reputacao.evento.recebido eventType={} particao={} offset={}",
                    evento.eventType(), registro.partition(), registro.offset());
            var aplicado = service.processar(evento);
            log.info("reputacao.evento.processado eventType={} aplicado={} durationMs={}",
                    evento.eventType(), aplicado, duracaoMs(inicio));
        } catch (RuntimeException ex) {
            log.warn("reputacao.evento.falha particao={} offset={} durationMs={} erro={}",
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
