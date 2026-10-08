package br.com.freela.reputacao.infrastructure.messaging;

import br.com.freela.reputacao.infrastructure.observabilidade.ContextoMdc;
import br.com.freela.reputacao.infrastructure.observabilidade.Correlacao;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
public class ContratoEventosListener {
    private static final Logger log = LoggerFactory.getLogger(ContratoEventosListener.class);
    private static final List<String> HEADERS_DE_CONTEXTO = List.of(Correlacao.MDC_CORRELATION_ID,
            Correlacao.MDC_CONTRATO_ID, Correlacao.MDC_EVENT_ID, Correlacao.MDC_EVENT_TYPE);

    private final ProcessadorDeEventos processador;

    public ContratoEventosListener(ProcessadorDeEventos processador) {
        this.processador = processador;
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
            processador.receber(registro);
        } catch (RuntimeException ex) {
            log.warn("reputacao.evento.falha particao={} offset={} durationMs={} erro={}",
                    registro.partition(), registro.offset(), (System.nanoTime() - inicio) / 1_000_000, ex.toString());
            throw ex;
        }
    }

    static Map<String, String> contextoDeLog(ConsumerRecord<?, ?> registro) {
        var contexto = new HashMap<String, String>();
        for (var chave : HEADERS_DE_CONTEXTO) {
            var header = registro.headers().lastHeader(chave);
            var valor = header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
            if (Correlacao.valido(valor)) contexto.put(chave, valor);
        }
        return contexto;
    }
}
