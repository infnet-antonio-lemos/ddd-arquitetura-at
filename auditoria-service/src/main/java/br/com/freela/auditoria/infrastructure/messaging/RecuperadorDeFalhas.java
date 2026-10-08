package br.com.freela.auditoria.infrastructure.messaging;

import br.com.freela.auditoria.infrastructure.observabilidade.ContextoMdc;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.stereotype.Component;

@Component
public class RecuperadorDeFalhas implements ConsumerRecordRecoverer {
    private static final Logger log = LoggerFactory.getLogger(RecuperadorDeFalhas.class);

    private final MensagemComFalhaRepository falhas;
    private final DeadLetterPublishingRecoverer dlt;
    private final String topicoDlt;

    public RecuperadorDeFalhas(MensagemComFalhaRepository falhas, KafkaTemplate<?, ?> kafka,
                               @Value("${freela.kafka.topico-dlt}") String topicoDlt) {
        this.falhas = falhas;
        this.topicoDlt = topicoDlt;
        this.dlt = new DeadLetterPublishingRecoverer(kafka, (registro, erro) -> new TopicPartition(topicoDlt, registro.partition()));
    }

    @Override
    public void accept(ConsumerRecord<?, ?> registro, Exception excecao) {
        var causa = excecao instanceof ListenerExecutionFailedException && excecao.getCause() != null ? excecao.getCause() : excecao;
        var status = causa instanceof EventoInvalidoException ? MensagemComFalha.Status.INVALIDA : MensagemComFalha.Status.FALHA;
        try (var mdc = ContextoMdc.com(ContratoEventosListener.contextoDeLog(registro))) {
            if (!falhas.jaRegistrada(registro)) {
                falhas.save(MensagemComFalha.de(registro, status, causa.toString()));
            }
            dlt.accept(registro, excecao);
            log.error("auditoria.evento.falha-definitiva status={} particao={} offset={} destino={} erro={}",
                    status, registro.partition(), registro.offset(), topicoDlt, causa.toString());
        }
    }
}
