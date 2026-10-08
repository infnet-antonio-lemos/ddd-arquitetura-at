package br.com.freela.reputacao.infrastructure.messaging;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class TratamentoDeFalhasConfig {

    @Bean
    DefaultErrorHandler tratadorDeErros(RecuperadorDeFalhas recuperador,
                                        @Value("${freela.kafka.falhas.retentativas}") long retentativas,
                                        @Value("${freela.kafka.falhas.intervalo-ms}") long intervaloMs) {
        var tratador = new DefaultErrorHandler(recuperador, new FixedBackOff(intervaloMs, retentativas));
        tratador.addNotRetryableExceptions(EventoInvalidoException.class);
        return tratador;
    }
}
