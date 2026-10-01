package br.com.freela.contrato.infrastructure.messaging.kafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicosConfig {

    @Bean
    NewTopic topicoContratos(@Value("${freela.kafka.topico-contratos}") String nome,
                             @Value("${freela.kafka.particoes:3}") int particoes) {
        return TopicBuilder.name(nome).partitions(particoes).replicas(1).build();
    }
}
