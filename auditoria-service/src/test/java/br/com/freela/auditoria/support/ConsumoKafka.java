package br.com.freela.auditoria.support;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.core.KafkaAdmin;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import static br.com.freela.auditoria.support.MensagensDeContrato.TOPICO;
import static org.awaitility.Awaitility.await;

public final class ConsumoKafka {
    private ConsumoKafka() {}

    public static void aguardarConsumoCompleto(KafkaAdmin kafkaAdmin, String grupo) {
        try (var admin = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            await().atMost(Duration.ofSeconds(60)).until(() -> lag(admin, grupo) == 0);
        }
    }

    private static long lag(AdminClient admin, String grupo) throws Exception {
        var particoes = admin.describeTopics(List.of(TOPICO)).allTopicNames().get().get(TOPICO).partitions().stream()
                .map(p -> new TopicPartition(TOPICO, p.partition())).toList();
        var fim = admin.listOffsets(particoes.stream().collect(Collectors.toMap(Function.identity(), p -> OffsetSpec.latest())))
                .all().get();
        var consumidos = admin.listConsumerGroupOffsets(grupo).partitionsToOffsetAndMetadata().get();
        return particoes.stream()
                .mapToLong(p -> fim.get(p).offset() - Optional.ofNullable(consumidos.get(p)).map(OffsetAndMetadata::offset).orElse(0L))
                .sum();
    }
}
