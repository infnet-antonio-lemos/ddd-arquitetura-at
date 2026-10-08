package br.com.freela.auditoria.support;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.core.ConsumerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.awaitility.Awaitility.await;

public final class LeitorDeTopico {
    private LeitorDeTopico() {}

    public static List<ConsumerRecord<String, String>> registrosComChave(ConsumerFactory<String, String> fabrica,
                                                                         String topico, UUID chave) {
        try (var consumidor = fabrica.createConsumer("teste-" + UUID.randomUUID(), null)) {
            consumidor.subscribe(List.of(topico));
            var encontrados = new ArrayList<ConsumerRecord<String, String>>();
            await().atMost(Duration.ofSeconds(30)).pollInSameThread().until(() -> {
                consumidor.poll(Duration.ofMillis(500)).forEach(r -> {
                    if (chave.toString().equals(r.key())) encontrados.add(r);
                });
                return !encontrados.isEmpty();
            });
            return encontrados;
        }
    }
}
