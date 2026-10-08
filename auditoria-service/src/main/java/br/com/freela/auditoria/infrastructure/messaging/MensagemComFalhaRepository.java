package br.com.freela.auditoria.infrastructure.messaging;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Collection;
import java.util.List;
import java.util.UUID;

public interface MensagemComFalhaRepository extends JpaRepository<MensagemComFalha, Long> {
    List<MensagemComFalha> findAllByOrderByIdAsc();
    List<MensagemComFalha> findByStatusOrderByIdAsc(MensagemComFalha.Status status);
    boolean existsByContratoIdAndStatusIn(UUID contratoId, Collection<MensagemComFalha.Status> status);
    List<MensagemComFalha> findByContratoIdAndStatusInOrderByParticaoAscKafkaOffsetAsc(UUID contratoId, Collection<MensagemComFalha.Status> status);
    boolean existsByTopicoAndParticaoAndKafkaOffset(String topico, int particao, long kafkaOffset);

    default boolean contratoBloqueado(UUID contratoId) {
        return existsByContratoIdAndStatusIn(contratoId, MensagemComFalha.PENDENTES);
    }

    default List<MensagemComFalha> pendentesDoContrato(UUID contratoId) {
        return findByContratoIdAndStatusInOrderByParticaoAscKafkaOffsetAsc(contratoId, MensagemComFalha.PENDENTES);
    }

    default boolean jaRegistrada(ConsumerRecord<?, ?> registro) {
        return existsByTopicoAndParticaoAndKafkaOffset(registro.topic(), registro.partition(), registro.offset());
    }
}
