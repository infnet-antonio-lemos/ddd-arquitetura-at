package br.com.freela.reputacao.domain;

import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;

import java.util.List;
import java.util.UUID;

public interface ReputacaoRepository extends JpaRepository<ReputacaoFreelancer, UUID> {
    List<ReputacaoFreelancer> findAllByOrderByFreelancerIdAsc();

    @Modifying
    @Query(value = """
            insert into reputacoes (freelancer_id, contratos_recebidos, contratos_concluidos, contratos_cancelados,
                                    valor_total_concluido, atualizado_em)
            values (:freelancerId, 0, 0, 0, 0, now())
            on conflict (freelancer_id) do nothing""", nativeQuery = true)
    void garantirExistencia(UUID freelancerId);

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select r from ReputacaoFreelancer r where r.freelancerId = :freelancerId")
    ReputacaoFreelancer buscarParaAtualizar(UUID freelancerId);
}
