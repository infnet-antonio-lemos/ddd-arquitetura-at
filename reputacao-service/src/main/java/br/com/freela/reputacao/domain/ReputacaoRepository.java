package br.com.freela.reputacao.domain;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface ReputacaoRepository extends JpaRepository<ReputacaoFreelancer, UUID> {
    List<ReputacaoFreelancer> findAllByOrderByFreelancerIdAsc();
}
