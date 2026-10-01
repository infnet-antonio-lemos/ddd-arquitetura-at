package br.com.freela.contrato.domain.repository;
import br.com.freela.contrato.domain.model.Contrato;
import br.com.freela.contrato.domain.model.ContratoId;
import java.util.List;
import java.util.Optional;
public interface ContratoRepository {
    Contrato salvar(Contrato contrato);
    Optional<Contrato> buscarPorId(ContratoId id);
    List<Contrato> listar();
}
