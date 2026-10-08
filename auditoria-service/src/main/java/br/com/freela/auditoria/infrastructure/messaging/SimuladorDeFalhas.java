package br.com.freela.auditoria.infrastructure.messaging;

import org.springframework.stereotype.Component;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class SimuladorDeFalhas {
    private final Set<UUID> contratos = ConcurrentHashMap.newKeySet();

    public void ativar(UUID contratoId) {
        contratos.add(contratoId);
    }

    public void desativar(UUID contratoId) {
        contratos.remove(contratoId);
    }

    void verificar(UUID contratoId) {
        if (contratos.contains(contratoId)) {
            throw new IllegalStateException("falha simulada no processamento do contrato " + contratoId);
        }
    }
}
