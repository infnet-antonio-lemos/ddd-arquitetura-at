package br.com.freela.reputacao.infrastructure.messaging;

import br.com.freela.reputacao.application.ReputacaoService;
import br.com.freela.reputacao.domain.EventoDeContrato;
import br.com.freela.reputacao.support.IntegrationTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

@IntegrationTest
class ReceptorIdempotenteIntegrationTest {
    private static final int ENTREGAS_SIMULTANEAS = 8;

    @Autowired ReceptorIdempotente receptor;
    @Autowired ReputacaoService service;

    @Test
    void mesmoEventoProcessadoAoMesmoTempoPorVariasThreadsTemEfeitoUnico() throws Exception {
        var freelancer = UUID.randomUUID();
        var evento = new EventoDeContrato(UUID.randomUUID(), "ContratoCriado", UUID.randomUUID(), freelancer, new BigDecimal("800.00"));
        var largada = new CountDownLatch(1);

        try (var threads = Executors.newFixedThreadPool(ENTREGAS_SIMULTANEAS)) {
            var resultados = IntStream.range(0, ENTREGAS_SIMULTANEAS)
                    .mapToObj(i -> CompletableFuture.supplyAsync(() -> {
                        aguardar(largada);
                        return receptor.processarUmaVez(evento.eventId(), evento.eventType(), evento.contratoId(),
                                () -> service.processar(evento));
                    }, threads))
                    .toList();
            largada.countDown();

            assertThat(resultados).map(CompletableFuture::join).filteredOn(r -> r.isPresent()).hasSize(1);
        }
        assertThat(service.buscar(freelancer)).hasValueSatisfying(r -> assertThat(r.getContratosRecebidos()).isEqualTo(1));
    }

    private static void aguardar(CountDownLatch largada) {
        try {
            largada.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
