package br.com.freela.contrato.infrastructure.messaging.outbox;

import br.com.freela.contrato.application.ContratoApplicationService;
import br.com.freela.contrato.application.CriarContratoCommand;
import br.com.freela.contrato.application.port.PublicadorDeEventos;
import br.com.freela.contrato.domain.repository.ContratoRepository;
import br.com.freela.contrato.domain.model.ContratoId;
import br.com.freela.contrato.infrastructure.observabilidade.Correlacao;
import br.com.freela.contrato.support.IntegrationTest;
import io.micrometer.tracing.Tracer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.transaction.IllegalTransactionStateException;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.kafka.KafkaContainer;
import tools.jackson.databind.json.JsonMapper;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@IntegrationTest
class OutboxIntegrationTest {

    @Autowired ContratoApplicationService service;
    @Autowired ContratoRepository contratos;
    @Autowired SpringDataOutboxRepository outbox;
    @Autowired OutboxRelayService relay;
    @Autowired PublicadorDeEventos publicador;
    @Autowired TransactionTemplate transacao;
    @Autowired KafkaContainer kafka;
    @Autowired JsonMapper json;
    @Autowired Tracer tracer;
    @Value("${freela.kafka.topico-contratos}") String topico;

    private CriarContratoCommand novoComando() {
        return new CriarContratoCommand(UUID.randomUUID(), UUID.randomUUID(), "API de pagamentos", new BigDecimal("3500"));
    }

    @Test
    void deveGravarContratoEEventoNaMesmaTransacao() {
        MDC.put(Correlacao.MDC_CORRELATION_ID, "teste-outbox-001");
        try {
            var contrato = service.criar(novoComando());

            var eventos = outbox.findByAggregateIdOrderByIdAsc(contrato.id().valor());
            assertThat(eventos).singleElement().satisfies(evento -> {
                assertThat(evento.getEventType()).isEqualTo("ContratoCriado");
                assertThat(evento.getStatus()).isEqualTo(StatusOutbox.PENDENTE);
                assertThat(evento.getChave()).isEqualTo(contrato.id().toString());
                assertThat(evento.getCorrelationId()).isEqualTo("teste-outbox-001");
            });

            var envelope = json.readTree(eventos.getFirst().getPayload());
            assertThat(envelope.get("eventId").asString()).isEqualTo(eventos.getFirst().getEventId().toString());
            assertThat(envelope.get("contratoId").asString()).isEqualTo(contrato.id().toString());
            assertThat(envelope.get("correlationId").asString()).isEqualTo("teste-outbox-001");
            assertThat(envelope.get("eventVersion").asInt()).isEqualTo(1);
            assertThat(eventos.getFirst().getPayload()).contains("\"valor\":3500.00");
            assertThat(envelope.get("payload").get("status").asString()).isEqualTo("ATIVO");
        } finally {
            MDC.clear();
        }
    }

    @Test
    void rollbackNaoDeveDeixarContratoNemEventoNaOutbox() {
        var id = new AtomicReference<ContratoId>();

        transacao.executeWithoutResult(status -> {
            id.set(service.criar(novoComando()).id());
            status.setRollbackOnly();
        });

        assertThat(contratos.buscarPorId(id.get())).isEmpty();
        assertThat(outbox.findByAggregateIdOrderByIdAsc(id.get().valor())).isEmpty();
    }

    @Test
    void publicadorExigeTransacaoAtiva() {
        assertThatThrownBy(() -> publicador.publicar(List.of()))
                .isInstanceOf(IllegalTransactionStateException.class);
    }

    @Test
    void relayDevePublicarCicloDeVidaComChaveDoContratoNaMesmaParticaoEmOrdem() throws Exception {
        var contrato = service.criar(novoComando());
        var id = contrato.id().valor();
        service.registrarEntrega(id);
        service.concluir(id);

        relay.publicarPendentes();

        assertThat(outbox.findByAggregateIdOrderByIdAsc(id))
                .extracting(OutboxEventoJpaEntity::getStatus)
                .containsOnly(StatusOutbox.PUBLICADO);

        var registros = consumirDoContrato(id.toString(), 3);
        assertThat(registros).extracting(r -> header(r, "eventType"))
                .containsExactly("ContratoCriado", "EntregaRegistrada", "ContratoConcluido");
        assertThat(registros).extracting(ConsumerRecord::key).containsOnly(id.toString());
        assertThat(registros).extracting(ConsumerRecord::partition).containsOnly(registros.getFirst().partition());
        assertThat(registros).extracting(ConsumerRecord::offset).isSorted();
        assertThat(registros).allSatisfy(r -> {
            assertThat(header(r, "eventId")).isNotBlank();
            assertThat(header(r, "contratoId")).isEqualTo(id.toString());
            assertThat(header(r, "correlationId")).isNotBlank();
        });

        try (var admin = AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))) {
            var descricao = admin.describeTopics(List.of(topico)).allTopicNames().get().get(topico);
            assertThat(descricao.partitions()).hasSize(3);
        }
    }

    @Test
    void mensagemPublicadaPeloRelayContinuaOTraceDaOperacao() {
        var span = tracer.nextSpan().name("requisicao-de-teste").start();
        UUID id;
        try (var escopo = tracer.withSpan(span)) {
            id = service.criar(novoComando()).id().valor();
        } finally {
            span.end();
        }

        relay.publicarPendentes();

        assertThat(consumirDoContrato(id.toString(), 1)).singleElement()
                .satisfies(r -> assertThat(header(r, "traceparent")).contains(span.context().traceId()));
    }

    private List<ConsumerRecord<String, String>> consumirDoContrato(String chave, int esperado) {
        var props = Map.<String, Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "teste-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        var encontrados = new ArrayList<ConsumerRecord<String, String>>();
        try (var consumer = new KafkaConsumer<>(props, new StringDeserializer(), new StringDeserializer())) {
            consumer.subscribe(List.of(topico));
            long limite = System.currentTimeMillis() + 20_000;
            while (encontrados.size() < esperado && System.currentTimeMillis() < limite) {
                for (var registro : consumer.poll(Duration.ofMillis(500))) {
                    if (chave.equals(registro.key())) encontrados.add(registro);
                }
            }
        }
        return encontrados;
    }

    private static String header(ConsumerRecord<String, String> registro, String nome) {
        var header = registro.headers().lastHeader(nome);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }
}
