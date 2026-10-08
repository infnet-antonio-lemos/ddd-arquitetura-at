package br.com.freela.reputacao.infrastructure.messaging;

import br.com.freela.reputacao.infrastructure.observabilidade.Correlacao;
import jakarta.persistence.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.time.Instant;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

@Entity
@Table(name = "mensagens_com_falha",
        uniqueConstraints = @UniqueConstraint(name = "uk_mensagens_com_falha_origem", columnNames = {"topico", "particao", "kafkaOffset"}),
        indexes = @Index(name = "idx_mensagens_com_falha_contrato", columnList = "contratoId, status"))
public class MensagemComFalha {
    public enum Status { FALHA, INVALIDA, ESTACIONADA, REPROCESSADA }

    public static final Set<Status> PENDENTES = EnumSet.of(Status.FALHA, Status.ESTACIONADA);
    private static final int TAMANHO_MAXIMO_ERRO = 1000;

    @Id @GeneratedValue(strategy = GenerationType.IDENTITY) private Long id;
    @Enumerated(EnumType.STRING) @Column(nullable=false, length=20) private Status status;
    @Column(nullable=false, length=200) private String topico;
    @Column(nullable=false) private int particao;
    @Column(nullable=false) private long kafkaOffset;
    @Column(length=100) private String chave;
    private UUID eventId;
    @Column(length=80) private String eventType;
    private UUID contratoId;
    @Column(length=100) private String correlationId;
    @Column(columnDefinition="text") private String conteudo;
    @Column(length=TAMANHO_MAXIMO_ERRO) private String erro;
    @Column(nullable=false) private int tentativasDeReprocessamento;
    @Column(nullable=false) private Instant registradaEm;
    private Instant reprocessadaEm;

    protected MensagemComFalha() {}

    static MensagemComFalha de(ConsumerRecord<?, ?> registro, Status status, String erro) {
        var contexto = ContratoEventosListener.contextoDeLog(registro);
        var mensagem = new MensagemComFalha();
        mensagem.status = status;
        mensagem.topico = registro.topic();
        mensagem.particao = registro.partition();
        mensagem.kafkaOffset = registro.offset();
        mensagem.chave = registro.key() == null ? null : registro.key().toString();
        mensagem.eventId = uuid(contexto.get(Correlacao.MDC_EVENT_ID));
        mensagem.eventType = contexto.get(Correlacao.MDC_EVENT_TYPE);
        var contratoDoHeader = uuid(contexto.get(Correlacao.MDC_CONTRATO_ID));
        mensagem.contratoId = contratoDoHeader != null ? contratoDoHeader : uuid(mensagem.chave);
        mensagem.correlationId = contexto.get(Correlacao.MDC_CORRELATION_ID);
        mensagem.conteudo = registro.value() == null ? null : registro.value().toString();
        mensagem.erro = truncar(erro);
        mensagem.registradaEm = Instant.now();
        return mensagem;
    }

    void marcarReprocessada() {
        status = Status.REPROCESSADA;
        tentativasDeReprocessamento++;
        reprocessadaEm = Instant.now();
    }

    void registrarFalhaNoReprocessamento(String erro) {
        status = Status.FALHA;
        tentativasDeReprocessamento++;
        this.erro = truncar(erro);
    }

    Map<String, String> contextoDeLog() {
        var contexto = new HashMap<String, String>();
        if (correlationId != null) contexto.put(Correlacao.MDC_CORRELATION_ID, correlationId);
        if (contratoId != null) contexto.put(Correlacao.MDC_CONTRATO_ID, contratoId.toString());
        if (eventId != null) contexto.put(Correlacao.MDC_EVENT_ID, eventId.toString());
        if (eventType != null) contexto.put(Correlacao.MDC_EVENT_TYPE, eventType);
        return contexto;
    }

    private static UUID uuid(String valor) {
        try {
            return valor == null ? null : UUID.fromString(valor);
        } catch (IllegalArgumentException ex) {
            return null;
        }
    }

    private static String truncar(String erro) {
        return erro == null ? null : erro.substring(0, Math.min(erro.length(), TAMANHO_MAXIMO_ERRO));
    }

    public Long getId(){return id;} public Status getStatus(){return status;} public String getTopico(){return topico;}
    public int getParticao(){return particao;} public long getKafkaOffset(){return kafkaOffset;} public String getChave(){return chave;}
    public UUID getEventId(){return eventId;} public String getEventType(){return eventType;} public UUID getContratoId(){return contratoId;}
    public String getCorrelationId(){return correlationId;} public String getConteudo(){return conteudo;} public String getErro(){return erro;}
    public int getTentativasDeReprocessamento(){return tentativasDeReprocessamento;}
    public Instant getRegistradaEm(){return registradaEm;} public Instant getReprocessadaEm(){return reprocessadaEm;}
}
