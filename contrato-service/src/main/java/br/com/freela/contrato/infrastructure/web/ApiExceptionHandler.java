package br.com.freela.contrato.infrastructure.web;

import br.com.freela.contrato.application.ContratoNaoEncontradoException;
import br.com.freela.contrato.domain.shared.RegraDeNegocioException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class ApiExceptionHandler {
    private static final Logger log = LoggerFactory.getLogger(ApiExceptionHandler.class);

    @ExceptionHandler(ContratoNaoEncontradoException.class)
    ProblemDetail naoEncontrado(ContratoNaoEncontradoException ex) {
        log.warn("http.erro.nao-encontrado contratoId={}", ex.getContratoId());
        var p = ProblemDetail.forStatusAndDetail(HttpStatus.NOT_FOUND, ex.getMessage());
        p.setTitle("Contrato não encontrado");
        p.setProperty("contratoId", ex.getContratoId());
        return p;
    }

    @ExceptionHandler(RegraDeNegocioException.class)
    ProblemDetail regraDeNegocio(RegraDeNegocioException ex) {
        log.warn("http.erro.regra-negocio motivo={}", ex.getMessage());
        var p = ProblemDetail.forStatusAndDetail(HttpStatus.UNPROCESSABLE_CONTENT, ex.getMessage());
        p.setTitle("Regra de negócio violada");
        return p;
    }

    @ExceptionHandler(OptimisticLockingFailureException.class)
    ProblemDetail conflitoDeConcorrencia(OptimisticLockingFailureException ex) {
        log.warn("http.erro.conflito-concorrencia erro={}", ex.getClass().getSimpleName());
        var p = ProblemDetail.forStatusAndDetail(HttpStatus.CONFLICT,
                "O contrato foi alterado por outra operação ao mesmo tempo. Consulte o estado atual e tente novamente.");
        p.setTitle("Conflito de concorrência");
        return p;
    }
}
