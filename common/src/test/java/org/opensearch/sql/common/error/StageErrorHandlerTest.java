/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.common.error;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.sql.SQLException;
import org.junit.jupiter.api.Test;

/** Unit tests for the codegen sanitiser in StageErrorHandler. */
public class StageErrorHandlerTest {

  private static final String COMPILER_DIAGNOSTIC =
      "Line 6, Column 9: Assignment conversion not possible from type \"java.util.Map\" to type"
          + " \"java.lang.String\"";

  /** Same simple name as the janino class, which is what detection matches on. */
  private static class CompileException extends RuntimeException {
    CompileException(String message) {
      super(message);
    }
  }

  private ErrorReport throughStage(QueryProcessingStage stage, ErrorReport report) {
    return assertThrows(
        ErrorReport.class,
        () ->
            StageErrorHandler.executeStage(
                stage,
                () -> {
                  throw report;
                },
                "while running the query"));
  }

  private ErrorReport codegenChain(Throwable innermost) {
    return ErrorReport.wrap(new SQLException("exception while executing query", null, innermost))
        .code(ErrorCode.PLANNING_ERROR)
        .build();
  }

  private ErrorReport fieldNotFound(String field) {
    return ErrorReport.wrap(
            new IllegalArgumentException(String.format("Field [%s] not found.", field)))
        .code(ErrorCode.FIELD_NOT_FOUND)
        .build();
  }

  @Test
  public void testCompileExceptionClassYieldsTheCompilerDiagnostic() {
    ErrorReport report =
        throughStage(
            QueryProcessingStage.EXECUTING,
            codegenChain(
                new RuntimeException(
                    "Error while compiling generated Java code:\npublic class Baz {}",
                    new CompileException(COMPILER_DIAGNOSTIC))));

    assertEquals("Internal error while compiling the query plan.", report.getReason());
    assertEquals(COMPILER_DIAGNOSTIC, report.getDetails());
  }

  @Test
  public void testSerializedWrapperStillYieldsTheCompilerDiagnostic() {
    // A transport hop replaces the concrete class, leaving the diagnostic behind a type prefix.
    ErrorReport report =
        throughStage(
            QueryProcessingStage.EXECUTING,
            codegenChain(new RuntimeException("compile_exception: " + COMPILER_DIAGNOSTIC)));

    assertEquals("Internal error while compiling the query plan.", report.getReason());
    assertEquals(COMPILER_DIAGNOSTIC, report.getDetails());
  }

  @Test
  public void testMarkerWhoseMessageEmbedsSourceFallsBackToGenericDetails() {
    ErrorReport report =
        throughStage(
            QueryProcessingStage.EXECUTING,
            codegenChain(
                new RuntimeException(
                    "Error while compiling generated Java code:\npublic class Baz {}")));

    assertEquals("Internal error while compiling the query plan.", report.getReason());
    // The wrapper's own message carries the generated source, so it must not be surfaced.
    org.junit.jupiter.api.Assertions.assertFalse(report.getDetails().contains("public class Baz"));
  }

  @Test
  public void testFieldNamedLikeAMarkerIsUntouchedAtAnalysisStages() {
    ErrorReport report =
        throughStage(QueryProcessingStage.ANALYZING, fieldNotFound("compile_exception"));

    assertNull(report.getReason());
    assertEquals("Field [compile_exception] not found.", report.getDetails());
  }

  @Test
  public void testFieldNamedLikeAMarkerIsUntouchedAtExecuting() {
    // The marker carries its separator, so a bare identifier cannot match even on the gated stage.
    ErrorReport report =
        throughStage(QueryProcessingStage.EXECUTING, fieldNotFound("compile_exception"));

    assertNull(report.getReason());
    assertEquals("Field [compile_exception] not found.", report.getDetails());
  }

  @Test
  public void testCodegenFailureIsIgnoredOutsideExecuting() {
    ErrorReport report =
        throughStage(
            QueryProcessingStage.ANALYZING,
            codegenChain(new CompileException(COMPILER_DIAGNOSTIC)));

    assertNull(report.getReason());
  }

  @Test
  public void testExistingReasonIsNeverOverwritten() {
    ErrorReport inner =
        ErrorReport.wrap(
                new SQLException("exception", null, new CompileException(COMPILER_DIAGNOSTIC)))
            .reason("a more specific message from an inner layer")
            .details("its own details")
            .build();

    ErrorReport report = throughStage(QueryProcessingStage.EXECUTING, inner);

    assertEquals("a more specific message from an inner layer", report.getReason());
    assertEquals("its own details", report.getDetails());
  }

  @Test
  public void testCyclicCauseChainTerminates() {
    Exception a = new Exception("a");
    Exception b = new Exception("b", a);
    a.initCause(b);

    ErrorReport report = throughStage(QueryProcessingStage.EXECUTING, ErrorReport.wrap(a).build());

    assertNull(report.getReason());
  }
}
