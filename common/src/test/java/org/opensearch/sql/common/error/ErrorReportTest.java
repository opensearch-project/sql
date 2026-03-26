/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.common.error;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Map;
import org.junit.jupiter.api.Test;

/** Unit tests for ErrorReport. */
public class ErrorReportTest {

  @Test
  public void testBasicErrorReport() {
    Exception cause = new IllegalArgumentException("Field not found");

    ErrorReport report =
        ErrorReport.wrap(cause)
            .code(ErrorCode.FIELD_NOT_FOUND)
            .stage(QueryProcessingStage.ANALYZING)
            .location("while resolving fields in projection")
            .context("field_name", "timestamp")
            .context("table", "logs")
            .suggestion("Check that field exists")
            .build();

    assertEquals(ErrorCode.FIELD_NOT_FOUND, report.getCode());
    assertEquals(QueryProcessingStage.ANALYZING, report.getStage());
    assertEquals(1, report.getLocationChain().size());
    assertEquals("while resolving fields in projection", report.getLocationChain().get(0));
    assertEquals("timestamp", report.getContext().get("field_name"));
    assertEquals("logs", report.getContext().get("table"));
    assertEquals("Check that field exists", report.getSuggestion());
    assertEquals("Field not found", report.getDetails());
  }

  @Test
  public void testErrorReportJsonMapWithStageInContext() {
    Exception cause = new IllegalArgumentException("Field not found");

    ErrorReport report =
        ErrorReport.wrap(cause)
            .code(ErrorCode.FIELD_NOT_FOUND)
            .stage(QueryProcessingStage.ANALYZING)
            .location("while analyzing query")
            .context("field_name", "test")
            .build();

    Map<String, Object> json = report.toJsonMap();

    // Check top-level fields
    assertEquals("IllegalArgumentException", json.get("type"));
    assertEquals("FIELD_NOT_FOUND", json.get("code"));
    assertEquals("Field not found", json.get("details"));

    // Check location
    assertTrue(json.containsKey("location"));

    // Check that stage is in context
    assertTrue(json.containsKey("context"));
    @SuppressWarnings("unchecked")
    Map<String, Object> context = (Map<String, Object>) json.get("context");
    assertEquals("analyzing", context.get("stage"));
    assertEquals("Validating the query", context.get("stage_description"));
    assertEquals("test", context.get("field_name"));
  }

  @Test
  public void testIdempotentWrapping() {
    Exception originalCause = new IllegalArgumentException("Original error");

    ErrorReport firstWrap =
        ErrorReport.wrap(originalCause)
            .code(ErrorCode.FIELD_NOT_FOUND)
            .stage(QueryProcessingStage.ANALYZING)
            .context("field_name", "test")
            .build();

    // Wrap again with additional context
    ErrorReport secondWrap =
        ErrorReport.wrap(firstWrap)
            .stage(QueryProcessingStage.PLAN_CONVERSION)
            .location("during plan conversion")
            .context("additional_context", "value")
            .build();

    // Original cause should still be the IllegalArgumentException
    assertEquals("Original error", secondWrap.getDetails());

    // Should have accumulated context
    Map<String, Object> context = secondWrap.getContext();
    assertEquals("test", context.get("field_name"));
    assertEquals("value", context.get("additional_context"));

    // Should have location from second wrap
    assertTrue(secondWrap.getLocationChain().contains("during plan conversion"));
  }

  @Test
  public void testStageErrorHandler() {
    // Test successful execution
    String result =
        StageErrorHandler.executeStage(
            QueryProcessingStage.ANALYZING, () -> "success", "test operation");

    assertEquals("success", result);

    // Test error wrapping
    Exception thrown =
        assertThrows(
            ErrorReport.class,
            () ->
                StageErrorHandler.executeStage(
                    QueryProcessingStage.ANALYZING,
                    () -> {
                      throw new IllegalArgumentException("Test error");
                    },
                    "while testing"));

    ErrorReport report = (ErrorReport) thrown;
    assertEquals(QueryProcessingStage.ANALYZING, report.getStage());
    assertTrue(report.getLocationChain().contains("while testing"));
  }

  @Test
  public void testToDetailedMessage() {
    Exception cause = new IllegalArgumentException("Field not found");

    ErrorReport report =
        ErrorReport.wrap(cause)
            .code(ErrorCode.FIELD_NOT_FOUND)
            .stage(QueryProcessingStage.ANALYZING)
            .location("while resolving fields")
            .context("field_name", "test")
            .suggestion("Check field name")
            .build();

    String message = report.toDetailedMessage();

    assertTrue(message.contains("FIELD_NOT_FOUND"));
    assertTrue(message.contains("Validating the query"));
    assertTrue(message.contains("Field not found"));
    assertTrue(message.contains("while resolving fields"));
    assertTrue(message.contains("field_name"));
    assertTrue(message.contains("Check field name"));
  }
}
