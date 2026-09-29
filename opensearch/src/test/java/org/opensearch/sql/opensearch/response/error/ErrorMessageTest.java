/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.response.error;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.opensearch.core.rest.RestStatus.BAD_REQUEST;
import static org.opensearch.core.rest.RestStatus.SERVICE_UNAVAILABLE;

import java.sql.SQLException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.common.error.ErrorCode;
import org.opensearch.sql.common.error.ErrorReport;

@ExtendWith(MockitoExtension.class)
class ErrorMessageTest {

  @Test
  public void testToString() {
    ErrorMessage errorMessage =
        new ErrorMessage(
            new IllegalStateException("illegal state"), SERVICE_UNAVAILABLE.getStatus());
    assertEquals(
        "{\n"
            + "  \"error\": {\n"
            + "    \"reason\": \"There was internal problem at backend\",\n"
            + "    \"details\": \"illegal state\",\n"
            + "    \"type\": \"IllegalStateException\"\n"
            + "  },\n"
            + "  \"status\": 503\n"
            + "}",
        errorMessage.toString());
  }

  @Test
  public void testBadRequestToString() {
    ErrorMessage errorMessage =
        new ErrorMessage(new IllegalStateException(), BAD_REQUEST.getStatus());
    assertEquals(
        "{\n"
            + "  \"error\": {\n"
            + "    \"reason\": \"Invalid Query\",\n"
            + "    \"details\": \"\",\n"
            + "    \"type\": \"IllegalStateException\"\n"
            + "  },\n"
            + "  \"status\": 400\n"
            + "}",
        errorMessage.toString());
  }

  @Test
  public void testToStringWithEmptyErrorMessage() {
    ErrorMessage errorMessage =
        new ErrorMessage(new IllegalStateException(), SERVICE_UNAVAILABLE.getStatus());
    assertEquals(
        "{\n"
            + "  \"error\": {\n"
            + "    \"reason\": \"There was internal problem at backend\",\n"
            + "    \"details\": \"\",\n"
            + "    \"type\": \"IllegalStateException\"\n"
            + "  },\n"
            + "  \"status\": 503\n"
            + "}",
        errorMessage.toString());
  }

  /** The plan must not reach 'reason', which clients render, on a Calcite planning failure. */
  @Test
  public void testPlanBearingCauseIsNotPublishedAsReason() {
    ErrorReport report =
        ErrorReport.wrap(
                new SQLException(
                    "Error while preparing plan [LogicalProject(a=[CONCAT($3, 'x')])\n"
                        + "  CalciteLogicalIndexScan(table=[[OpenSearch, logs]])\n]"))
            .code(ErrorCode.PLANNING_ERROR)
            .details("Line 6, Column 9: Assignment conversion not possible")
            .build();

    String json = new ErrorMessage(report, SERVICE_UNAVAILABLE.getStatus()).toString();

    assertEquals("Failed to prepare the query plan for execution.", report.getUserFacingMessage());
    assertFalse(json.contains("LogicalProject"), "reason must not carry the plan");
    assertFalse(json.contains("CalciteLogicalIndexScan"), "reason must not carry the plan");
  }

  /**
   * The formatters publish getReason and getDetails directly, bypassing getErrorAsJson, so those
   * two fields must carry the curated values and not the raw cause message.
   */
  @Test
  public void testFormatterFieldsCarryCuratedValuesNotTheCauseMessage() {
    String nestedChain =
        "exception while executing query:"
            + " RemoteTransportException[[node][127.0.0.1:9310][indices:data/read/search]];"
            + " nested: GeneralScriptException[Failed to compile inline script [BASE64]]";
    ErrorReport report =
        ErrorReport.wrap(new SQLException(nestedChain))
            .reason("Internal error while compiling the query plan.")
            .details("Line 6, Column 9: Assignment conversion not possible")
            .build();

    ErrorMessage errorMessage = new ErrorMessage(report, SERVICE_UNAVAILABLE.getStatus());

    assertEquals("Internal error while compiling the query plan.", errorMessage.getReason());
    assertEquals("Line 6, Column 9: Assignment conversion not possible", errorMessage.getDetails());
    assertFalse(errorMessage.getDetails().contains("127.0.0.1"), "no node address");
    assertFalse(errorMessage.getDetails().contains("BASE64"), "no script blob");
  }
}
