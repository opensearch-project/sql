/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.unittest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.opensearch.sql.legacy.request.SqlRequestParam.QUERY_PARAMS_FORMAT;
import static org.opensearch.sql.legacy.request.SqlRequestParam.QUERY_PARAMS_PRETTY;

import com.google.common.collect.ImmutableMap;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.opensearch.sql.legacy.executor.Format;
import org.opensearch.sql.legacy.request.SqlRequestParam;

public class SqlRequestParamTest {
  @Rule public final ExpectedException exceptionRule = ExpectedException.none();

  @Test
  public void shouldReturnTrueIfPrettyParamsIsTrue() {
    assertTrue(SqlRequestParam.isPrettyFormat(ImmutableMap.of(QUERY_PARAMS_PRETTY, "true")));
  }

  @Test
  public void shouldReturnTrueIfPrettyParamsIsEmpty() {
    assertTrue(SqlRequestParam.isPrettyFormat(ImmutableMap.of(QUERY_PARAMS_PRETTY, "")));
  }

  @Test
  public void shouldReturnFalseIfNoPrettyParams() {
    assertFalse(SqlRequestParam.isPrettyFormat(ImmutableMap.of()));
  }

  @Test
  public void shouldReturnFalseIfPrettyParamsIsUnknownValue() {
    assertFalse(SqlRequestParam.isPrettyFormat(ImmutableMap.of(QUERY_PARAMS_PRETTY, "unknown")));
  }

  @Test
  public void shouldReturnJSONIfFormatParamsIsJSON() {
    assertEquals(
        Format.JSON, SqlRequestParam.getFormat(ImmutableMap.of(QUERY_PARAMS_FORMAT, "json")));
  }

  @Test
  public void shouldReturnDefaultFormatIfNoFormatParams() {
    assertEquals(Format.JDBC, SqlRequestParam.getFormat(ImmutableMap.of()));
  }

  @Test
  public void shouldThrowExceptionIfFormatParamsIsEmpty() {
    exceptionRule.expect(IllegalArgumentException.class);
    exceptionRule.expectMessage("Failed to create executor due to unknown response format: ");

    assertEquals(Format.JDBC, SqlRequestParam.getFormat(ImmutableMap.of(QUERY_PARAMS_FORMAT, "")));
  }

  @Test
  public void shouldThrowExceptionIfFormatParamsIsNotSupported() {
    exceptionRule.expect(IllegalArgumentException.class);
    exceptionRule.expectMessage("Failed to create executor due to unknown response format: xml");

    SqlRequestParam.getFormat(ImmutableMap.of(QUERY_PARAMS_FORMAT, "xml"));
  }
}
