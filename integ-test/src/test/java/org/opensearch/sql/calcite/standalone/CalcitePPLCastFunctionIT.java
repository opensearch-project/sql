/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.standalone;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_STATE_COUNTRY;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_STATE_COUNTRY_WITH_NULL;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.sql.common.antlr.SyntaxCheckException;

public class CalcitePPLCastFunctionIT extends CalcitePPLIntegTestCase {
  @Override
  public void init() throws IOException {
    super.init();
    loadIndex(Index.STATE_COUNTRY);
    loadIndex(Index.STATE_COUNTRY_WITH_NULL);
  }

  @Test
  public void testCast() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval a = cast(age as string) | fields a", TEST_INDEX_STATE_COUNTRY));

    verifySchema(actual, schema("a", "string"));

    verifyDataRows(actual, rows("70"), rows("30"), rows("25"), rows("20"));
  }

  @Test
  public void testCastOverriding() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval age = cast(age as STRING) | fields age",
                TEST_INDEX_STATE_COUNTRY));

    verifySchema(actual, schema("age", "string"));

    verifyDataRows(actual, rows("70"), rows("30"), rows("25"), rows("20"));
  }

  @Test
  public void testCastToUnknownType() {
    assertThrows(
        SyntaxCheckException.class,
        () ->
            executeQuery(
                String.format(
                    "source=%s | eval age = cast(age as VARCHAR) | fields age",
                    TEST_INDEX_STATE_COUNTRY)));
  }

  @Test
  public void testChainedCast() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval age = cast(concat(cast(age as string), '0') as DOUBLE) | fields"
                    + " age",
                TEST_INDEX_STATE_COUNTRY));

    verifySchema(actual, schema("age", "double"));

    verifyDataRows(actual, rows(700.0), rows(300.0), rows(250.0), rows(200.0));
  }

  @Test
  public void testCastNullValues() {
    String actual =
        execute(
            String.format(
                "source=%s | eval a = cast(state as string) | fields a",
                TEST_INDEX_STATE_COUNTRY_WITH_NULL));

    assertEquals(
        "{\n"
            + "  \"schema\": [\n"
            + "    {\n"
            + "      \"name\": \"a\",\n"
            + "      \"type\": \"string\"\n"
            + "    }\n"
            + "  ],\n"
            + "  \"datarows\": [\n"
            + "    [\n"
            + "      \"California\"\n"
            + "    ],\n"
            + "    [\n"
            + "      \"New York\"\n"
            + "    ],\n"
            + "    [\n"
            + "      \"Ontario\"\n"
            + "    ],\n"
            + "    [\n"
            + "      \"Quebec\"\n"
            + "    ],\n"
            + "    [\n"
            + "      null\n"
            + "    ]\n"
            + "  ],\n"
            + "  \"total\": 5,\n"
            + "  \"size\": 5\n"
            + "}",
        actual);
  }

  @Test
  public void testCastToUnsupportedType() {
    String actual =
        execute(
            String.format(
                "source=%s | eval a = cast(name as boolean) | fields a", TEST_INDEX_STATE_COUNTRY));

    assertEquals(
        "{\n"
            + "  \"schema\": [\n"
            + "    {\n"
            + "      \"name\": \"a\",\n"
            + "      \"type\": \"boolean\"\n"
            + "    }\n"
            + "  ],\n"
            + "  \"datarows\": [\n"
            + "    [\n"
            + "      null\n"
            + "    ],\n"
            + "    [\n"
            + "      null\n"
            + "    ],\n"
            + "    [\n"
            + "      null\n"
            + "    ],\n"
            + "    [\n"
            + "      null\n"
            + "    ]\n"
            + "  ],\n"
            + "  \"total\": 4,\n"
            + "  \"size\": 4\n"
            + "}",
        actual);
  }
}
