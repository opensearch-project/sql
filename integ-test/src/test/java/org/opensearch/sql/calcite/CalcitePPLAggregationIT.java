/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;

import java.io.IOException;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;

public class CalcitePPLAggregationIT extends CalcitePPLIntegTestCase {

  @Override
  public void init() throws IOException {
    super.init();

    loadIndex(Index.BANK);
  }

  @Test
  public void testSimpleCount0() throws IOException {
    Request request1 = new Request("PUT", "/test/_doc/1?refresh=true");
    request1.setJsonEntity("{\"name\": \"hello\", \"age\": 20}");
    client().performRequest(request1);
    Request request2 = new Request("PUT", "/test/_doc/2?refresh=true");
    request2.setJsonEntity("{\"name\": \"world\", \"age\": 30}");
    client().performRequest(request2);

    String actual = execute("source=test | stats count() as c");
    assertEquals(
        "{\n"
            + "  \"schema\": [\n"
            + "    {\n"
            + "      \"name\": \"c\",\n"
            + "      \"type\": \"long\"\n"
            + "    }\n"
            + "  ],\n"
            + "  \"datarows\": [\n"
            + "    [\n"
            + "      2\n"
            + "    ]\n"
            + "  ],\n"
            + "  \"total\": 1,\n"
            + "  \"size\": 1\n"
            + "}",
        actual);
  }

  @Test
  public void testSimpleCount() {
    String actual = execute(String.format("source=%s | stats count() as c", TEST_INDEX_BANK));
    assertEquals(
        "{\n"
            + "  \"schema\": [\n"
            + "    {\n"
            + "      \"name\": \"c\",\n"
            + "      \"type\": \"long\"\n"
            + "    }\n"
            + "  ],\n"
            + "  \"datarows\": [\n"
            + "    [\n"
            + "      7\n"
            + "    ]\n"
            + "  ],\n"
            + "  \"total\": 1,\n"
            + "  \"size\": 1\n"
            + "}",
        actual);
  }

  @Test
  public void testSimpleAvg() {
    String actual = execute(String.format("source=%s | stats avg(balance)", TEST_INDEX_BANK));
    assertEquals(
        "{\n"
            + "  \"schema\": [\n"
            + "    {\n"
            + "      \"name\": \"avg(balance)\",\n"
            + "      \"type\": \"double\"\n"
            + "    }\n"
            + "  ],\n"
            + "  \"datarows\": [\n"
            + "    [\n"
            + "      26710.428571428572\n"
            + "    ]\n"
            + "  ],\n"
            + "  \"total\": 1,\n"
            + "  \"size\": 1\n"
            + "}",
        actual);
  }

  @Test
  public void testSumAvg() {
    String actual = execute(String.format("source=%s | stats sum(balance)", TEST_INDEX_BANK));
    assertEquals(
        "{\n"
            + "  \"schema\": [\n"
            + "    {\n"
            + "      \"name\": \"sum(balance)\",\n"
            + "      \"type\": \"long\"\n"
            + "    }\n"
            + "  ],\n"
            + "  \"datarows\": [\n"
            + "    [\n"
            + "      186973\n"
            + "    ]\n"
            + "  ],\n"
            + "  \"total\": 1,\n"
            + "  \"size\": 1\n"
            + "}",
        actual);
  }

  @Test
  public void testMultipleAggregatesWithAliases() {
    String actual =
        execute(
            String.format(
                "source=%s | stats avg(balance) as avg, max(balance) as max, min(balance) as min,"
                    + " count()",
                TEST_INDEX_BANK));
    assertEquals(
        "{\n"
            + "  \"schema\": [\n"
            + "    {\n"
            + "      \"name\": \"avg\",\n"
            + "      \"type\": \"double\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"max\",\n"
            + "      \"type\": \"long\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"min\",\n"
            + "      \"type\": \"long\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"count()\",\n"
            + "      \"type\": \"long\"\n"
            + "    }\n"
            + "  ],\n"
            + "  \"datarows\": [\n"
            + "    [\n"
            + "      26710.428571428572,\n"
            + "      48086,\n"
            + "      4180,\n"
            + "      7\n"
            + "    ]\n"
            + "  ],\n"
            + "  \"total\": 1,\n"
            + "  \"size\": 1\n"
            + "}",
        actual);
  }

  @Test
  public void testAvgByField() {
    String actual =
        execute(String.format("source=%s | stats avg(balance) by gender", TEST_INDEX_BANK));
    assertEquals(
        "{\n"
            + "  \"schema\": [\n"
            + "    {\n"
            + "      \"name\": \"gender\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"avg(balance)\",\n"
            + "      \"type\": \"double\"\n"
            + "    }\n"
            + "  ],\n"
            + "  \"datarows\": [\n"
            + "    [\n"
            + "      \"F\",\n"
            + "      40488.0\n"
            + "    ],\n"
            + "    [\n"
            + "      \"M\",\n"
            + "      16377.25\n"
            + "    ]\n"
            + "  ],\n"
            + "  \"total\": 2,\n"
            + "  \"size\": 2\n"
            + "}",
        actual);
  }

  @org.junit.Test
  public void testAvgBySpan() {
    String actual =
        execute(String.format("source=%s | stats avg(balance) by span(age, 10)", TEST_INDEX_BANK));
    assertEquals(
        "{\n"
            + "  \"schema\": [\n"
            + "    {\n"
            + "      \"name\": \"span(age,10)\",\n"
            + "      \"type\": \"double\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"avg(balance)\",\n"
            + "      \"type\": \"double\"\n"
            + "    }\n"
            + "  ],\n"
            + "  \"datarows\": [\n"
            + "    [\n"
            + "      20.0,\n"
            + "      32838.0\n"
            + "    ],\n"
            + "    [\n"
            + "      30.0,\n"
            + "      25689.166666666668\n"
            + "    ]\n"
            + "  ],\n"
            + "  \"total\": 2,\n"
            + "  \"size\": 2\n"
            + "}",
        actual);
  }

  @Test
  public void testAvgBySpanAndFields() {
    String actual =
        execute(
            String.format(
                "source=%s | stats avg(balance) by span(age, 10) as age_span, gender",
                TEST_INDEX_BANK));
    assertEquals(
        "{\n"
            + "  \"schema\": [\n"
            + "    {\n"
            + "      \"name\": \"gender\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"age_span\",\n"
            + "      \"type\": \"double\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"avg(balance)\",\n"
            + "      \"type\": \"double\"\n"
            + "    }\n"
            + "  ],\n"
            + "  \"datarows\": [\n"
            + "    [\n"
            + "      \"F\",\n"
            + "      30.0,\n"
            + "      44313.0\n"
            + "    ],\n"
            + "    [\n"
            + "      \"M\",\n"
            + "      30.0,\n"
            + "      16377.25\n"
            + "    ],\n"
            + "    [\n"
            + "      \"F\",\n"
            + "      20.0,\n"
            + "      32838.0\n"
            + "    ]\n"
            + "  ],\n"
            + "  \"total\": 3,\n"
            + "  \"size\": 3\n"
            + "}",
        actual);
  }

  // TODO fallback to V2 because missing conversion LogicalAggregate[convention: NONE -> ENUMERABLE]
  @Test
  public void testCountDistinct() {
    String actual =
        execute(
            String.format("source=%s | stats distinct_count(state) by gender", TEST_INDEX_BANK));
    assertEquals(
        "{\n"
            + "  \"schema\": [\n"
            + "    {\n"
            + "      \"name\": \"distinct_count(state)\",\n"
            + "      \"type\": \"integer\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"gender\",\n"
            + "      \"type\": \"string\"\n"
            + "    }\n"
            + "  ],\n"
            + "  \"datarows\": [\n"
            + "    [\n"
            + "      3,\n"
            + "      \"f\"\n"
            + "    ],\n"
            + "    [\n"
            + "      4,\n"
            + "      \"m\"\n"
            + "    ]\n"
            + "  ],\n"
            + "  \"total\": 2,\n"
            + "  \"size\": 2\n"
            + "}",
        actual);
  }
}
