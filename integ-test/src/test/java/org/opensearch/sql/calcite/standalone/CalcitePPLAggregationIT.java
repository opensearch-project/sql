/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.standalone;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;

import java.io.IOException;
import org.junit.Ignore;
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
  public void testMultipleAggregatesWithAliasesByClause() {
    String actual =
        execute(
            String.format(
                "source=%s | stats avg(balance) as avg, max(balance) as max, min(balance) as min,"
                    + " count() as cnt by gender",
                TEST_INDEX_BANK));
    assertEquals(
        "{\n"
            + "  \"schema\": [\n"
            + "    {\n"
            + "      \"name\": \"gender\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
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
            + "      \"name\": \"cnt\",\n"
            + "      \"type\": \"long\"\n"
            + "    }\n"
            + "  ],\n"
            + "  \"datarows\": [\n"
            + "    [\n"
            + "      \"F\",\n"
            + "      40488.0,\n"
            + "      48086,\n"
            + "      32838,\n"
            + "      3\n"
            + "    ],\n"
            + "    [\n"
            + "      \"M\",\n"
            + "      16377.25,\n"
            + "      39225,\n"
            + "      4180,\n"
            + "      4\n"
            + "    ]\n"
            + "  ],\n"
            + "  \"total\": 2,\n"
            + "  \"size\": 2\n"
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

  /**
   * TODO Calcite doesn't support group by window, but it support Tumble table function. See
   * `SqlToRelConverterTest`
   */
  @Ignore
  public void testAvgByTimeSpanAndFields() {
    String actual =
        execute(
            String.format(
                "source=%s | stats avg(balance) by span(birthdate, 1 day) as age_balance",
                TEST_INDEX_BANK));
    assertEquals("", actual);
  }

  @Test
  public void testCountDistinct() {
    String actual =
        execute(
            String.format("source=%s | stats distinct_count(state) by gender", TEST_INDEX_BANK));
    assertEquals(
        "{\n"
            + "  \"schema\": [\n"
            + "    {\n"
            + "      \"name\": \"gender\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"distinct_count(state)\",\n"
            + "      \"type\": \"long\"\n"
            + "    }\n"
            + "  ],\n"
            + "  \"datarows\": [\n"
            + "    [\n"
            + "      \"F\",\n"
            + "      3\n"
            + "    ],\n"
            + "    [\n"
            + "      \"M\",\n"
            + "      4\n"
            + "    ]\n"
            + "  ],\n"
            + "  \"total\": 2,\n"
            + "  \"size\": 2\n"
            + "}",
        actual);
  }

  @Test
  public void testCountDistinctWithAlias() {
    String actual =
        execute(
            String.format(
                "source=%s | stats distinct_count(state) as dc by gender", TEST_INDEX_BANK));
    assertEquals(
        "{\n"
            + "  \"schema\": [\n"
            + "    {\n"
            + "      \"name\": \"gender\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"dc\",\n"
            + "      \"type\": \"long\"\n"
            + "    }\n"
            + "  ],\n"
            + "  \"datarows\": [\n"
            + "    [\n"
            + "      \"F\",\n"
            + "      3\n"
            + "    ],\n"
            + "    [\n"
            + "      \"M\",\n"
            + "      4\n"
            + "    ]\n"
            + "  ],\n"
            + "  \"total\": 2,\n"
            + "  \"size\": 2\n"
            + "}",
        actual);
  }

  @Ignore
  public void testApproxCountDistinct() {
    String actual =
        execute(
            String.format(
                "source=%s | stats distinct_count_approx(state) by gender", TEST_INDEX_BANK));
  }

  @Test
  public void testStddevSampStddevPop() {
    String actual =
        execute(
            String.format(
                "source=%s | stats stddev_samp(balance) as ss, stddev_pop(balance) as sp by gender",
                TEST_INDEX_BANK));
    assertEquals(
        "{\n"
            + "  \"schema\": [\n"
            + "    {\n"
            + "      \"name\": \"gender\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"ss\",\n"
            + "      \"type\": \"double\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"sp\",\n"
            + "      \"type\": \"double\"\n"
            + "    }\n"
            + "  ],\n"
            + "  \"datarows\": [\n"
            + "    [\n"
            + "      \"F\",\n"
            + "      7624.132999889233,\n"
            + "      6225.078526947806\n"
            + "    ],\n"
            + "    [\n"
            + "      \"M\",\n"
            + "      16177.114233282358,\n"
            + "      14009.791885945344\n"
            + "    ]\n"
            + "  ],\n"
            + "  \"total\": 2,\n"
            + "  \"size\": 2\n"
            + "}",
        actual);
  }

  @Test
  public void testAggWithEval() {
    String actual =
        execute(
            String.format(
                "source=%s | eval a = 1, b = a | stats avg(a) as avg_a by b", TEST_INDEX_BANK));
    assertEquals(
        "{\n"
            + "  \"schema\": [\n"
            + "    {\n"
            + "      \"name\": \"b\",\n"
            + "      \"type\": \"integer\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"avg_a\",\n"
            + "      \"type\": \"double\"\n"
            + "    }\n"
            + "  ],\n"
            + "  \"datarows\": [\n"
            + "    [\n"
            + "      1,\n"
            + "      1.0\n"
            + "    ]\n"
            + "  ],\n"
            + "  \"total\": 1,\n"
            + "  \"size\": 1\n"
            + "}",
        actual);
  }

  @Test
  public void testAggWithBackticksAlias() {
    String actual =
        execute(String.format("source=%s | stats sum(`balance`) as `sum_b`", TEST_INDEX_BANK));
    assertEquals(
        "{\n"
            + "  \"schema\": [\n"
            + "    {\n"
            + "      \"name\": \"sum_b\",\n"
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
  public void testSimpleTwoLevelStats() {
    String actual =
        execute(
            String.format(
                "source=%s | stats avg(balance) as avg_by_gender by gender | stats"
                    + " avg(avg_by_gender) as avg_avg",
                TEST_INDEX_BANK));
    assertEquals(
        "{\n"
            + "  \"schema\": [\n"
            + "    {\n"
            + "      \"name\": \"avg_avg\",\n"
            + "      \"type\": \"double\"\n"
            + "    }\n"
            + "  ],\n"
            + "  \"datarows\": [\n"
            + "    [\n"
            + "      28432.625\n"
            + "    ]\n"
            + "  ],\n"
            + "  \"total\": 1,\n"
            + "  \"size\": 1\n"
            + "}",
        actual);
  }
}
