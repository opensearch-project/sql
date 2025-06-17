/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.util.MatcherUtils.assertJsonEquals;

import java.io.IOException;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;
import org.opensearch.sql.ppl.PPLIntegTestCase;

public class CalcitePPLExplainIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    disallowCalciteFallback();

    Request request1 = new Request("PUT", "/test/_doc/1?refresh=true");
    request1.setJsonEntity("{\"name\": \"hello\", \"age\": 20}");
    client().performRequest(request1);
    Request request2 = new Request("PUT", "/test/_doc/2?refresh=true");
    request2.setJsonEntity("{\"name\": \"world\", \"age\": 30}");
    client().performRequest(request2);
    // PUT index test1
    Request request3 = new Request("PUT", "/test1/_doc/1?refresh=true");
    request3.setJsonEntity("{\"name\": \"HELLO\", \"alias\": \"Hello\"}");
    client().performRequest(request3);
  }

  @Test
  public void testExplainCommand() throws IOException {
    var result = explainQueryToString("explain source=test | where age = 20 | fields name, age");
    String expected =
        isPushdownEnabled()
            ? loadFromFile("expectedOutput/calcite/explain_filter_w_pushdown.json")
            : loadFromFile("expectedOutput/calcite/explain_filter_wo_pushdown.json");

    assertJsonEquals(expected, result);
  }

  @Test
  public void testExplainCommandExtended() throws IOException {
    var result =
        explainQueryToString("explain extended source=test | where age = 20 | fields name, age");
    assertTrue(
        result.contains(
            "public org.apache.calcite.linq4j.Enumerable bind(final"
                + " org.apache.calcite.DataContext root)"));
  }

  @Test
  public void testExplainCommandCost() throws IOException {
    var result =
        explainQueryToString("explain cost source=test | where age = 20 | fields name, age");
    String expected =
        isPushdownEnabled()
            ? loadFromFile("expectedOutput/calcite/explain_filter_cost_w_pushdown.txt")
            : loadFromFile("expectedOutput/calcite/explain_filter_cost_wo_pushdown.txt");
    assertTrue(
        String.format("Got: %s\n, expected: %s", result, expected), result.contains(expected));
  }

  @Test
  public void testExplainCommandSimple() throws IOException {
    var result =
        explainQueryToString("explain simple source=test | where age = 20 | fields name, age");
    String expected = loadFromFile("expectedOutput/calcite/explain_filter_simple.json");
    assertJsonEquals(expected, result);
  }
}
