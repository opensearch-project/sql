/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.util.MatcherUtils.assertJsonEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;
import org.opensearch.sql.ast.statement.ExplainMode;
import org.opensearch.sql.ppl.PPLIntegTestCase;
import org.opensearch.sql.ppl.PPLIntegTestCase.GlobalPushdownConfig;
import org.opensearch.sql.protocol.response.format.Format;

public class CalcitePPLExplainIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();

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
    var result = explainQueryToString("source=test | where age = 20 | fields name, age");
    String expected =
        !isPushdownDisabled()
            ? loadFromFile("expectedOutput/calcite/explain_filter_w_pushdown.json")
            : loadFromFile("expectedOutput/calcite/explain_filter_wo_pushdown.json");

    assertJsonEquals(expected, result);
  }

  @Test
  public void testExplainCommandExtendedWithCodegen() throws IOException {
    var result =
        executeWithReplace(
            "explain extended source=test | where age = 20 | join left=l right=r on l.age=r.age"
                + " test");
    assertTrue(
        result.contains(
            "public org.apache.calcite.linq4j.Enumerable bind(final"
                + " org.apache.calcite.DataContext root)"));
  }

  @Test
  public void testExplainCommandCost() throws IOException {
    var result = executeWithReplace("explain cost source=test | where age = 20 | fields name, age");
    String expected =
        !isPushdownDisabled()
            ? loadFromFile("expectedOutput/calcite/explain_filter_cost_w_pushdown.txt")
            : loadFromFile("expectedOutput/calcite/explain_filter_cost_wo_pushdown.txt");
    assertTrue(
        String.format("Got: %s\n, expected: %s", result, expected),
        result.contains(expected.trim()));
  }

  @Test
  public void testExplainCommandSimple() throws IOException {
    var result =
        executeWithReplace("explain simple source=test | where age = 20 | fields name, age");
    String expected = loadFromFile("expectedOutput/calcite/explain_filter_simple.json");
    assertJsonEquals(expected, result);
  }

  @Test
  public void testJsonTreeFormat() throws IOException {
    var resultStr =
        explainQuery(
            "source=test | where age > 20 | fields name", Format.JSON_TREE, ExplainMode.STANDARD);

    // Parse JSON
    var mapper = new ObjectMapper();
    var result = mapper.readTree(resultStr);

    // Verify tree structure exists
    assertTrue(result.has("calcite"));
    assertTrue(result.get("calcite").has("logical"));
    assertTrue(result.get("calcite").has("physical"));

    // Verify logical and physical are parsed JSON objects, not strings
    assertTrue(result.get("calcite").get("logical").isObject());
    assertTrue(result.get("calcite").get("physical").isObject());

    // Verify sourceBuilder exists in physical plan rels
    var physical = result.get("calcite").get("physical");
    assertTrue(physical.has("rels"));
    var rels = physical.get("rels");
    assertTrue(rels.isArray());

    // Find a rel with sourceBuilder (only present when pushdown is enabled)
    boolean foundSourceBuilder = false;
    for (int i = 0; i < rels.size(); i++) {
      var rel = rels.get(i);
      if (rel.has("sourceBuilder")) {
        foundSourceBuilder = true;
        // Verify sourceBuilder is a parsed JSON object, not a string
        assertTrue(rel.get("sourceBuilder").isObject());
        // Verify it has expected OpenSearch DSL fields
        assertTrue(rel.get("sourceBuilder").has("from"));
        assertTrue(rel.get("sourceBuilder").has("size"));
        break;
      }
    }
    // Only assert sourceBuilder exists when pushdown is enabled
    if (GlobalPushdownConfig.enabled) {
      assertTrue("sourceBuilder not found in physical plan rels", foundSourceBuilder);
    }
  }

  /**
   * Executes the PPL query and returns the result as a string with windows-style line breaks
   * replaced with Unix-style ones.
   *
   * @param ppl the PPL query to execute
   * @return the result of the query as a string with line breaks replaced
   * @throws IOException if an error occurs during query execution
   */
  private String executeWithReplace(String ppl) throws IOException {
    var result = executeQueryToString(ppl);
    return result.replace("\\r\\n", "\\n");
  }
}
