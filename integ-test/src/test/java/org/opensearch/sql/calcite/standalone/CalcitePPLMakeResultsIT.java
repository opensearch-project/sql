/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.standalone;

import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for the makeresults leading command.
 *
 * <p>The count path output ({@code @timestamp = now()}) is non-deterministic, so it asserts schema
 * + row count only. The data= path is deterministic and asserts schema + datarows.
 */
public class CalcitePPLMakeResultsIT extends CalcitePPLIntegTestCase {
  @Override
  public void init() throws IOException {
    super.init();
    enableCalcite();
  }

  @Test
  public void testCount() throws IOException {
    JSONObject result = executeQuery("makeresults count=5");
    verifySchema(result, schema("@timestamp", "timestamp"));
    assertEquals(5, result.getInt("total"));
  }

  @Test
  public void testBare() throws IOException {
    JSONObject result = executeQuery("makeresults");
    verifySchema(result, schema("@timestamp", "timestamp"));
    assertEquals(1, result.getInt("total"));
  }

  @Test
  public void testJson() throws IOException {
    String data =
        "makeresults format=json data='[{\"name\":\"John\",\"age\":35,\"score\":3.5},"
            + "{\"name\":\"Sarah\",\"age\":39,\"score\":4.0}]'";
    JSONObject result = executeQuery(data);
    verifySchema(
        result,
        schema("@timestamp", "timestamp"),
        schema("name", "string"),
        schema("age", "bigint"),
        schema("score", "float"));
    JSONObject projected = executeQuery(data + " | fields name, age, score");
    verifyDataRows(projected, rows("John", 35, 3.5), rows("Sarah", 39, 4.0));
  }

  @Test
  public void testNestedJsonSerializesToString() throws IOException {
    String data =
        "makeresults format=json data='[{\"name\":\"John\","
            + "\"addr\":{\"city\":\"NYC\",\"zip\":10001},\"tags\":[\"a\",\"b\"]}]'";
    JSONObject result = executeQuery(data);
    verifySchema(
        result,
        schema("@timestamp", "timestamp"),
        schema("name", "string"),
        schema("addr", "string"),
        schema("tags", "string"));
    JSONObject projected = executeQuery(data + " | fields name, addr, tags");
    verifyDataRows(projected, rows("John", "{\"city\":\"NYC\",\"zip\":10001}", "[\"a\",\"b\"]"));
  }

  @Test
  public void testNestedJsonSpathRoundTrip() throws IOException {
    JSONObject result =
        executeQuery(
            "makeresults format=json data='[{\"addr\":{\"city\":\"NYC\"}}]'"
                + " | spath input=addr output=city path=city | fields addr, city");
    verifyDataRows(result, rows("{\"city\":\"NYC\"}", "NYC"));
  }

  @Test
  public void testTypedCsv() throws IOException {
    JSONObject result =
        executeQuery("makeresults format=csv data='name:string,age:int\nJohn,35\nSarah,39'");
    verifySchema(result, schema("name", "string"), schema("age", "int"));
    verifyDataRows(result, rows("John", 35), rows("Sarah", 39));
  }

  @Test
  public void testBareCsv() throws IOException {
    JSONObject result = executeQuery("makeresults format=csv data='name,age\nJohn,35\nSarah,39'");
    verifySchema(result, schema("name", "string"), schema("age", "string"));
    verifyDataRows(result, rows("John", "35"), rows("Sarah", "39"));
  }

  @Test
  public void testComposesAsSource() throws IOException {
    JSONObject result = executeQuery("makeresults count=3 | eval n=1");
    verifySchema(result, schema("@timestamp", "timestamp"), schema("n", "int"));
    assertEquals(3, result.getInt("total"));
  }

  @Test
  public void testBareGlobalCountIsUnsupported() {
    assertThrows(Exception.class, () -> executeQuery("makeresults count=5 | stats count() as c"));
  }

  @Test
  public void testBareGlobalCountWorkaroundCountArg() throws IOException {
    JSONObject result = executeQuery("makeresults count=5 | stats count(1) as c");
    verifySchema(result, schema("c", "bigint"));
    verifyDataRows(result, rows(5));
  }

  @Test
  public void testBareGlobalCountWorkaroundByTimestamp() throws IOException {
    JSONObject result = executeQuery("makeresults count=5 | stats count() as c by @timestamp");
    verifyDataRows(result, rows(5, result.getJSONArray("datarows").getJSONArray(0).get(1)));
  }

  @Test
  public void testBareGlobalCountWorkaroundEvalGroup() throws IOException {
    JSONObject result = executeQuery("makeresults count=5 | eval g=1 | stats count() as c by g");
    verifyDataRows(result, rows(5, 1));
  }
}
