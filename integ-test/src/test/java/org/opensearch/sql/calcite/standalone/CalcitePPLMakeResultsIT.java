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
    JSONObject result =
        executeQuery(
            "makeresults format=json data='[{\"name\":\"John\",\"age\":35,\"score\":3.5},"
                + "{\"name\":\"Sarah\",\"age\":39,\"score\":4.0}]'");
    verifySchema(
        result, schema("name", "string"), schema("age", "bigint"), schema("score", "float"));
    verifyDataRows(result, rows("John", 35, 3.5), rows("Sarah", 39, 4.0));
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
}
