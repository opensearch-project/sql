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
    var result = executeQuery("explain source=test | where age = 20 | fields name, age");
    String expected =
        isPushdownEnabled()
            ? """
            {
              "calcite": {
                "physical": "CalciteEnumerableIndexScan(table=[[OpenSearch, test]], PushDownContext=[[PROJECT->[name, age], FILTER->=($1, 20)], OpenSearchRequestBuilder(sourceBuilder={\\"from\\":0,\\"timeout\\":\\"1m\\",\\"query\\":{\\"term\\":{\\"age\\":{\\"value\\":20,\\"boost\\":1.0}}},\\"_source\\":{\\"includes\\":[\\"name\\",\\"age\\"],\\"excludes\\":[]},\\"sort\\":[{\\"_doc\\":{\\"order\\":\\"asc\\"}}]}, requestedTotalSize=2147483647, pageSize=null, startFrom=0)])\\n",
                "logical": "LogicalProject(name=[$0], age=[$1])\\n  LogicalFilter(condition=[=($1, 20)])\\n    CalciteLogicalIndexScan(table=[[OpenSearch, test]])\\n"
              }
            }
            """
            : """
            {
              "calcite": {
                "logical": "LogicalProject(name=[$0], age=[$1])\\n  LogicalFilter(condition=[=($1, 20)])\\n    CalciteLogicalIndexScan(table=[[OpenSearch, test]])\\n",
                "physical": "EnumerableCalc(expr#0..7=[{inputs}], expr#8=[20], expr#9=[=($t1,\
             $t8)], proj#0..1=[{exprs}], $condition=[$t9])\\n  CalciteEnumerableIndexScan(table=[[OpenSearch, test]])\\n"
              }
            }""";

    assertJsonEquals(expected, result.toString());
  }

  @Test
  public void testExplainCommandExtended() throws IOException {
    var result = executeQuery("explain extended source=test | where age = 20 | fields name, age");
    assertTrue(
        result
            .toString()
            .contains(
                "public org.apache.calcite.linq4j.Enumerable bind(final"
                    + " org.apache.calcite.DataContext root)"));
  }

  @Test
  public void testExplainCommandCost() throws IOException {
    var result = executeQuery("explain cost source=test | where age = 20 | fields name, age");
    String expected =
        isPushdownEnabled()
            ? "\"CalciteEnumerableIndexScan(table=[[OpenSearch, test]],"
                  + " PushDownContext=[[PROJECT->[name, age], FILTER->=($1, 20)],"
                  + " OpenSearchRequestBuilder(sourceBuilder={\\\"from\\\":0,\\\"timeout\\\":\\\"1m\\\",\\\"query\\\":{\\\"term\\\":{\\\"age\\\":{\\\"value\\\":20,\\\"boost\\\":1.0}}},\\\"_source\\\":{\\\"includes\\\":[\\\"name\\\",\\\"age\\\"],\\\"excludes\\\":[]},\\\"sort\\\":[{\\\"_doc\\\":{\\\"order\\\":\\\"asc\\\"}}]},"
                  + " requestedTotalSize=2147483647, pageSize=null, startFrom=0)]): rowcount ="
                  + " 1215.0, cumulative cost = {1215.0 rows, 1216.0 cpu, 0.0 io}"
            : "CalciteEnumerableIndexScan(table=[[OpenSearch, test]]): rowcount = 10000.0,"
                + " cumulative cost = {10000.0 rows, 10001.0 cpu, 0.0 io}";
    assertTrue("Got:\n" + result.toString(), result.toString().contains(expected));
  }

  @Test
  public void testExplainCommandSimple() throws IOException {
    var result = executeQuery("explain simple source=test | where age = 20 | fields name, age");
    assertJsonEquals(
        "{\n"
            + "  \"calcite\": {\n"
            + "    \"logical\": \"LogicalProject\\n  LogicalFilter\\n    CalciteLogicalIndexScan\\n"
            + "\"\n"
            + "  }\n"
            + "}",
        result.toString());
  }
}
