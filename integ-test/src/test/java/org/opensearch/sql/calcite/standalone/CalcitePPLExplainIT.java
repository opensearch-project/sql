/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.standalone;

import java.io.IOException;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;

public class CalcitePPLExplainIT extends CalcitePPLIntegTestCase {

  @Override
  public void init() throws IOException {
    super.init();
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
  public void testExplainCommand() {
    String result = explainQuery("explain source=test | where age = 20 | fields name, age");
    assertEquals(
        "{\n"
            + "  \"calcite\": {\n"
            + "    \"logical\": \"LogicalProject(name=[$0], age=[$1])\\n"
            + "  LogicalFilter(condition=[=($1, 20)])\\n"
            + "    CalciteLogicalIndexScan(table=[[OpenSearch, test]])\\n"
            + "\",\n"
            + "    \"physical\": \"EnumerableCalc(expr#0..7=[{inputs}], expr#8=[20], expr#9=[=($t1,"
            + " $t8)], proj#0..1=[{exprs}], $condition=[$t9])\\n"
            + "  CalciteEnumerableIndexScan(table=[[OpenSearch, test]])\\n"
            + "\"\n"
            + "  }\n"
            + "}",
        result);
  }

  @Test
  public void testExplainCommandExtended() {
    String result =
        explainQuery("explain extended source=test | where age = 20 | fields name, age");
    assertTrue(
        result.contains(
            "public org.apache.calcite.linq4j.Enumerable bind(final org.apache.calcite.DataContext"
                + " root)"));
  }

  @Test
  public void testExplainCommandCost() {
    String result = explainQuery("explain cost source=test | where age = 20 | fields name, age");
    assertTrue(
        result.contains(
            "CalciteEnumerableIndexScan(table=[[OpenSearch, test]]): rowcount = 100.0, cumulative"
                + " cost = {100.0 rows, 101.0 cpu, 0.0 io}"));
  }

  @Test
  public void testExplainCommandSimple() {
    String result = explainQuery("explain simple source=test | where age = 20 | fields name, age");
    assertEquals(
        "{\n"
            + "  \"calcite\": {\n"
            + "    \"logical\": \"LogicalProject\\n  LogicalFilter\\n    CalciteLogicalIndexScan\\n"
            + "\"\n"
            + "  }\n"
            + "}",
        result);
  }

  @Test
  public void testExplainModeUnsupportedInV2() throws IOException {
    withCalciteDisabled(
        () -> {
          try {
            explainQuery("explain cost source=test | where age = 20 | fields name, age");
          } catch (Exception e) {
            assertTrue(e.getMessage().contains("explain mode COST is not supported in v2 engine"));
          }
        }
    );
  }
}
