/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.standalone;

import org.junit.jupiter.api.Test;
import org.opensearch.sql.common.setting.Settings;

public class CalcitePPLExplainPushdownIT extends CalcitePPLExplainIT {
  @Override
  protected Settings getSettings() {
    return enablePushdown();
  }

  @Override
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
            + "    \"physical\": \"CalciteEnumerableIndexScan(table=[[OpenSearch, test]],"
            + " PushDownContext=[[PROJECT->[name, age], FILTER->=($1, 20)],"
            + " OpenSearchRequestBuilder(sourceBuilder={\\\"from\\\":0,\\\"timeout\\\":\\\"1m\\\",\\\"query\\\":{\\\"term\\\":{\\\"age\\\":{\\\"value\\\":20,\\\"boost\\\":1.0}}},\\\"_source\\\":{\\\"includes\\\":[\\\"name\\\",\\\"age\\\"],\\\"excludes\\\":[]},\\\"sort\\\":[{\\\"_doc\\\":{\\\"order\\\":\\\"asc\\\"}}]},"
            + " requestedTotalSize=200, pageSize=null, startFrom=0)])\\n"
            + "\"\n"
            + "  }\n"
            + "}",
        result);
  }

  @Override
  @Test
  public void testExplainCommandCost() {
    String result = explainQuery("explain cost source=test | where age = 20 | fields name, age");
    String expected =
        "{\n"
            + "  \"calcite\": {\n"
            + "    \"logical\": \"LogicalProject(name=[$0], age=[$1]): rowcount = 30.0, cumulative"
            + " cost = {260.0 rows, 461.0 cpu, 0.0 io}, id = *\\n"
            + "  LogicalFilter(condition=[=($1, 20)]): rowcount = 30.0, cumulative cost = {230.0"
            + " rows, 401.0 cpu, 0.0 io}, id = *\\n"
            + "    CalciteLogicalIndexScan(table=[[OpenSearch, test]]): rowcount = 200.0,"
            + " cumulative cost = {200.0 rows, 201.0 cpu, 0.0 io}, id = *\\n"
            + "\",\n"
            + "    \"physical\": \"CalciteEnumerableIndexScan(table=[[OpenSearch, test]],"
            + " PushDownContext=[[PROJECT->[name, age], FILTER->=($1, 20)],"
            + " OpenSearchRequestBuilder(sourceBuilder={\\\"from\\\":0,\\\"timeout\\\":\\\"1m\\\",\\\"query\\\":{\\\"term\\\":{\\\"age\\\":{\\\"value\\\":20,\\\"boost\\\":1.0}}},\\\"_source\\\":{\\\"includes\\\":[\\\"name\\\",\\\"age\\\"],\\\"excludes\\\":[]},\\\"sort\\\":[{\\\"_doc\\\":{\\\"order\\\":\\\"asc\\\"}}]},"
            + " requestedTotalSize=200, pageSize=null, startFrom=0)]): rowcount = 24.3, cumulative"
            + " cost = {24.3 rows, 25.3 cpu, 0.0 io}, id = *\\n"
            + "\"\n"
            + "  }\n"
            + "}";
    assertEquals(expected, result.replaceAll("id = \\d+", "id = *"));
  }
}
