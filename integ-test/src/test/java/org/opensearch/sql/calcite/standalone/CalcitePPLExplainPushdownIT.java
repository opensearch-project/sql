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
            + " requestedTotalSize=2147483647, pageSize=null, startFrom=0)])\\n"
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
            + "    \"logical\": \"LogicalProject(name=[$0], age=[$1]): rowcount = 1500.0,"
            + " cumulative cost = {13000.0 rows, 23001.0 cpu, 0.0 io}, id = *\\n"
            + "  LogicalFilter(condition=[=($1, 20)]): rowcount = 1500.0, cumulative cost ="
            + " {11500.0 rows, 20001.0 cpu, 0.0 io}, id = *\\n"
            + "    CalciteLogicalIndexScan(table=[[OpenSearch, test]]): rowcount = 10000.0,"
            + " cumulative cost = {10000.0 rows, 10001.0 cpu, 0.0 io}, id = *\\n"
            + "\",\n"
            + "    \"physical\": \"CalciteEnumerableIndexScan(table=[[OpenSearch, test]],"
            + " PushDownContext=[[PROJECT->[name, age], FILTER->=($1, 20)],"
            + " OpenSearchRequestBuilder(sourceBuilder={\\\"from\\\":0,\\\"timeout\\\":\\\"1m\\\",\\\"query\\\":{\\\"term\\\":{\\\"age\\\":{\\\"value\\\":20,\\\"boost\\\":1.0}}},\\\"_source\\\":{\\\"includes\\\":[\\\"name\\\",\\\"age\\\"],\\\"excludes\\\":[]},\\\"sort\\\":[{\\\"_doc\\\":{\\\"order\\\":\\\"asc\\\"}}]},"
            + " requestedTotalSize=2147483647, pageSize=null, startFrom=0)]): rowcount = 1215.0,"
            + " cumulative cost = {1215.0 rows, 1216.0 cpu, 0.0 io}, id = *\\n"
            + "\"\n"
            + "  }\n"
            + "}";
    assertEquals(expected, result.replaceAll("id = \\d+", "id = *"));
  }
}
