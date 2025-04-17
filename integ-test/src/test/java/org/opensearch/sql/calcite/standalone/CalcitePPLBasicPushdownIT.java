/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.standalone;

import org.junit.jupiter.api.Test;
import org.opensearch.sql.common.setting.Settings;

public class CalcitePPLBasicPushdownIT extends CalcitePPLBasicIT {

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
            + "    \"logical\": \"LogicalFilter(condition=[=($1, 20)])\\n"
            + "  CalciteLogicalIndexScan(table=[[OpenSearch, test]])\\n"
            + "\",\n"
            + "    \"physical\": \"CalciteEnumerableIndexScan(table=[[OpenSearch, test]],"
            + " PushDownContext=[[FILTER->=($1, 20)],"
            + " OpenSearchRequestBuilder(sourceBuilder={\\\"from\\\":0,\\\"timeout\\\":\\\"1m\\\",\\\"query\\\":{\\\"term\\\":{\\\"age\\\":{\\\"value\\\":20,\\\"boost\\\":1.0}}},\\\"sort\\\":[{\\\"_doc\\\":{\\\"order\\\":\\\"asc\\\"}}]},"
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
    assertTrue(
        result.contains(
            "CalciteEnumerableIndexScan(table=[[OpenSearch, test]], PushDownContext=[[FILTER->=($1,"
                + " 20)],"
                + " OpenSearchRequestBuilder(sourceBuilder={\\\"from\\\":0,\\\"timeout\\\":\\\"1m\\\",\\\"query\\\":{\\\"term\\\":{\\\"age\\\":{\\\"value\\\":20,\\\"boost\\\":1.0}}},\\\"sort\\\":[{\\\"_doc\\\":{\\\"order\\\":\\\"asc\\\"}}]},"
                + " requestedTotalSize=200, pageSize=null, startFrom=0)]): rowcount = 100.0,"
                + " cumulative cost = {100.0 rows, 101.0 cpu, 0.0 io}"));
  }
}
