/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.junit.jupiter.api.Assertions.*;
import static org.opensearch.sql.legacy.TestUtils.createIndexByRestClient;
import static org.opensearch.sql.legacy.TestUtils.isIndexExist;
import static org.opensearch.sql.legacy.TestUtils.loadDataByRestClient;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ppl.PPLIntegTestCase;

public class CalciteTimechartPerFunctionIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    disallowCalciteFallback();

    // Load shared events index for basic per_second tests
    loadIndex(Index.EVENTS);

    // Create dedicated per_second test data with clean math
    createPerSecondTestIndex();
  }

  private void createPerSecondTestIndex() throws IOException {
    String mapping =
        "{\"mappings\":{\"properties\":{\"@timestamp\":{\"type\":\"date\"},\"packets\":{\"type\":\"integer\"},\"host\":{\"type\":\"keyword\"}}}}";

    if (!isIndexExist(client(), "timechart_per_second_test")) {
      createIndexByRestClient(client(), "timechart_per_second_test", mapping);
      loadDataByRestClient(
          client(),
          "timechart_per_second_test",
          "src/test/resources/timechart_per_second_test.json");
    }
  }

  @Test
  public void testTimechartPerSecondWithDefaultSpan() throws IOException {
    JSONObject result = executeQuery("source=events | timechart per_second(cpu_usage)");

    verifySchema(
        result, schema("@timestamp", "timestamp"), schema("per_second(cpu_usage)", "double"));

    // Default span should be 1m, so per_second uses runtime calculation
    assertTrue("Results should not be empty", result.getJSONArray("datarows").length() > 0);
    assertEquals(5, result.getInt("total"));
  }

  @Test
  public void testTimechartPerSecondWithOneMinuteSpan() throws IOException {
    JSONObject result = executeQuery("source=events | timechart span=1m per_second(cpu_usage)");

    verifySchema(
        result, schema("@timestamp", "timestamp"), schema("per_second(cpu_usage)", "double"));

    // With 1m span: uses timestampdiff(SECOND, @timestamp, timestampadd(MINUTE, 1, @timestamp)) * 1
    assertEquals(5, result.getInt("total"));
  }

  @Test
  public void testTimechartPerSecondWithTwoHourSpan() throws IOException {
    JSONObject result = executeQuery("source=events | timechart span=2h per_second(cpu_usage)");

    verifySchema(
        result, schema("@timestamp", "timestamp"), schema("per_second(cpu_usage)", "double"));

    // With 2h span: uses runtime calculation with timestampadd(HOUR, 2, @timestamp)
    assertEquals(1, result.getInt("total"));
  }

  @Test
  public void testTimechartPerSecondWithByLimitUseother() throws IOException {
    JSONObject result =
        executeQuery(
            "source=events | timechart span=1m limit=1 useother=true per_second(cpu_usage) by"
                + " host");

    verifySchema(
        result,
        schema("@timestamp", "timestamp"),
        schema("host", "string"),
        schema("per_second(cpu_usage)", "double"));

    // Should show top 1 host + OTHER category across time spans
    boolean foundOther = false;
    for (int i = 0; i < result.getJSONArray("datarows").length(); i++) {
      Object[] row = result.getJSONArray("datarows").getJSONArray(i).toList().toArray();
      if ("OTHER".equals(row[1])) {
        foundOther = true;
        break;
      }
    }
    assertTrue("OTHER category should be present with limit=1", foundOther);
  }

  @Test
  public void testTimechartPerSecondWithLeapYearFebruary() throws IOException {
    // Test February 2024 with runtime TIMESTAMPDIFF calculation
    // sum(packets) = 360, uses dynamic timestampdiff for actual interval calculation
    JSONObject result =
        executeQuery(
            "source=timechart_per_second_test | where year(@timestamp)=2024 and month(@timestamp)=2"
                + " | timechart span=1mon per_second(packets)");

    verifySchema(
        result, schema("@timestamp", "timestamp"), schema("per_second(packets)", "double"));

    assertEquals(1, result.getInt("total"));
    Object[] row = result.getJSONArray("datarows").getJSONArray(0).toList().toArray();
    double perSecondValue = ((Number) row[1]).doubleValue();

    // With dynamic TIMESTAMPDIFF calculation, per_second uses actual runtime calculation
    assertTrue("per_second value should be positive", perSecondValue > 0);
  }

  @Test
  public void testTimechartPerSecondWithRegularFebruary() throws IOException {
    // Test February 2023 with runtime TIMESTAMPDIFF calculation
    // sum(packets) = 360, uses dynamic timestampdiff for actual interval calculation
    JSONObject result =
        executeQuery(
            "source=timechart_per_second_test | where year(@timestamp)=2023 and month(@timestamp)=2"
                + " | timechart span=1mon per_second(packets)");

    verifySchema(
        result, schema("@timestamp", "timestamp"), schema("per_second(packets)", "double"));

    assertEquals(1, result.getInt("total"));
    Object[] row = result.getJSONArray("datarows").getJSONArray(0).toList().toArray();
    double perSecondValue = ((Number) row[1]).doubleValue();

    // With dynamic TIMESTAMPDIFF calculation, per_second uses actual runtime calculation
    assertTrue("per_second value should be positive", perSecondValue > 0);
  }

  @Test
  public void testTimechartPerSecondWithOctober() throws IOException {
    // Test October 2023 with runtime TIMESTAMPDIFF calculation
    // sum(packets) = 360, uses dynamic timestampdiff for actual interval calculation
    JSONObject result =
        executeQuery(
            "source=timechart_per_second_test | where year(@timestamp)=2023 and"
                + " month(@timestamp)=10 | timechart span=1mon per_second(packets)");

    verifySchema(
        result, schema("@timestamp", "timestamp"), schema("per_second(packets)", "double"));

    assertEquals(1, result.getInt("total"));
    Object[] row = result.getJSONArray("datarows").getJSONArray(0).toList().toArray();
    double perSecondValue = ((Number) row[1]).doubleValue();

    // With dynamic TIMESTAMPDIFF calculation, per_second uses actual runtime calculation
    assertTrue("per_second value should be positive", perSecondValue > 0);
  }

  @Test
  public void testTimechartPerSecondWithByClauseAndLimit() throws IOException {
    // Test with 2025 data, by clause, limit=1, useother=true
    JSONObject result =
        executeQuery(
            "source=timechart_per_second_test | where year(@timestamp)=2025 | timechart span=1h"
                + " limit=1 useother=true per_second(packets) by host");

    verifySchema(
        result,
        schema("@timestamp", "timestamp"),
        schema("host", "string"),
        schema("per_second(packets)", "double"));

    // Should have results for each hour with top 1 host + OTHER
    boolean foundOther = false;
    boolean foundServer1 = false;

    for (int i = 0; i < result.getJSONArray("datarows").length(); i++) {
      Object[] row = result.getJSONArray("datarows").getJSONArray(i).toList().toArray();
      String host = (String) row[1];

      if ("OTHER".equals(host)) {
        foundOther = true;
      } else if ("server1".equals(host)) {
        foundServer1 = true;
      }
    }

    assertTrue("Should have server1 data", foundServer1);
    assertTrue("Should have OTHER category with limit=1", foundOther);
  }
}
