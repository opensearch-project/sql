/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_NULL_MISSING;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.legacy.SQLIntegTestCase;

public class AggregationIT extends SQLIntegTestCase {
  @Override
  protected void init() throws Exception {
    loadIndex(Index.BANK);
    loadIndex(Index.NULL_MISSING);
  }

  @Test
  void filteredAggregatePushedDown() throws IOException {
    JSONObject response = executeQuery(
        "SELECT COUNT(*) FILTER(WHERE age > 35) FROM " + TEST_INDEX_BANK);
    verifySchema(response, schema("COUNT(*)", null, "integer"));
    verifyDataRows(response, rows(3));
  }

  @Test
  void filteredAggregateNotPushedDown() throws IOException {
    JSONObject response = executeQuery(
        "SELECT COUNT(*) FILTER(WHERE age > 35) FROM (SELECT * FROM " + TEST_INDEX_BANK
            + ") AS a");
    verifySchema(response, schema("COUNT(*)", null, "integer"));
    verifyDataRows(response, rows(3));
  }

  @Test
  void pushedDownAggregationOnNullValues() throws IOException {
    // OpenSearch aggregation query (MetricAggregation)
    var response = executeQuery(String.format(
        "SELECT min(`int`), max(`int`), avg(`int`), min(`dbl`), max(`dbl`), avg(`dbl`) " +
        "FROM %s WHERE `key` = 'null'", TEST_INDEX_NULL_MISSING));
    verifySchema(response,
        schema("min(`int`)", null, "integer"), schema("max(`int`)", null, "integer"),
        schema("avg(`int`)", null, "integer"), schema("min(`dbl`)", null, "integer"),
        schema("max(`dbl`)", null, "integer"), schema("avg(`dbl`)", null, "integer"));
    verifyDataRows(response, rows(null, null, null, null, null, null));
  }

  @Test
  void pushedDownAggregationOnMissingValues() throws IOException {
    // OpenSearch aggregation query (MetricAggregation)
    var response = executeQuery(String.format(
        "SELECT min(`int`), max(`int`), avg(`int`), min(`dbl`), max(`dbl`), avg(`dbl`) " +
        "FROM %s WHERE `key` = 'null'", TEST_INDEX_NULL_MISSING));
    verifySchema(response,
        schema("min(`int`)", null, "integer"), schema("max(`int`)", null, "integer"),
        schema("avg(`int`)", null, "integer"), schema("min(`dbl`)", null, "integer"),
        schema("max(`dbl`)", null, "integer"), schema("avg(`dbl`)", null, "integer"));
    verifyDataRows(response, rows(null, null, null, null, null, null));
  }

  @Test
  void inMemoryAggregationOnNullValues() throws IOException {
    // In-memory aggregation performed by the plugin
    var response = executeQuery(String.format("SELECT"
        + " min(`int`) over (PARTITION BY `key`), max(`int`) over (PARTITION BY `key`),"
        + " avg(`int`) over (PARTITION BY `key`), min(`dbl`) over (PARTITION BY `key`),"
        + " max(`dbl`) over (PARTITION BY `key`), avg(`dbl`) over (PARTITION BY `key`)"
        + " FROM %s WHERE `key` = 'null'", TEST_INDEX_NULL_MISSING));
    verifySchema(response,
        schema("min(`int`) over (PARTITION BY `key`)", null, "integer"),
        schema("max(`int`) over (PARTITION BY `key`)", null, "integer"),
        schema("avg(`int`) over (PARTITION BY `key`)", null, "integer"),
        schema("min(`dbl`) over (PARTITION BY `key`)", null, "integer"),
        schema("max(`dbl`) over (PARTITION BY `key`)", null, "integer"),
        schema("avg(`dbl`) over (PARTITION BY `key`)", null, "integer"));
    verifyDataRows(response, // 4 rows with null values
        rows(null, null, null, null, null, null),
        rows(null, null, null, null, null, null),
        rows(null, null, null, null, null, null),
        rows(null, null, null, null, null, null));
  }

  @Test
  void inMemoryAggregationOnMissingValues() throws IOException {
    // In-memory aggregation performed by the plugin
    var response = executeQuery(String.format("SELECT"
        + " min(`int`) over (PARTITION BY `key`), max(`int`) over (PARTITION BY `key`),"
        + " avg(`int`) over (PARTITION BY `key`), min(`dbl`) over (PARTITION BY `key`),"
        + " max(`dbl`) over (PARTITION BY `key`), avg(`dbl`) over (PARTITION BY `key`)"
        + " FROM %s WHERE `key` = 'missing'", TEST_INDEX_NULL_MISSING));
    verifySchema(response,
        schema("min(`int`) over (PARTITION BY `key`)", null, "integer"),
        schema("max(`int`) over (PARTITION BY `key`)", null, "integer"),
        schema("avg(`int`) over (PARTITION BY `key`)", null, "integer"),
        schema("min(`dbl`) over (PARTITION BY `key`)", null, "integer"),
        schema("max(`dbl`) over (PARTITION BY `key`)", null, "integer"),
        schema("avg(`dbl`) over (PARTITION BY `key`)", null, "integer"));
    verifyDataRows(response, // 4 rows with null values
        rows(null, null, null, null, null, null),
        rows(null, null, null, null, null, null),
        rows(null, null, null, null, null, null),
        rows(null, null, null, null, null, null));
  }
}
