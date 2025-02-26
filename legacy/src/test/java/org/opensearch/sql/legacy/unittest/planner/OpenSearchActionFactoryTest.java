/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.unittest.planner;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.opensearch.sql.legacy.executor.Format;
import org.opensearch.sql.legacy.query.OpenSearchActionFactory;
import org.opensearch.sql.legacy.util.SqlParserUtils;

public class OpenSearchActionFactoryTest {
  // TODO: deprecate json
  @Test
  public void josnOutputRequestShouldNotMigrateToQueryPlan() {
    String sql = "SELECT age, MAX(balance) FROM account GROUP BY age";

    assertFalse(
        OpenSearchActionFactory.shouldMigrateToQueryPlan(SqlParserUtils.parse(sql), Format.JSON));
  }

  @Test
  public void nestQueryShouldNotMigrateToQueryPlan() {
    String sql = "SELECT age, nested(balance) FROM account GROUP BY age";

    assertFalse(
        OpenSearchActionFactory.shouldMigrateToQueryPlan(SqlParserUtils.parse(sql), Format.JDBC));
  }

  @Test
  public void nonAggregationQueryShouldNotMigrateToQueryPlan() {
    String sql = "SELECT age FROM account ";

    assertFalse(
        OpenSearchActionFactory.shouldMigrateToQueryPlan(SqlParserUtils.parse(sql), Format.JDBC));
  }

  @Test
  public void aggregationQueryWithoutGroupByShouldMigrateToQueryPlan() {
    String sql = "SELECT age, COUNT(balance) FROM account ";

    assertTrue(
        OpenSearchActionFactory.shouldMigrateToQueryPlan(SqlParserUtils.parse(sql), Format.JDBC));
  }

  @Test
  public void aggregationQueryWithExpressionByShouldMigrateToQueryPlan() {
    String sql = "SELECT age, MAX(balance) - MIN(balance) FROM account ";

    assertTrue(
        OpenSearchActionFactory.shouldMigrateToQueryPlan(SqlParserUtils.parse(sql), Format.JDBC));
  }

  @Test
  public void queryOnlyHasGroupByShouldMigrateToQueryPlan() {
    String sql = "SELECT CAST(age AS DOUBLE) as alias FROM account GROUP BY alias";

    assertTrue(
        OpenSearchActionFactory.shouldMigrateToQueryPlan(SqlParserUtils.parse(sql), Format.JDBC));
  }
}
