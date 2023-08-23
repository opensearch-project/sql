/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql;

import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.sql.legacy.SQLIntegTestCase;

/**
 * This manual IT for NULL literal cannot be replaced with comparison test because other database
 * has different type for expression with NULL involved, such as NULL rather than concrete type
 * inferred like what we do in core engine.
 */
public class NullLiteralIT extends SQLIntegTestCase {

  @Test
  public void testNullLiteralSchema() {
    verifySchema(
        query("SELECT NULL, ABS(NULL), 1 + NULL, NULL + 1.0"),
        schema("NULL", "undefined"),
        schema("ABS(NULL)", "byte"),
        schema("1 + NULL", "integer"),
        schema("NULL + 1.0", "double"));
  }

  @Test
  public void testNullLiteralInOperator() {
    verifyDataRows(query("SELECT NULL = NULL, NULL AND TRUE"), rows(null, null));
  }

  @Test
  public void testNullLiteralInFunction() {
    verifyDataRows(query("SELECT ABS(NULL), POW(2, FLOOR(NULL))"), rows(null, null));
  }

  @Test
  public void testNullLiteralInInterval() {
    verifyDataRows(
        query("SELECT INTERVAL NULL DAY, INTERVAL 60 * 60 * 24 * (NULL - FLOOR(NULL)) SECOND"),
        rows(null, null));
  }

  private JSONObject query(String sql) {
    return new JSONObject(executeQuery(sql, "jdbc"));
  }
}
