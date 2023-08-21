/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_DATATYPE_NONNUMERIC;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_DATATYPE_NUMERIC;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;

import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.sql.legacy.SQLIntegTestCase;

public class SystemFunctionIT extends SQLIntegTestCase {

  @Override
  protected void init() throws Exception {
    loadIndex(Index.DATA_TYPE_NONNUMERIC);
    loadIndex(Index.DATA_TYPE_NUMERIC);
  }

  @Test
  public void typeof_sql_types() {
    JSONObject response = executeJdbcRequest("SELECT typeof('pewpew'), typeof(NULL), typeof(1.0),"
        + "typeof(12345), typeof(1234567891011), typeof(INTERVAL 2 DAY);");
    verifyDataRows(response,
        rows("KEYWORD", "UNDEFINED", "DOUBLE", "INTEGER", "LONG", "INTERVAL"));

    response = executeJdbcRequest("SELECT"
        + " typeof(CAST('1961-04-12 09:07:00' AS TIMESTAMP)),"
        + " typeof(CAST('09:07:00' AS TIME)),"
        + " typeof(CAST('1961-04-12' AS DATE)),"
        + " typeof(DATETIME('1961-04-12 09:07:00'))");
    verifyDataRows(response,
        rows("TIMESTAMP", "TIME", "DATE", "DATETIME"));
  }

  @Test
  public void typeof_opensearch_types() {
    JSONObject response = executeJdbcRequest(String.format("SELECT typeof(double_number),"
        + "typeof(long_number), typeof(integer_number), typeof(byte_number), typeof(short_number),"
        + "typeof(float_number), typeof(half_float_number), typeof(scaled_float_number)"
        + " from %s;", TEST_INDEX_DATATYPE_NUMERIC));
    verifyDataRows(response,
        rows("DOUBLE", "LONG", "INTEGER", "BYTE", "SHORT", "FLOAT", "HALF_FLOAT", "SCALED_FLOAT"));

    response = executeJdbcRequest(String.format("SELECT typeof(text_value),"
        + "typeof(date_value), typeof(boolean_value), typeof(object_value), typeof(keyword_value),"
        + "typeof(ip_value), typeof(binary_value), typeof(geo_point_value), typeof(nested_value)"
        + " from %s;", TEST_INDEX_DATATYPE_NONNUMERIC));
    verifyDataRows(response,
        rows("TEXT", "TIMESTAMP", "BOOLEAN", "OBJECT", "KEYWORD",
                "IP", "BINARY", "GEO_POINT", "NESTED"));
  }
}
