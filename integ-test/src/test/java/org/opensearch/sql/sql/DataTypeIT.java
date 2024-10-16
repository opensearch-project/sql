/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql;

import static org.opensearch.sql.legacy.SQLIntegTestCase.Index.DATA_TYPE_NONNUMERIC;
import static org.opensearch.sql.legacy.SQLIntegTestCase.Index.DATA_TYPE_NUMERIC;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.sql.legacy.SQLIntegTestCase;

public class DataTypeIT extends SQLIntegTestCase {
  @Override
  public void init() throws IOException {
    loadIndex(DATA_TYPE_NUMERIC);
    loadIndex(DATA_TYPE_NONNUMERIC);
  }

  @Test
  public void testReadingData() throws IOException {
    String query =
        String.format(
            "SELECT"
                + " long_number,integer_number,short_number,byte_number,double_number,float_number,half_float_number,scaled_float_number"
                + " FROM %s LIMIT 5",
            Index.DATA_TYPE_NUMERIC.getName());
    JSONObject result = executeJdbcRequest(query);
    verifySchema(
        result,
        schema("long_number", "long"),
        schema("integer_number", "integer"),
        schema("short_number", "short"),
        schema("byte_number", "byte"),
        schema("float_number", "float"),
        schema("double_number", "double"),
        schema("half_float_number", "float"),
        schema("scaled_float_number", "double"));
    verifyDataRows(
        result,
        rows(1, 2, 3, 4, 5.1, 6.2, 7.3, 8.4),
        rows(1, 2, 3, 4, 5.1, 6.2, 7.3, 8.4),
        rows(1, 2, 3, 4, 5.1, 6.2, 7.3, 8.4));

    query =
        String.format(
            "SELECT boolean_value,keyword_value FROM %s LIMIT 5",
            Index.DATA_TYPE_NONNUMERIC.getName());
    result = executeJdbcRequest(query);
    verifySchema(result, schema("boolean_value", "boolean"), schema("keyword_value", "keyword"));
    verifyDataRows(result, rows(true, "123"), rows(true, "123"), rows(true, "123"));
  }
}
