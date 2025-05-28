/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.standalone;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_STATE_COUNTRY_WITH_NULL;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Test;

public class CalcitePPLFillnullIT extends CalcitePPLIntegTestCase {

  @Override
  public void init() throws IOException {
    super.init();
    loadIndex(Index.STATE_COUNTRY_WITH_NULL);
  }

  @Test
  public void testFillnullWith() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | fillnull with 'N/A' in name, state, country | fields name, age, state,"
                    + " country, year, month",
                TEST_INDEX_STATE_COUNTRY_WITH_NULL));
    verifySchema(
        actual,
        schema("name", "string"),
        schema("age", "integer"),
        schema("state", "string"),
        schema("country", "string"),
        schema("year", "integer"),
        schema("month", "integer"));
    verifyDataRows(
        actual,
        rows("Jake", 70, "California", "USA", 2023, 4),
        rows("Hello", 30, "New York", "USA", 2023, 4),
        rows("John", 25, "Ontario", "Canada", 2023, 4),
        rows("Jane", 20, "Quebec", "Canada", 2023, 4),
        rows("Kevin", null, "N/A", "N/A", 2023, 4),
        rows("N/A", 10, "N/A", "Canada", 2023, 4));
  }

  @Test
  public void testFillnullUsing() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | fillnull using name='N/A', state='N/A', country='N/A' | fields name,"
                    + " age, state, country, year, month",
                TEST_INDEX_STATE_COUNTRY_WITH_NULL));
    verifySchema(
        actual,
        schema("name", "string"),
        schema("age", "integer"),
        schema("state", "string"),
        schema("country", "string"),
        schema("year", "integer"),
        schema("month", "integer"));
    verifyDataRows(
        actual,
        rows("Jake", 70, "California", "USA", 2023, 4),
        rows("Hello", 30, "New York", "USA", 2023, 4),
        rows("John", 25, "Ontario", "Canada", 2023, 4),
        rows("Jane", 20, "Quebec", "Canada", 2023, 4),
        rows("Kevin", null, "N/A", "N/A", 2023, 4),
        rows("N/A", 10, "N/A", "Canada", 2023, 4));
  }

  @Test
  public void testFillnullWithoutFieldList() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | fields name, state, country | fillnull with 'N/A'",
                TEST_INDEX_STATE_COUNTRY_WITH_NULL));
    verifySchema(
        actual, schema("name", "string"), schema("state", "string"), schema("country", "string"));
    verifyDataRows(
        actual,
        rows("Jake", "California", "USA"),
        rows("Hello", "New York", "USA"),
        rows("John", "Ontario", "Canada"),
        rows("Jane", "Quebec", "Canada"),
        rows("Kevin", "N/A", "N/A"),
        rows("N/A", "N/A", "Canada"));
  }
}
