/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.sql.calcite.standalone;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_STATE_COUNTRY;
import static org.opensearch.sql.util.MatcherUtils.*;
import static org.opensearch.sql.util.MatcherUtils.rows;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Ignore;
import org.junit.jupiter.api.Test;

public class CalcitePPLBuiltinFunctionIT extends CalcitePPLIntegTestCase {
  @Override
  public void init() throws IOException {
    super.init();
    loadIndex(Index.STATE_COUNTRY);
    loadIndex(Index.STATE_COUNTRY_WITH_NULL);
  }

  @Test
  public void testTypeOfBasic() {
    JSONObject result =
        executeQuery(
            String.format(
                """
                    source=%s
                    | eval `typeof(1)` = typeof(1)
                    | eval `typeof(true)` = typeof(true)
                    | eval `typeof(2.0)` = typeof(2.0)
                    | eval `typeof("2.0")` = typeof("2.0")
                    | eval `typeof(name)` = typeof(name)
                    | eval `typeof(country)` = typeof(country)
                    | eval `typeof(age)` = typeof(age)
                    | eval `typeof(interval)` = typeof(INTERVAL 2 DAY)
                    | fields `typeof(1)`, `typeof(true)`, `typeof(2.0)`, `typeof("2.0")`, `typeof(name)`, `typeof(country)`, `typeof(age)`, `typeof(interval)`
                    | head 1
                    """,
                TEST_INDEX_STATE_COUNTRY));
    verifyDataRows(
        result,
        rows(
            "INTEGER",
            "BOOLEAN",
            "DOUBLE",
            "KEYWORD",
            "KEYWORD",
            "KEYWORD",
            "INTEGER",
            "INTERVAL"));
  }

  @Ignore("https://github.com/opensearch-project/sql/issues/3400")
  public void testTypeOfDateTime() {
    JSONObject result =
        executeQuery(
            String.format(
                """
                    source=%s
                    | eval `typeof(date)` = typeof(DATE('2008-04-14'))
                    | eval `typeof(now())` = typeof(now())
                    | fields `typeof(date)`, `typeof(now())`
                    """,
                TEST_INDEX_STATE_COUNTRY));
  }

  @Test
  public void testSqrtAndPow() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where sqrt(pow(age, 2)) = 30.0 | fields name, age",
                TEST_INDEX_STATE_COUNTRY));

    verifySchema(actual, schema("name", "string"), schema("age", "integer"));

    verifyDataRows(actual, rows("Hello", 30));
  }
}
