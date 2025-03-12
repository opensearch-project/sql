/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.sql.calcite.standalone;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_STATE_COUNTRY;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_STATE_COUNTRY_WITH_NULL;
import static org.opensearch.sql.util.MatcherUtils.*;
import static org.opensearch.sql.util.MatcherUtils.rows;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;

public class CalcitePPLBuiltinFunctionIT extends CalcitePPLIntegTestCase {
  @Override
  public void init() throws IOException {
    super.init();
    loadIndex(Index.STATE_COUNTRY);
    loadIndex(Index.STATE_COUNTRY_WITH_NULL);
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

  @Test
  public void testIsNull() {
    JSONObject actual = executeQuery(
            String.format(
                    "source=%s | where isnull(name) | fields age",
                    TEST_INDEX_STATE_COUNTRY_WITH_NULL
            )
    );

    verifySchema(
            actual,
            schema("age", "integer")
    );

    verifyDataRows(
            actual,
            rows(10)
    );
  }

  @Test
  public void testIsNotNull() {
    JSONObject actual =
            executeQuery(
                    String.format(
                            "source=%s | where isnotnull(name) | fields name",
                            TEST_INDEX_STATE_COUNTRY_WITH_NULL));

    verifySchema(
            actual,
            schema("name", "string"));

    verifyDataRows(
            actual,
            rows("John"),
            rows("Jane"),
            rows("Jake"),
            rows("Hello"));

  }

  @Test
  public void testNullIf() {
    JSONObject actual =
            executeQuery(
                    String.format(
                            "source=%s | eval new_age = nullif(age, 20) | fields name, new_age",
                            TEST_INDEX_STATE_COUNTRY_WITH_NULL));

    verifySchema(
            actual,
            schema("name", "string"),
            schema("new_age", "integer"));

    verifyDataRows(
            actual,
            rows("John", 25),
            rows("Jane", null),
            rows(null, 10),
            rows("Jake", 70),
            rows("Hello", 30));
  }


  @Test
  public void testIfNull() {
    JSONObject actual =
            executeQuery(
                    String.format(
                            "source=%s | eval new_name = ifnull(name, 'Unknown') | fields new_name, age",
                            TEST_INDEX_STATE_COUNTRY_WITH_NULL));

    verifySchema(
            actual,
            schema("new_name", "string"),
            schema("age", "integer"));

    verifyDataRows(
            actual,
            rows("John", 25),
            rows("Jane", 20),
            rows("Unknown", 10),
            rows("Jake", 70),
            rows("Hello", 30));

  }

  @Test
  public void testIf() {
    JSONObject actual =
            executeQuery(
                    String.format(
                            "source=%s | eval judge = if(age>50, 'old', 'young') | fields judge, age",
                            TEST_INDEX_STATE_COUNTRY_WITH_NULL));

    verifySchema(
            actual,
            schema("judge", "string"),
            schema("age", "integer"));

    verifyDataRows(
            actual,
            rows("young", 25),
            rows("young", 20),
            rows("young", 10),
            rows("old", 70),
            rows("young", 30));
  }

  @Test
  public void testIfWithLike() {
    JSONObject actual =
            executeQuery(
                    String.format(
                            "source=%s | eval judge = if(Like(name, 'He%'), 1.0, 0.0) | fields judge, name",
                            TEST_INDEX_STATE_COUNTRY));

    verifySchema(
            actual,
            schema("judge", "double"),
            schema("name", "string"));

    verifyDataRows(
            actual,
            rows("John", 0.0),
            rows("Jane", 0.0),
            rows("Jake", 0.0),
            rows("Hello", 1.0));
  }

}
