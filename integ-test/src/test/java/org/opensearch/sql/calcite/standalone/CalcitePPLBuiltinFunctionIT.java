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

    // Verify schema
    verifySchema(
            actual,
            schema("age", "integer")
    );

    // Verify data rows
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
  // Test
  @Test
  public void testConcat() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where name=concat('He', 'llo') | fields name, age",
                TEST_INDEX_STATE_COUNTRY));

    verifySchema(actual, schema("name", "string"), schema("age", "integer"));

    verifyDataRows(actual, rows("Hello", 30));
  }

  @Test
  public void testConcatWithField() throws IOException {
    Request request1 =
        new Request("PUT", "/opensearch-sql_test_index_state_country/_doc/5?refresh=true");
    request1.setJsonEntity(
        "{\"name\":\"HelloWay\",\"age\":27,\"state\":\"Way\",\"country\":\"Canada\",\"year\":2023,\"month\":4}");
    client().performRequest(request1);
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where name=concat('Hello', state) | fields name, age",
                TEST_INDEX_STATE_COUNTRY));

    verifySchema(actual, schema("name", "string"), schema("age", "integer"));

    verifyDataRows(actual, rows("HelloWay", 27));
  }

  @Test
  public void testConcatWs() throws IOException {
    Request request1 =
        new Request("PUT", "/opensearch-sql_test_index_state_country/_doc/5?refresh=true");
    request1.setJsonEntity(
        "{\"name\":\"John,Way\",\"age\":27,\"state\":\"Way\",\"country\":\"Canada\",\"year\":2023,\"month\":4}");
    client().performRequest(request1);
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where name=concat_ws(',', 'John', state) | fields name, age",
                TEST_INDEX_STATE_COUNTRY));

    verifySchema(actual, schema("name", "string"), schema("age", "integer"));

    verifyDataRows(actual, rows("John,Way", 27));
  }

  @Test
  public void testLength() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where length(name) = 5 | fields name, age", TEST_INDEX_STATE_COUNTRY));

    verifySchema(actual, schema("name", "string"), schema("age", "integer"));

    verifyDataRows(actual, rows("Hello", 30));
  }

  @Test
  public void testLengthShouldBeInsensitive() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where leNgTh(name) = 5 | fields name, age", TEST_INDEX_STATE_COUNTRY));

    verifySchema(actual, schema("name", "string"), schema("age", "integer"));

    verifyDataRows(actual, rows("Hello", 30));
  }

  @Test
  public void testLower() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where lower(name) = 'hello' | fields name, age",
                TEST_INDEX_STATE_COUNTRY));

    verifySchema(actual, schema("name", "string"), schema("age", "integer"));

    verifyDataRows(actual, rows("Hello", 30));
  }

  @Test
  public void testUpper() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where upper(name) = upper('hello') | fields name, age",
                TEST_INDEX_STATE_COUNTRY));

    verifySchema(actual, schema("name", "string"), schema("age", "integer"));

    verifyDataRows(actual, rows("Hello", 30));
  }

  @Test
  public void testLike() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where like(name, '_ello%%') | fields name, age",
                TEST_INDEX_STATE_COUNTRY));

    verifySchema(actual, schema("name", "string"), schema("age", "integer"));

    verifyDataRows(actual, rows("Hello", 30));
  }

  @Test
  public void testSubstring() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where substring(name, 2, 2) = 'el' | fields name, age",
                TEST_INDEX_STATE_COUNTRY));

    verifySchema(actual, schema("name", "string"), schema("age", "integer"));

    verifyDataRows(actual, rows("Hello", 30));
  }

  @Test
  public void testPosition() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where position('He' in name) = 1 | fields name, age",
                TEST_INDEX_STATE_COUNTRY));

    verifySchema(actual, schema("name", "string"), schema("age", "integer"));

    verifyDataRows(actual, rows("Hello", 30));
  }

  @Test
  public void testTrim() throws IOException {
    prepareTrim();
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where Trim(name) = 'Jim' | fields name, age",
                TEST_INDEX_STATE_COUNTRY));

    verifySchema(actual, schema("name", "string"), schema("age", "integer"));

    verifyDataRows(actual, rows("   Jim", 27), rows("Jim   ", 57), rows("   Jim   ", 70));
  }

  @Test
  public void testRTrim() throws IOException {
    prepareTrim();
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where RTrim(name) = 'Jim' | fields name, age",
                TEST_INDEX_STATE_COUNTRY));

    verifySchema(actual, schema("name", "string"), schema("age", "integer"));

    verifyDataRows(actual, rows("Jim   ", 57));
  }

  @Test
  public void testLTrim() throws IOException {
    prepareTrim();
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where LTrim(name) = 'Jim' | fields name, age",
                TEST_INDEX_STATE_COUNTRY));

    verifySchema(actual, schema("name", "string"), schema("age", "integer"));

    verifyDataRows(actual, rows("   Jim", 27));
  }

  @Test
  public void testReverse() throws IOException {
    Request request1 =
        new Request("PUT", "/opensearch-sql_test_index_state_country/_doc/5?refresh=true");
    request1.setJsonEntity(
        "{\"name\":\"DeD\",\"age\":27,\"state\":\"B.C\",\"country\":\"Canada\",\"year\":2023,\"month\":4}");
    client().performRequest(request1);
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where Reverse(name) = name | fields name, age",
                TEST_INDEX_STATE_COUNTRY));

    verifySchema(actual, schema("name", "string"), schema("age", "integer"));

    verifyDataRows(actual, rows("DeD", 27));
  }

  @Test
  public void testRight() throws IOException {
    Request request1 =
        new Request("PUT", "/opensearch-sql_test_index_state_country/_doc/5?refresh=true");
    request1.setJsonEntity(
        "{\"name\":\"DeD\",\"age\":27,\"state\":\"B.C\",\"country\":\"Canada\",\"year\":2023,\"month\":4}");
    client().performRequest(request1);
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where right(name, 2) = 'lo' | fields name, age",
                TEST_INDEX_STATE_COUNTRY));

    verifySchema(actual, schema("name", "string"), schema("age", "integer"));

    verifyDataRows(actual, rows("Hello", 30));
  }

  private void prepareTrim() throws IOException {
    Request request1 =
        new Request("PUT", "/opensearch-sql_test_index_state_country/_doc/5?refresh=true");
    request1.setJsonEntity(
        "{\"name\":\"  "
            + " Jim\",\"age\":27,\"state\":\"B.C\",\"country\":\"Canada\",\"year\":2023,\"month\":4}");
    client().performRequest(request1);
    Request request2 =
        new Request("PUT", "/opensearch-sql_test_index_state_country/_doc/6?refresh=true");
    request2.setJsonEntity(
        "{\"name\":\"Jim  "
            + " \",\"age\":57,\"state\":\"B.C\",\"country\":\"Canada\",\"year\":2023,\"month\":4}");
    client().performRequest(request2);
    Request request3 =
        new Request("PUT", "/opensearch-sql_test_index_state_country/_doc/7?refresh=true");
    request3.setJsonEntity(
        "{\"name\":\"   Jim  "
            + " \",\"age\":70,\"state\":\"B.C\",\"country\":\"Canada\",\"year\":2023,\"month\":4}");
    client().performRequest(request3);
  }
}
