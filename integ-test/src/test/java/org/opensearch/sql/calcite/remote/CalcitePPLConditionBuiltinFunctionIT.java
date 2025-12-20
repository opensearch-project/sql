/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.*;
import static org.opensearch.sql.util.MatcherUtils.*;
import static org.opensearch.sql.util.MatcherUtils.rows;

import java.io.IOException;
import java.util.Locale;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;
import org.opensearch.sql.ppl.PPLIntegTestCase;

public class CalcitePPLConditionBuiltinFunctionIT extends PPLIntegTestCase {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();

    loadIndex(Index.STATE_COUNTRY);
    loadIndex(Index.STATE_COUNTRY_WITH_NULL);
    loadIndex(Index.CALCS);
    loadIndex(Index.NESTED_SIMPLE);
    loadIndex(Index.BIG5);
    Request request1 =
        new Request("PUT", "/" + TEST_INDEX_STATE_COUNTRY_WITH_NULL + "/_doc/7?refresh=true");
    request1.setJsonEntity(
        "{\"name\":\"   "
            + " \",\"age\":27,\"state\":\"B.C\",\"country\":\"Canada\",\"year\":2023,\"month\":4}");
    client().performRequest(request1);
    Request request2 =
        new Request("PUT", "/" + TEST_INDEX_STATE_COUNTRY_WITH_NULL + "/_doc/8?refresh=true");
    request2.setJsonEntity(
        "{\"name\":\"\",\"age\":57,\"state\":\"B.C\",\"country\":\"Canada\",\"year\":2023,\"month\":4}");
    client().performRequest(request2);
  }

  @Test
  public void testIsNull() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where isnull(name) | fields age", TEST_INDEX_STATE_COUNTRY_WITH_NULL));

    verifySchema(actual, schema("age", "int"));

    verifyDataRows(actual, rows(10));
  }

  @Test
  public void testIsNullWithStruct() throws IOException {
    JSONObject actual = executeQuery("source=big5 | where isnull(aws) | fields aws");
    verifySchema(actual, schema("aws", "struct"));
    verifyNumOfRows(actual, 0);
  }

  @Test
  public void testIsNullWithNested() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                Locale.ROOT,
                "source=%s | where isnull(address) | fields address",
                TEST_INDEX_NESTED_SIMPLE));
    verifySchema(actual, schema("address", "array"));
    verifyNumOfRows(actual, 0);
  }

  @Test
  public void testIsNotNull() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where isnotnull(name) | fields name",
                TEST_INDEX_STATE_COUNTRY_WITH_NULL));

    verifySchema(actual, schema("name", "string"));

    verifyDataRows(
        actual,
        rows("John"),
        rows("Jane"),
        rows("Jake"),
        rows("Hello"),
        rows("Kevin"),
        rows("    "),
        rows(""));
  }

  @Test
  public void testIsNotNullWithStruct() throws IOException {
    JSONObject actual = executeQuery("source=big5 | where isnotnull(aws) | fields aws");
    verifySchema(actual, schema("aws", "struct"));
    verifyNumOfRows(actual, 3);
  }

  @Test
  public void testIsNotNullWithNested() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                Locale.ROOT,
                "source=%s | where isnotnull(address) | fields address",
                TEST_INDEX_NESTED_SIMPLE));
    verifySchema(actual, schema("address", "array"));
    verifyNumOfRows(actual, 5);
  }

  @Test
  public void testNullIf() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval new_age = nullif(age, 20) | fields name, new_age",
                TEST_INDEX_STATE_COUNTRY_WITH_NULL));

    verifySchema(actual, schema("name", "string"), schema("new_age", "int"));

    verifyDataRows(
        actual,
        rows("John", 25),
        rows("Jane", null),
        rows(null, 10),
        rows("Jake", 70),
        rows("Kevin", null),
        rows("Hello", 30),
        rows("    ", 27),
        rows("", 57));
  }

  @Test
  public void testNullIfWithExpression() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval new_name = Nullif(concat('H', name), 'HHello') | fields name,"
                    + " new_name",
                TEST_INDEX_STATE_COUNTRY_WITH_NULL));

    verifySchema(actual, schema("name", "string"), schema("new_name", "string"));

    verifyDataRows(
        actual,
        rows("John", "HJohn"),
        rows("Jane", "HJane"),
        rows(null, null),
        rows("Jake", "HJake"),
        rows("Kevin", "HKevin"),
        rows("Hello", null),
        rows("    ", "H    "),
        rows("", "H"));
  }

  @Test
  public void testIfNull() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval new_name = ifnull(name, 'Unknown') | fields new_name, age",
                TEST_INDEX_STATE_COUNTRY_WITH_NULL));

    verifySchema(actual, schema("new_name", "string"), schema("age", "int"));

    verifyDataRows(
        actual,
        rows("John", 25),
        rows("Jane", 20),
        rows("Unknown", 10),
        rows("Jake", 70),
        rows("Kevin", null),
        rows("Hello", 30),
        rows("    ", 27),
        rows("", 57));
  }

  @Test
  public void testCoalesce() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where age = 10 | eval new_country = coalesce(name, state, country),"
                    + " null = coalesce(name, state, name)  | fields name, state, country,"
                    + " new_country, null",
                TEST_INDEX_STATE_COUNTRY_WITH_NULL));

    verifySchema(
        actual,
        schema("name", "string"),
        schema("state", "string"),
        schema("country", "string"),
        schema("new_country", "string"),
        schema("null", "string"));

    verifyDataRows(actual, rows(null, null, "Canada", "Canada", null));
  }

  @Test
  public void testIf() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where isnotnull(age) | eval judge = if(age>50, 'old', 'young') |"
                    + " fields judge, age",
                TEST_INDEX_STATE_COUNTRY_WITH_NULL));

    verifySchema(actual, schema("judge", "string"), schema("age", "int"));

    verifyDataRows(
        actual,
        rows("young", 25),
        rows("young", 20),
        rows("young", 10),
        rows("old", 70),
        rows("young", 30),
        rows("young", 27),
        rows("old", 57));
  }

  @Test
  public void testIfWithLike() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval judge = if(Like(name, 'He%%'), 1.0, 0.0) | fields judge, name",
                TEST_INDEX_STATE_COUNTRY));

    verifySchema(actual, schema("judge", "double"), schema("name", "string"));

    verifyDataRows(
        actual, rows(0.0, "John"), rows(0.0, "Jane"), rows(0.0, "Jake"), rows(1.0, "Hello"));
  }

  @Test
  public void testIfWithEquals() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval jake = if(name='Jake', 1, 0) | fields name, jake",
                TEST_INDEX_STATE_COUNTRY));

    verifySchema(actual, schema("name", "string"), schema("jake", "int"));

    verifyDataRows(actual, rows("Jake", 1), rows("Hello", 0), rows("John", 0), rows("Jane", 0));
  }

  @Test
  public void testIsPresent() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where ispresent(name) | fields name, age",
                TEST_INDEX_STATE_COUNTRY_WITH_NULL));

    verifySchema(actual, schema("name", "string"), schema("age", "int"));

    verifyDataRows(
        actual,
        rows("Jake", 70),
        rows("Hello", 30),
        rows("John", 25),
        rows("Jane", 20),
        rows("Kevin", null),
        rows("    ", 27),
        rows("", 57));
  }

  @Test
  public void testIsEmpty() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where isempty(name) | fields name, age",
                TEST_INDEX_STATE_COUNTRY_WITH_NULL));

    verifySchema(actual, schema("name", "string"), schema("age", "int"));

    verifyDataRows(actual, rows(null, 10), rows("", 57));
  }

  @Test
  public void testIsBlank() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where isblank(name) | fields name, age",
                TEST_INDEX_STATE_COUNTRY_WITH_NULL));

    verifySchema(actual, schema("name", "string"), schema("age", "int"));

    verifyDataRows(actual, rows(null, 10), rows("    ", 27), rows("", 57));
  }

  @Test
  public void testEarliest() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where earliest('07/28/2004:12:34:27', datetime0) | stats COUNT() as"
                    + " cnt",
                TEST_INDEX_CALCS));

    verifySchema(actual, schema("cnt", "bigint"));

    verifyDataRows(actual, rows(4));
  }

  @Test
  public void testLatest() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where latest('07/28/2004:12:34:27', datetime0) | stats COUNT() as cnt",
                TEST_INDEX_CALCS));

    verifySchema(actual, schema("cnt", "bigint"));

    verifyDataRows(actual, rows(13));
  }

  @Test
  public void testEarliestWithEval() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval now=utc_timestamp() | eval a = earliest('now', now), b ="
                    + " earliest('-2d@d', now) | fields a,b | head 1",
                TEST_INDEX_CALCS));

    verifySchema(actual, schema("a", "boolean"), schema("b", "boolean"));

    verifyDataRows(actual, rows(false, true));
  }

  @Test
  public void testEvalIsNullWithIf() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval n=if(isnull(name), 'yes', 'no') | fields name, n",
                TEST_INDEX_STATE_COUNTRY_WITH_NULL));

    verifySchema(actual, schema("name", "string"), schema("n", "string"));

    verifyDataRows(
        actual,
        rows("John", "no"),
        rows("Jane", "no"),
        rows(null, "yes"),
        rows("Jake", "no"),
        rows("Kevin", "no"),
        rows("Hello", "no"),
        rows("    ", "no"),
        rows("", "no"));
  }

  @Test
  public void testEvalIsNotNullDirect() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval is_not_null_name=isnotnull(name) | fields name, is_not_null_name",
                TEST_INDEX_STATE_COUNTRY_WITH_NULL));

    verifySchema(actual, schema("name", "string"), schema("is_not_null_name", "boolean"));

    verifyDataRows(
        actual,
        rows("John", true),
        rows("Jane", true),
        rows(null, false),
        rows("Jake", true),
        rows("Kevin", true),
        rows("Hello", true),
        rows("    ", true),
        rows("", true));
  }

  @Test
  public void testEvalIsNullInComplexExpression() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval safe_name=if(isnull(name), 'Unknown', name) | fields safe_name,"
                    + " age",
                TEST_INDEX_STATE_COUNTRY_WITH_NULL));

    verifySchema(actual, schema("safe_name", "string"), schema("age", "int"));

    verifyDataRows(
        actual,
        rows("John", 25),
        rows("Jane", 20),
        rows("Unknown", 10),
        rows("Jake", 70),
        rows("Kevin", null),
        rows("Hello", 30),
        rows("    ", 27),
        rows("", 57));
  }
}
