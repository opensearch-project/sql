/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ACCOUNT;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK_WITH_NULL_VALUES;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_DATE_TIME;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

public class WhereCommandIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    loadIndex(Index.ACCOUNT);
    loadIndex(Index.BANK_WITH_NULL_VALUES);
    loadIndex(Index.GAME_OF_THRONES);
    loadIndex(Index.DATETIME);
  }

  @Test
  public void testWhereWithLogicalExpr() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | fields firstname | where firstname='Amber' | fields firstname",
                TEST_INDEX_ACCOUNT));
    verifyDataRows(result, rows("Amber"));
  }

  @Test
  public void testWhereWithMultiLogicalExpr() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s "
                    + "| where firstname='Amber' and lastname='Duke' and age=32 "
                    + "| fields firstname, lastname, age",
                TEST_INDEX_ACCOUNT));
    verifyDataRows(result, rows("Amber", "Duke", 32));
  }

  @Test
  public void testMultipleWhereCommands() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s "
                    + "| where firstname='Amber' "
                    + "| fields lastname, age"
                    + "| where lastname='Duke' "
                    + "| fields age "
                    + "| where age=32 "
                    + "| fields age",
                TEST_INDEX_ACCOUNT));
    verifyDataRows(result, rows(32));
  }

  @Test
  public void testWhereEquivalentSortCommand() throws IOException {
    assertEquals(
        executeQueryToString(
            String.format("source=%s | where firstname='Amber'", TEST_INDEX_ACCOUNT)),
        executeQueryToString(String.format("source=%s firstname='Amber'", TEST_INDEX_ACCOUNT)));
  }

  @Test
  public void testLikeFunction() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | fields firstname | where like(firstname, 'Ambe_') | fields firstname",
                TEST_INDEX_ACCOUNT));
    verifyDataRows(result, rows("Amber"));
  }

  @Test
  public void testLikeFunctionNoHit() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | where like(firstname, 'Duk_') | fields lastname",
                TEST_INDEX_BANK_WITH_NULL_VALUES));
    assertEquals(0, result.getInt("total"));
  }

  @Test
  public void testLikeOperator() throws IOException {
    // Test LIKE operator syntax
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | fields firstname | where firstname LIKE 'Ambe_' | fields firstname",
                TEST_INDEX_ACCOUNT));
    verifyDataRows(result, rows("Amber"));

    // Test with wildcard %
    result =
        executeQuery(
            String.format(
                "source=%s | where firstname LIKE 'A%%' | fields firstname", TEST_INDEX_ACCOUNT));
    assertTrue(result.getInt("total") > 0);
  }

  @Test
  public void testLikeOperatorCaseInsensitive() throws IOException {
    // Test LIKE operator with different cases - all should work the same

    // Uppercase LIKE
    JSONObject result1 =
        executeQuery(
            String.format(
                "source=%s | where firstname LIKE 'Ambe_' | fields firstname", TEST_INDEX_ACCOUNT));

    // Lowercase like
    JSONObject result2 =
        executeQuery(
            String.format(
                "source=%s | where firstname like 'Ambe_' | fields firstname", TEST_INDEX_ACCOUNT));

    // Mixed case Like
    JSONObject result3 =
        executeQuery(
            String.format(
                "source=%s | where firstname Like 'Ambe_' | fields firstname", TEST_INDEX_ACCOUNT));

    // All should return the same result
    verifyDataRows(result1, rows("Amber"));
    verifyDataRows(result2, rows("Amber"));
    verifyDataRows(result3, rows("Amber"));
  }

  @Test
  public void testIsNullFunction() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | where isnull(age) | fields firstname",
                TEST_INDEX_BANK_WITH_NULL_VALUES));
    verifyDataRows(result, rows("Virginia"));
  }

  @Test
  public void testIsNotNullFunction() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | where isnotnull(age) and like(firstname, 'Ambe_%%') | fields"
                    + " firstname",
                TEST_INDEX_BANK_WITH_NULL_VALUES));
    verifyDataRows(result, rows("Amber JOHnny"));
  }

  @Test
  public void testWhereWithMetadataFields() throws IOException {
    JSONObject result =
        executeQuery(
            String.format("source=%s | where _id='1' | fields firstname", TEST_INDEX_ACCOUNT));
    verifyDataRows(result, rows("Amber"));
  }

  @Test
  public void testWhereWithMetadataFields2() throws IOException {
    JSONObject result =
        executeQuery(String.format("source=%s | where _id='1'", TEST_INDEX_ACCOUNT));
    verifyDataRows(
        result,
        rows(
            1,
            "Amber",
            "880 Holmes Lane",
            39225,
            "M",
            "Brogan",
            "Pyrami",
            "IL",
            32,
            "amberduke@pyrami.com",
            "Duke"));
  }

  @Test
  public void testWhereWithIn() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | where firstname in ('Amber') | fields firstname", TEST_INDEX_ACCOUNT));
    verifyDataRows(result, rows("Amber"));

    result =
        executeQuery(
            String.format(
                "source=%s | where firstname in ('Amber', 'Dale') | fields firstname",
                TEST_INDEX_ACCOUNT));
    verifyDataRows(result, rows("Amber"), rows("Dale"));

    result =
        executeQuery(
            String.format(
                "source=%s | where balance in (4180, 5686.0) | fields balance",
                TEST_INDEX_ACCOUNT));
    verifyDataRows(result, rows(4180), rows(5686));
  }

  @Test
  public void testWhereWithNotIn() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | where account_number < 4 | where firstname not in ('Amber', 'Levine')"
                    + " | fields firstname",
                TEST_INDEX_ACCOUNT));
    verifyDataRows(result, rows("Roberta"), rows("Bradshaw"));

    result =
        executeQuery(
            String.format(
                "source=%s | where account_number < 4 | where not firstname in ('Amber', 'Levine')"
                    + " | fields firstname",
                TEST_INDEX_ACCOUNT));
    verifyDataRows(result, rows("Roberta"), rows("Bradshaw"));

    result =
        executeQuery(
            String.format(
                "source=%s | where not firstname not in ('Amber', 'Dale') | fields firstname",
                TEST_INDEX_ACCOUNT));
    verifyDataRows(result, rows("Amber"), rows("Dale"));
  }

  @Test
  public void testFilterScriptPushDown() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where firstname ='Amber' and age - 2.0 = 30 | fields firstname, age",
                TEST_INDEX_ACCOUNT));
    verifySchema(actual, schema("firstname", "string"), schema("age", "bigint"));
    verifyDataRows(actual, rows("Amber", 32));
  }

  @Test
  public void testFilterScriptPushDownWithCalciteStdFunction() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where length(firstname) = 5 and abs(age) = 32 and balance = 39225 |"
                    + " fields firstname, age",
                TEST_INDEX_ACCOUNT));
    verifySchema(actual, schema("firstname", "string"), schema("age", "bigint"));
    verifyDataRows(actual, rows("Amber", 32));
  }

  @Test
  public void testFilterScriptPushDownWithPPLBuiltInFunction() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format("source=%s | where month(login_time) = 1", TEST_INDEX_DATE_TIME));
    verifySchema(actual, schema("birthday", "timestamp"), schema("login_time", "timestamp"));
    verifyDataRows(
        actual,
        rows(null, "2015-01-01 00:00:00"),
        rows(null, "2015-01-01 12:10:30"),
        rows(null, "1970-01-19 08:31:22.955"));
  }

  @Test
  public void testFilterScriptPushDownWithCalciteStdLibraryFunction() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where left(firstname, 3) = 'Ama' | fields firstname",
                TEST_INDEX_ACCOUNT));
    verifySchema(actual, schema("firstname", "string"));
    verifyDataRows(actual, rows("Amalia"), rows("Amanda"));
  }

  // ===================== DOUBLE EQUAL (==) OPERATOR TESTS =====================

  @Test
  public void testDoubleEqualOperatorBasic() throws IOException {
    // Test == operator works same as = operator
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | where age == 32 | fields firstname, age | head 3",
                TEST_INDEX_ACCOUNT));
    verifySchema(result, schema("firstname", "string"), schema("age", "bigint"));
    assertEquals(3, result.getJSONArray("datarows").length());
    // Verify all returned records have age 32
    for (int i = 0; i < 3; i++) {
      assertEquals(32, result.getJSONArray("datarows").getJSONArray(i).getLong(1));
    }
  }

  @Test
  public void testDoubleEqualWithStringComparison() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | where firstname == 'Amber' | fields firstname, lastname",
                TEST_INDEX_ACCOUNT));
    verifyDataRows(result, rows("Amber", "Duke"));
  }

  @Test
  public void testDoubleEqualProducesSameResultsAsSingleEqual() throws IOException {
    // Verify = and == produce identical results
    JSONObject resultSingleEqual =
        executeQuery(
            String.format(
                "source=%s | where age = 32 AND gender = 'M' | stats count() as total",
                TEST_INDEX_ACCOUNT));

    JSONObject resultDoubleEqual =
        executeQuery(
            String.format(
                "source=%s | where age == 32 AND gender == 'M' | stats count() as total",
                TEST_INDEX_ACCOUNT));

    // Both queries should return the same count
    assertEquals(
        resultSingleEqual.getJSONArray("datarows").getJSONArray(0).getLong(0),
        resultDoubleEqual.getJSONArray("datarows").getJSONArray(0).getLong(0));
  }

  @Test
  public void testMixedEqualOperators() throws IOException {
    // Test mixing = and == in the same query
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | where age = 32 AND state == 'TN' | fields firstname, age, state",
                TEST_INDEX_ACCOUNT));
    verifySchema(
        result, schema("firstname", "string"), schema("age", "bigint"), schema("state", "string"));
    verifyDataRows(result, rows("Norma", 32, "TN"));

    // Test with operators reversed
    JSONObject result2 =
        executeQuery(
            String.format(
                "source=%s | where age == 32 AND state = 'TN' | fields firstname, age, state",
                TEST_INDEX_ACCOUNT));
    assertEquals(
        result.getJSONArray("datarows").toString(), result2.getJSONArray("datarows").toString());
  }

  @Test
  public void testDoubleEqualWithMultipleConditions() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | where age == 32 AND gender == 'M' | fields firstname, age, gender |"
                    + " head 3",
                TEST_INDEX_ACCOUNT));
    verifySchema(
        result, schema("firstname", "string"), schema("age", "bigint"), schema("gender", "string"));
    // Verify all returned records match the conditions
    for (int i = 0; i < result.getJSONArray("datarows").length(); i++) {
      assertEquals(32, result.getJSONArray("datarows").getJSONArray(i).getLong(1));
      assertEquals("M", result.getJSONArray("datarows").getJSONArray(i).getString(2));
    }
  }

  @Test
  public void testDoubleEqualWithOrCondition() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | where age == 28 OR age == 32 | stats count() as total",
                TEST_INDEX_ACCOUNT));
    // Should count accounts with age 28 or 32
    assertTrue(result.getJSONArray("datarows").getJSONArray(0).getLong(0) > 0);
  }

  @Test
  public void testDoubleEqualInEvalCommand() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval is_thirty_two = (age == 32) | where is_thirty_two == true |"
                    + " fields firstname, age, is_thirty_two | head 2",
                TEST_INDEX_ACCOUNT));
    verifySchema(
        result,
        schema("firstname", "string"),
        schema("age", "bigint"),
        schema("is_thirty_two", "boolean"));
    // All results should have age 32 and is_thirty_two = true
    for (int i = 0; i < result.getJSONArray("datarows").length(); i++) {
      assertEquals(32, result.getJSONArray("datarows").getJSONArray(i).getLong(1));
      assertTrue(result.getJSONArray("datarows").getJSONArray(i).getBoolean(2));
    }
  }

  @Test
  public void testDoubleEqualWithComplexExpression() throws IOException {
    // Test == in complex expression with parentheses
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | where (age == 32 OR age == 28) AND (state == 'ID' OR state == 'WY') |"
                    + " fields firstname, age, state",
                TEST_INDEX_ACCOUNT));
    verifySchema(
        result, schema("firstname", "string"), schema("age", "bigint"), schema("state", "string"));
    // Verify all results match the complex condition
    for (int i = 0; i < result.getJSONArray("datarows").length(); i++) {
      long age = result.getJSONArray("datarows").getJSONArray(i).getLong(1);
      String state = result.getJSONArray("datarows").getJSONArray(i).getString(2);
      assertTrue((age == 32 || age == 28) && (state.equals("ID") || state.equals("WY")));
    }
  }

  @Test
  public void testDoubleEqualChainedWhereCommands() throws IOException {
    // Test multiple where commands with ==
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | where age == 32 | where gender == 'M' | where state == 'TN' | fields"
                    + " firstname, age, gender, state",
                TEST_INDEX_ACCOUNT));
    verifyDataRows(result, rows("Norma", 32, "M", "TN"));
  }

  @Test
  public void testDoubleEqualCaseSensitiveStringComparison() throws IOException {
    // Test case sensitivity in string comparison
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | where firstname == 'amber' | fields firstname", TEST_INDEX_ACCOUNT));
    // Should return no results as 'amber' != 'Amber' (case sensitive)
    assertEquals(0, result.getJSONArray("datarows").length());

    // Correct case should work
    JSONObject result2 =
        executeQuery(
            String.format(
                "source=%s | where firstname == 'Amber' | fields firstname", TEST_INDEX_ACCOUNT));
    verifyDataRows(result2, rows("Amber"));
  }

  @Test
  public void testDoubleEqualWithSpecialCharacters() throws IOException {
    // Test == with strings containing special characters
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | where email == 'amberduke@pyrami.com' | fields firstname, email",
                TEST_INDEX_ACCOUNT));
    verifyDataRows(result, rows("Amber", "amberduke@pyrami.com"));
  }

  @Test
  public void testDoubleEqualWithoutSpaces() throws IOException {
    // Test that == works without spaces
    JSONObject result1 =
        executeQuery(
            String.format(
                "source=%s | where age==32 | stats count() as total", TEST_INDEX_ACCOUNT));

    JSONObject result2 =
        executeQuery(
            String.format(
                "source=%s | where age == 32 | stats count() as total", TEST_INDEX_ACCOUNT));

    // Both should return same count
    assertEquals(
        result1.getJSONArray("datarows").getJSONArray(0).getLong(0),
        result2.getJSONArray("datarows").getJSONArray(0).getLong(0));
  }
}
