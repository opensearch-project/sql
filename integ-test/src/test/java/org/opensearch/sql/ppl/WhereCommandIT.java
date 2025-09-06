/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.hamcrest.CoreMatchers.containsString;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ACCOUNT;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import java.util.stream.Collectors;
import org.hamcrest.MatcherAssert;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.type.ExprCoreType;

public class WhereCommandIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    loadIndex(Index.ACCOUNT);
    loadIndex(Index.BANK_WITH_NULL_VALUES);
    loadIndex(Index.GAME_OF_THRONES);
    loadIndex(Index.DATE_TIME);
  }

  @Test
  public void testWhereWithLogicalExpr() throws IOException {
    JSONObject result =
        executeQuery(
            Index.ACCOUNT.ppl("fields firstname | where firstname='Amber' | fields firstname"));
    verifyDataRows(result, rows("Amber"));
  }

  @Test
  public void testWhereWithMultiLogicalExpr() throws IOException {
    JSONObject result =
        executeQuery(
            Index.ACCOUNT.ppl(
                "where firstname='Amber' and lastname='Duke' and age=32 "
                    + "| fields firstname, lastname, age"));
    verifyDataRows(result, rows("Amber", "Duke", 32));
  }

  @Test
  public void testMultipleWhereCommands() throws IOException {
    JSONObject result =
        executeQuery(
            Index.ACCOUNT.ppl(
                "where firstname='Amber' "
                    + "| fields lastname, age"
                    + "| where lastname='Duke' "
                    + "| fields age "
                    + "| where age=32 "
                    + "| fields age"));
    verifyDataRows(result, rows(32));
  }

  @Test
  public void testWhereEquivalentSortCommand() throws IOException {
    assertEquals(
        executeQueryToString(Index.ACCOUNT.ppl("where firstname='Amber'")),
        executeQueryToString(String.format("source=%s firstname='Amber'", TEST_INDEX_ACCOUNT)));
  }

  @Test
  public void testLikeFunction() throws IOException {
    JSONObject result =
        executeQuery(
            Index.ACCOUNT.ppl(
                "fields firstname | where like(firstname, 'Ambe_') | fields firstname"));
    verifyDataRows(result, rows("Amber"));
  }

  @Test
  public void testLikeFunctionNoHit() throws IOException {
    JSONObject result =
        executeQuery(
            Index.BANK_WITH_NULL_VALUES.ppl("where like(firstname, 'Duk_') | fields lastname"));
    assertEquals(0, result.getInt("total"));
  }

  @Test
  public void testLikeOperator() throws IOException {
    // Test LIKE operator syntax
    JSONObject result =
        executeQuery(
            Index.ACCOUNT.ppl(
                "fields firstname | where firstname LIKE 'Ambe_' | fields firstname"));
    verifyDataRows(result, rows("Amber"));

    // Test with wildcard %
    result = executeQuery(Index.ACCOUNT.ppl("where firstname LIKE 'A%%' | fields firstname"));
    assertTrue(result.getInt("total") > 0);
  }

  @Test
  public void testLikeOperatorCaseInsensitive() throws IOException {
    // Test LIKE operator with different cases - all should work the same

    // Uppercase LIKE
    JSONObject result1 =
        executeQuery(Index.ACCOUNT.ppl("where firstname LIKE 'Ambe_' | fields firstname"));

    // Lowercase like
    JSONObject result2 =
        executeQuery(Index.ACCOUNT.ppl("where firstname like 'Ambe_' | fields firstname"));

    // Mixed case Like
    JSONObject result3 =
        executeQuery(Index.ACCOUNT.ppl("where firstname Like 'Ambe_' | fields firstname"));

    // All should return the same result
    verifyDataRows(result1, rows("Amber"));
    verifyDataRows(result2, rows("Amber"));
    verifyDataRows(result3, rows("Amber"));
  }

  @Test
  public void testIsNullFunction() throws IOException {
    JSONObject result =
        executeQuery(Index.BANK_WITH_NULL_VALUES.ppl("where isnull(age) | fields firstname"));
    verifyDataRows(result, rows("Virginia"));
  }

  @Test
  public void testIsNotNullFunction() throws IOException {
    JSONObject result =
        executeQuery(
            Index.BANK_WITH_NULL_VALUES.ppl(
                "where isnotnull(age) and like(firstname, 'Ambe_%%') | fields" + " firstname"));
    verifyDataRows(result, rows("Amber JOHnny"));
  }

  @Test
  public void testWhereWithMetadataFields() throws IOException {
    JSONObject result = executeQuery(Index.ACCOUNT.ppl("where _id='1' | fields firstname"));
    verifyDataRows(result, rows("Amber"));
  }

  @Test
  public void testWhereWithMetadataFields2() throws IOException {
    JSONObject result = executeQuery(Index.ACCOUNT.ppl("where _id='1'"));
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
        executeQuery(Index.ACCOUNT.ppl("where firstname in ('Amber') | fields firstname"));
    verifyDataRows(result, rows("Amber"));

    result =
        executeQuery(Index.ACCOUNT.ppl("where firstname in ('Amber', 'Dale') | fields firstname"));
    verifyDataRows(result, rows("Amber"), rows("Dale"));

    result = executeQuery(Index.ACCOUNT.ppl("where balance in (4180, 5686.0) | fields balance"));
    verifyDataRows(result, rows(4180), rows(5686));
  }

  @Test
  public void testWhereWithNotIn() throws IOException {
    JSONObject result =
        executeQuery(
            Index.ACCOUNT.ppl(
                "where account_number < 4 | where firstname not in ('Amber', 'Levine')"
                    + " | fields firstname"));
    verifyDataRows(result, rows("Roberta"), rows("Bradshaw"));

    result =
        executeQuery(
            Index.ACCOUNT.ppl(
                "where account_number < 4 | where not firstname in ('Amber', 'Levine')"
                    + " | fields firstname"));
    verifyDataRows(result, rows("Roberta"), rows("Bradshaw"));

    result =
        executeQuery(
            Index.ACCOUNT.ppl("where not firstname not in ('Amber', 'Dale') | fields firstname"));
    verifyDataRows(result, rows("Amber"), rows("Dale"));
  }

  @Test
  public void testInWithIncompatibleType() {
    Exception e =
        assertThrows(
            Exception.class,
            () -> {
              executeQuery(
                  Index.ACCOUNT.ppl("where balance in (4180, 5686, '6077') | fields firstname"));
            });
    MatcherAssert.assertThat(e.getMessage(), containsString(getIncompatibleTypeErrMsg()));
  }

  protected String getIncompatibleTypeErrMsg() {
    return String.format(
        "function expected %s, but got %s",
        ExprCoreType.coreTypes().stream()
            .map(type -> String.format("[%s,%s]", type.typeName(), type.typeName()))
            .collect(Collectors.joining(",", "{", "}")),
        "[LONG,STRING]");
  }

  @Test
  public void testFilterScriptPushDown() throws IOException {
    JSONObject actual =
        executeQuery(
            Index.ACCOUNT.ppl(
                "where firstname ='Amber' and age - 2.0 = 30 | fields firstname, age"));
    verifySchema(actual, schema("firstname", "string"), schema("age", "bigint"));
    verifyDataRows(actual, rows("Amber", 32));
  }

  @Test
  public void testFilterScriptPushDownWithCalciteStdFunction() throws IOException {
    JSONObject actual =
        executeQuery(
            Index.ACCOUNT.ppl(
                "where length(firstname) = 5 and abs(age) = 32 and balance = 39225 |"
                    + " fields firstname, age"));
    verifySchema(actual, schema("firstname", "string"), schema("age", "bigint"));
    verifyDataRows(actual, rows("Amber", 32));
  }

  @Test
  public void testFilterScriptPushDownWithPPLBuiltInFunction() throws IOException {
    JSONObject actual = executeQuery(Index.DATE_TIME.ppl("where month(login_time) = 1"));
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
        executeQuery(Index.ACCOUNT.ppl("where left(firstname, 3) = 'Ama' | fields firstname"));
    verifySchema(actual, schema("firstname", "string"));
    verifyDataRows(actual, rows("Amalia"), rows("Amanda"));
  }

  // ===================== DOUBLE EQUAL (==) OPERATOR TESTS =====================

  @Test
  public void testDoubleEqualOperatorBasic() throws IOException {
    // Test == operator works same as = operator
    JSONObject result =
        executeQuery(Index.ACCOUNT.ppl("where age == 32 | fields firstname, age | head 3"));
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
        executeQuery(Index.ACCOUNT.ppl("where firstname == 'Amber' | fields firstname, lastname"));
    verifyDataRows(result, rows("Amber", "Duke"));
  }

  @Test
  public void testDoubleEqualProducesSameResultsAsSingleEqual() throws IOException {
    // Verify = and == produce identical results
    JSONObject resultSingleEqual =
        executeQuery(Index.ACCOUNT.ppl("where age = 32 AND gender = 'M' | stats count() as total"));

    JSONObject resultDoubleEqual =
        executeQuery(
            Index.ACCOUNT.ppl("where age == 32 AND gender == 'M' | stats count() as total"));

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
            Index.ACCOUNT.ppl("where age = 32 AND state == 'TN' | fields firstname, age, state"));
    verifySchema(
        result, schema("firstname", "string"), schema("age", "bigint"), schema("state", "string"));
    verifyDataRows(result, rows("Norma", 32, "TN"));

    // Test with operators reversed
    JSONObject result2 =
        executeQuery(
            Index.ACCOUNT.ppl("where age == 32 AND state = 'TN' | fields firstname, age, state"));
    assertEquals(
        result.getJSONArray("datarows").toString(), result2.getJSONArray("datarows").toString());
  }

  @Test
  public void testDoubleEqualWithMultipleConditions() throws IOException {
    JSONObject result =
        executeQuery(
            Index.ACCOUNT.ppl(
                "where age == 32 AND gender == 'M' | fields firstname, age, gender |" + " head 3"));
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
        executeQuery(Index.ACCOUNT.ppl("where age == 28 OR age == 32 | stats count() as total"));
    // Should count accounts with age 28 or 32
    assertTrue(result.getJSONArray("datarows").getJSONArray(0).getLong(0) > 0);
  }

  @Test
  public void testDoubleEqualInEvalCommand() throws IOException {
    JSONObject result =
        executeQuery(
            Index.ACCOUNT.ppl(
                "eval is_thirty_two = (age == 32) | where is_thirty_two == true |"
                    + " fields firstname, age, is_thirty_two | head 2"));
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
            Index.ACCOUNT.ppl(
                "where (age == 32 OR age == 28) AND (state == 'ID' OR state == 'WY') |"
                    + " fields firstname, age, state"));
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
            Index.ACCOUNT.ppl(
                "where age == 32 | where gender == 'M' | where state == 'TN' | fields"
                    + " firstname, age, gender, state"));
    verifyDataRows(result, rows("Norma", 32, "M", "TN"));
  }

  @Test
  public void testDoubleEqualCaseSensitiveStringComparison() throws IOException {
    // Test case sensitivity in string comparison
    JSONObject result =
        executeQuery(Index.ACCOUNT.ppl("where firstname == 'amber' | fields firstname"));
    // Should return no results as 'amber' != 'Amber' (case sensitive)
    assertEquals(0, result.getJSONArray("datarows").length());

    // Correct case should work
    JSONObject result2 =
        executeQuery(Index.ACCOUNT.ppl("where firstname == 'Amber' | fields firstname"));
    verifyDataRows(result2, rows("Amber"));
  }

  @Test
  public void testDoubleEqualWithSpecialCharacters() throws IOException {
    // Test == with strings containing special characters
    JSONObject result =
        executeQuery(
            Index.ACCOUNT.ppl("where email == 'amberduke@pyrami.com' | fields firstname, email"));
    verifyDataRows(result, rows("Amber", "amberduke@pyrami.com"));
  }

  @Test
  public void testDoubleEqualWithoutSpaces() throws IOException {
    // Test that == works without spaces
    JSONObject result1 = executeQuery(Index.ACCOUNT.ppl("where age==32 | stats count() as total"));

    JSONObject result2 =
        executeQuery(Index.ACCOUNT.ppl("where age == 32 | stats count() as total"));

    // Both should return same count
    assertEquals(
        result1.getJSONArray("datarows").getJSONArray(0).getLong(0),
        result2.getJSONArray("datarows").getJSONArray(0).getLong(0));
  }
}
