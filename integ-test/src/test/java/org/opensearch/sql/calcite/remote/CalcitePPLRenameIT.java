/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_STATE_COUNTRY;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifyErrorMessageContains;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;
import static org.opensearch.sql.util.MatcherUtils.verifySchemaInOrder;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.sql.ppl.PPLIntegTestCase;

public class CalcitePPLRenameIT extends PPLIntegTestCase {

  public void init() throws Exception {
    super.init();
    enableCalcite();

    loadIndex(Index.STATE_COUNTRY);
  }

  @Test
  public void testRename() throws IOException {
    JSONObject result =
        executeQuery(
            String.format("source = %s | rename age as renamed_age", TEST_INDEX_STATE_COUNTRY));
    verifySchema(
        result,
        schema("name", "string"),
        schema("renamed_age", "int"),
        schema("state", "string"),
        schema("country", "string"),
        schema("year", "int"),
        schema("month", "int"));
  }

  @Test
  public void testRefRenamedField() {
    Throwable e =
      assertThrowsWithReplace(
        IllegalArgumentException.class,
        () ->
          executeQuery(
            String.format(
              "source = %s | rename age as renamed_age | fields age",
              TEST_INDEX_STATE_COUNTRY)));
    verifyNotFoundAndInputFields(e.getMessage(),
            "field [age] not found; input fields are: [country, month, year, name, state, renamed_age, _id, _index, _score, _maxscore, _sort, _routing]");
  }

  @Test
  public void testRenameToMetaField() throws IOException {
    Throwable e =
        assertThrowsWithReplace(
            IllegalArgumentException.class,
            () ->
                executeQuery(
                    String.format("source = %s | rename name as _id", TEST_INDEX_STATE_COUNTRY)));
    verifyErrorMessageContains(e, "Cannot use metadata field [_id] in Rename command.");

    // Test rename to _ID, which is allowed as metadata fields name is case-sensitive
    JSONObject result =
        executeQuery(String.format("source = %s | rename age as _ID", TEST_INDEX_STATE_COUNTRY));
    verifySchema(
        result,
        schema("name", "string"),
        schema("_ID", "int"),
        schema("state", "string"),
        schema("country", "string"),
        schema("year", "int"),
        schema("month", "int"));
  }

  @Test
  public void testMultipleRename() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source = %s | rename name as renamed_name, country as renamed_country"
                    + "| fields renamed_name, age, renamed_country",
                TEST_INDEX_STATE_COUNTRY));
    verifySchema(
        result,
        schema("renamed_name", "string"),
        schema("age", "int"),
        schema("renamed_country", "string"));
    verifyDataRows(
        result,
        rows("Jake", 70, "USA"),
        rows("Hello", 30, "USA"),
        rows("John", 25, "Canada"),
        rows("Jane", 20, "Canada"));
  }

  @Test
  public void testRenameInAgg() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source = %s | rename age as user_age | stats avg(user_age) by country",
                TEST_INDEX_STATE_COUNTRY));
    verifySchemaInOrder(result, schema("avg(user_age)", "double"), schema("country", "string"));
    verifyDataRows(result, rows(22.5, "Canada"), rows(50.0, "USA"));
  }

  @Test
  public void testMultipleRenameWithBackticks() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source = %s |  rename name as `renamed_name`, country as `renamed_country`"
                    + "| fields `renamed_name`, `age`, `renamed_country`",
                TEST_INDEX_STATE_COUNTRY));
    verifySchema(
        result,
        schema("renamed_name", "string"),
        schema("age", "int"),
        schema("renamed_country", "string"));
    verifyDataRows(
        result,
        rows("Jake", 70, "USA"),
        rows("Hello", 30, "USA"),
        rows("John", 25, "Canada"),
        rows("Jane", 20, "Canada"));
  }

  @Test
  public void testRenameWithBackticksInAgg() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source = %s | rename age as `user_age` | stats avg(`user_age`) by country",
                TEST_INDEX_STATE_COUNTRY));
    verifySchemaInOrder(result, schema("avg(`user_age`)", "double"), schema("country", "string"));
    verifyDataRows(result, rows(22.5, "Canada"), rows(50.0, "USA"));
  }

  @Test
  public void testRenameWildcardFields() throws IOException {
    JSONObject result =
        executeQuery(String.format("source = %s | fields name, country, state, month, year, age | rename *ame as *AME", TEST_INDEX_STATE_COUNTRY));
    verifySchema(
        result,
        schema("nAME", "string"),
        schema("age", "int"),
        schema("state", "string"),
        schema("country", "string"),
        schema("year", "int"),
        schema("month", "int"));
    verifyStandardDataRows(result);
  }

  @Test
  public void testRenameMultipleWildcardFields() throws IOException {
    JSONObject result =
        executeQuery(String.format("source = %s | fields name, country, state, month, year, age | rename *nt* as *NT*", TEST_INDEX_STATE_COUNTRY));
    verifySchema(
        result,
        schema("name", "string"),
        schema("age", "int"),
        schema("state", "string"),
        schema("couNTry", "string"),
        schema("year", "int"),
        schema("moNTh", "int"));
    verifyStandardDataRows(result);
  }

  @Test
  public void testRenameWildcardPrefix() throws IOException {
    JSONObject result =
        executeQuery(String.format("source = %s | fields name, country, state, month, year, age | rename *me as new_*", TEST_INDEX_STATE_COUNTRY));
    verifySchema(
        result,
        schema("new_na", "string"),
        schema("age", "int"),
        schema("state", "string"),
        schema("country", "string"),
        schema("year", "int"),
        schema("month", "int"));
    verifyStandardDataRows(result);
  }

  @Test
  public void testRenameFullWildcard() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source = %s | fields name, age | rename * as old_*", TEST_INDEX_STATE_COUNTRY));
    verifySchema(result, schema("old_name", "string"), schema("old_age", "int"));
    verifyDataRows(result, rows("Jake", 70), rows("Hello", 30), rows("John", 25), rows("Jane", 20));
  }

  @Test
  public void testRenameMultipleWildcards() throws IOException {
    JSONObject result =
        executeQuery(
            String.format("source = %s | fields name, country, state, month, year, age | rename m*n*h as M*N*H", TEST_INDEX_STATE_COUNTRY));
    verifySchema(
        result,
        schema("name", "string"),
        schema("age", "int"),
        schema("state", "string"),
        schema("country", "string"),
        schema("year", "int"),
        schema("MoNtH", "int"));
    verifyStandardDataRows(result);
  }

  @Test
  public void testMultipleRenameWithWildcard() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source = %s | fields name, age | rename name as user_name | rename user_name as"
                    + " final_name",
                TEST_INDEX_STATE_COUNTRY));
    verifySchema(result, schema("final_name", "string"), schema("age", "int"));
    verifyDataRows(result, rows("Jake", 70), rows("Hello", 30), rows("John", 25), rows("Jane", 20));
  }

  @Test
  public void testChainedRename() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source = %s | fields name, age | rename name as user_name, user_name as"
                    + " final_name",
                TEST_INDEX_STATE_COUNTRY));
    verifySchema(result, schema("final_name", "string"), schema("age", "int"));
    verifyDataRows(result, rows("Jake", 70), rows("Hello", 30), rows("John", 25), rows("Jane", 20));
  }

  @Test
  public void testChainedRenameWithWildcard() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source = %s | fields name, age | rename *ame as *_ame, *_ame as *_AME",
                TEST_INDEX_STATE_COUNTRY));
    verifySchema(result, schema("n_AME", "string"), schema("age", "int"));
    verifyDataRows(result, rows("Jake", 70), rows("Hello", 30), rows("John", 25), rows("Jane", 20));
  }

  @Test
  public void testRenamingToExistingField() throws IOException {
    JSONObject result =
        executeQuery(String.format("source = %s | fields name, country, state, month, year | rename name as age", TEST_INDEX_STATE_COUNTRY));
    verifySchema(
        result,
        schema("age", "string"),
        schema("state", "string"),
        schema("country", "string"),
        schema("year", "int"),
        schema("month", "int"));
    verifyDataRows(
        result,
        rows("Jake", "USA", "California", 4, 2023),
        rows("Hello", "USA", "New York", 4, 2023),
        rows("John", "Canada", "Ontario", 4, 2023),
        rows("Jane", "Canada", "Quebec", 4, 2023));
  }

  @Test
  public void testRenamingNonExistentField() throws IOException {
    JSONObject result =
        executeQuery(
            String.format("source = %s | fields name, country, state, month, year, age | rename none as nothing", TEST_INDEX_STATE_COUNTRY));
    verifySchema(
        result,
        schema("name", "string"),
        schema("age", "int"),
        schema("state", "string"),
        schema("country", "string"),
        schema("year", "int"),
        schema("month", "int"));
    verifyStandardDataRows(result);
  }

  @Test
  public void testRenamingNonExistentFieldToExistingField() throws IOException {
    JSONObject result =
        executeQuery(String.format("source = %s | fields name, country, state, month, year | rename none as age", TEST_INDEX_STATE_COUNTRY));
    verifySchema(
        result,
        schema("name", "string"),
        schema("state", "string"),
        schema("country", "string"),
        schema("year", "int"),
        schema("month", "int"));
    verifyDataRows(
        result,
        rows("Jake", "USA", "California", 4, 2023),
        rows("Hello", "USA", "New York", 4, 2023),
        rows("John", "Canada", "Ontario", 4, 2023),
        rows("Jane", "Canada", "Quebec", 4, 2023));
  }

  @Test
  public void testWildcardPatternDifferentCounts() {
    Throwable e =
        assertThrowsWithReplace(
            IllegalArgumentException.class,
            () ->
                executeQuery(
                    String.format("source = %s | rename *a*e as *new", TEST_INDEX_STATE_COUNTRY)));
    verifyErrorMessageContains(e, "Source and target patterns have different wildcard counts");
  }

  @Test
  public void testRenameSameField() throws IOException {
    JSONObject result =
        executeQuery(String.format("source = %s | fields name, country, state, month, year, age | rename age as age", TEST_INDEX_STATE_COUNTRY));
    verifySchema(
        result,
        schema("name", "string"),
        schema("age", "int"),
        schema("state", "string"),
        schema("country", "string"),
        schema("year", "int"),
        schema("month", "int"));
    verifyStandardDataRows(result);
  }

  @Test
  public void testMultipleRenameWithoutComma() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source = %s | fields name, country, state, month, year, age | rename name as user_name age as user_age country as location",
                TEST_INDEX_STATE_COUNTRY));
    verifySchema(
        result,
        schema("user_name", "string"),
        schema("user_age", "int"),
        schema("state", "string"),
        schema("location", "string"),
        schema("year", "int"),
        schema("month", "int"));
    verifyStandardDataRows(result);
  }

  @Test
  public void testRenameMixedCommaAndSpace() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source = %s | fields name, country, state, month, year, age | rename name as user_name, age as user_age country as location",
                TEST_INDEX_STATE_COUNTRY));
    verifySchema(
        result,
        schema("user_name", "string"),
        schema("user_age", "int"),
        schema("state", "string"),
        schema("location", "string"),
        schema("year", "int"),
        schema("month", "int"));
    verifyStandardDataRows(result);
  }

  private void verifyStandardDataRows(JSONObject result) {
    verifyDataRows(
        result,
        rows("Jake", "USA", "California", 4, 2023, 70),
        rows("Hello", "USA", "New York", 4, 2023, 30),
        rows("John", "Canada", "Ontario", 4, 2023, 25),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20));
  }

  /**
   * Verify the error message for a not found field and the input fields.
   * This helper method makes tests insensitive to the order of input fields.
   *
   * @param actual the actual error message from the exception
   * @param expected the expected error message format
   */
  private static void verifyNotFoundAndInputFields(String actual, String expected) {
    String notFoundFieldPattern = "field \\[(.*?)\\] not found";
    String inputFieldsPattern = "input fields are: \\[(.*?)\\]";
    String actualUnfoundField = extractByPattern(actual, notFoundFieldPattern);
    String expectedUnfoundField = extractByPattern(expected, notFoundFieldPattern);
    // splitIntoSet makes it order-insensitive
    Set<String> actualInputFields = splitIntoSet(extractByPattern(actual, inputFieldsPattern));
    Set<String> expectedInputFields = splitIntoSet(extractByPattern(expected, inputFieldsPattern));
    org.hamcrest.MatcherAssert.assertThat("Not found field mismatch",
      actualUnfoundField, org.hamcrest.Matchers.equalTo(expectedUnfoundField));
    org.hamcrest.MatcherAssert.assertThat("Input fields mismatch",
      actualInputFields, org.hamcrest.Matchers.equalTo(expectedInputFields));
  }

    /**
    * Split a string of comma-separated fields into a Set.
    *
    * @param str the string representation of the list
    * @return a HashSet containing the items from the list
    */
  private static Set<String> splitIntoSet(String str) {
    if (str.isEmpty()) {
      return new HashSet<>();
    }
    return Arrays.stream(str.split(",")).map(String::trim).collect(Collectors.toSet());
  }

  /**
   * Extracts a substring from the input string that matches the given pattern.
   *
   * @param str the input string from which to extract the substring
   * @param pattern the regex pattern to match
   * @return the extracted substring, or an empty string if no match is found
   */
  private static String extractByPattern(String str, String pattern) {
    Pattern compiledPattern = Pattern.compile(pattern);
    Matcher matcher = compiledPattern.matcher(str);
    String extracted;
    if (matcher.find()) {
      extracted = matcher.group(1);
    } else {
      extracted = "";
    }
    return extracted;
  }
}
