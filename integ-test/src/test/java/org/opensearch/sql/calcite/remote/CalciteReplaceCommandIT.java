/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.*;
import static org.opensearch.sql.util.MatcherUtils.*;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.sql.common.antlr.SyntaxCheckException;
import org.opensearch.sql.ppl.PPLIntegTestCase;

public class CalciteReplaceCommandIT extends PPLIntegTestCase {

  public void init() throws Exception {
    super.init();
    enableCalcite();
    disallowCalciteFallback();
    loadIndex(Index.STATE_COUNTRY);
  }

  @Test
  public void testReplaceWithFields() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source = %s | replace 'USA' WITH 'United States' IN country | fields name, age,"
                    + " country",
                TEST_INDEX_STATE_COUNTRY));

    verifySchema(
        result, schema("name", "string"), schema("age", "int"), schema("country", "string"));

    verifyDataRows(
        result,
        rows("Jake", 70, "United States"),
        rows("Hello", 30, "United States"),
        rows("John", 25, "Canada"),
        rows("Jane", 20, "Canada"));
  }

  @Test
  public void testMultipleReplace() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source = %s | replace 'USA' WITH 'United States' IN country | replace 'Jane' WITH"
                    + " 'Joseph' IN name",
                TEST_INDEX_STATE_COUNTRY));

    verifySchema(
        result,
        schema("name", "string"),
        schema("country", "string"),
        schema("state", "string"),
        schema("month", "int"),
        schema("year", "int"),
        schema("age", "int"));

    verifyDataRows(
        result,
        rows("Jake", "United States", "California", 4, 2023, 70),
        rows("Hello", "United States", "New York", 4, 2023, 30),
        rows("John", "Canada", "Ontario", 4, 2023, 25),
        rows("Joseph", "Canada", "Quebec", 4, 2023, 20));
  }

  @Test
  public void testReplaceWithSort() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source = %s | replace 'US' WITH 'United States' IN country | sort country",
                TEST_INDEX_STATE_COUNTRY));

    verifySchema(
        result,
        schema("name", "string"),
        schema("age", "int"),
        schema("state", "string"),
        schema("country", "string"),
        schema("year", "int"),
        schema("month", "int"));
  }

  @Test
  public void testReplaceWithWhereClause() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source = %s | where country = 'US' | replace 'US' WITH 'United States' IN country",
                TEST_INDEX_STATE_COUNTRY));

    verifySchema(
        result,
        schema("name", "string"),
        schema("age", "int"),
        schema("state", "string"),
        schema("country", "string"),
        schema("year", "int"),
        schema("month", "int"));
  }

  @Test
  public void testEmptyStringReplacement() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source = %s | replace 'USA' WITH '' IN country", TEST_INDEX_STATE_COUNTRY));

    verifySchema(
        result,
        schema("name", "string"),
        schema("country", "string"),
        schema("state", "string"),
        schema("month", "int"),
        schema("year", "int"),
        schema("age", "int"));

    verifyDataRows(
        result,
        rows("Jake", "", "California", 4, 2023, 70),
        rows("Hello", "", "New York", 4, 2023, 30),
        rows("John", "Canada", "Ontario", 4, 2023, 25),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20));
  }

  @Test
  public void testMultipleFieldsInClause() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source = %s | replace 'USA' WITH 'United States' IN country,state",
                TEST_INDEX_STATE_COUNTRY));

    verifySchema(
        result,
        schema("name", "string"),
        schema("country", "string"),
        schema("state", "string"),
        schema("month", "int"),
        schema("year", "int"),
        schema("age", "int"));

    verifyDataRows(
        result,
        rows("Jake", "United States", "California", 4, 2023, 70),
        rows("Hello", "United States", "New York", 4, 2023, 30),
        rows("John", "Canada", "Ontario", 4, 2023, 25),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20));
  }

  @Test
  public void testReplaceNonExistentField() {
    Throwable e =
        assertThrowsWithReplace(
            IllegalArgumentException.class,
            () ->
                executeQuery(
                    String.format(
                        "source = %s | replace 'USA' WITH 'United States' IN non_existent_field",
                        TEST_INDEX_STATE_COUNTRY)));
    verifyErrorMessageContains(
        e,
        "field [non_existent_field] not found; input fields are: [name, country, state, month,"
            + " year, age, _id, _index, _score, _maxscore, _sort, _routing]");
  }

  @Test
  public void testReplaceAfterFieldRemoved() {
    Throwable e =
        assertThrowsWithReplace(
            IllegalArgumentException.class,
            () ->
                executeQuery(
                    String.format(
                        "source = %s | fields name, age | replace 'USA' WITH 'United States' IN"
                            + " country",
                        TEST_INDEX_STATE_COUNTRY)));
    verifyErrorMessageContains(e, "field [country] not found; input fields are: [name, age]");
  }

  @Test
  public void testMissingInClause() {
    Throwable e =
        assertThrowsWithReplace(
            SyntaxCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source = %s | replace 'USA' WITH 'United States'",
                        TEST_INDEX_STATE_COUNTRY)));

    verifyErrorMessageContains(e, "[<EOF>] is not a valid term at this part of the query");
    verifyErrorMessageContains(e, "Expecting tokens: 'IN'");
  }

  @Test
  public void testDuplicateFieldsInReplace() {
    Throwable e =
        assertThrowsWithReplace(
            IllegalArgumentException.class,
            () ->
                executeQuery(
                    String.format(
                        "source = %s | replace 'USA' WITH 'United States' IN country, state,"
                            + " country",
                        TEST_INDEX_STATE_COUNTRY)));
    verifyErrorMessageContains(e, "Duplicate fields [country] in Replace command");
  }

  @Test
  public void testNonStringLiteralPattern() {
    Throwable e =
        assertThrowsWithReplace(
            SyntaxCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source = %s | replace 23 WITH 'test' IN field1",
                        TEST_INDEX_STATE_COUNTRY)));
    verifyErrorMessageContains(e, "is not a valid term at this part of the query");
    verifyErrorMessageContains(e, "Expecting tokens: DQUOTA_STRING, SQUOTA_STRING");
  }

  @Test
  public void testNonStringLiteralReplacement() {
    Throwable e =
        assertThrowsWithReplace(
            SyntaxCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source = %s | replace 'test' WITH 45 IN field1",
                        TEST_INDEX_STATE_COUNTRY)));
    verifyErrorMessageContains(e, "is not a valid term at this part of the query");
    verifyErrorMessageContains(e, "Expecting tokens: DQUOTA_STRING, SQUOTA_STRING");
  }

  @Test
  public void testMultiplePairsInSingleCommand() throws IOException {
    // Test replacing multiple patterns in a single command
    JSONObject result =
        executeQuery(
            String.format(
                "source = %s | replace 'USA' WITH 'United States', 'Canada' WITH 'CA' IN country",
                TEST_INDEX_STATE_COUNTRY));

    verifySchema(
        result,
        schema("name", "string"),
        schema("country", "string"),
        schema("state", "string"),
        schema("month", "int"),
        schema("year", "int"),
        schema("age", "int"));

    verifyDataRows(
        result,
        rows("Jake", "United States", "California", 4, 2023, 70),
        rows("Hello", "United States", "New York", 4, 2023, 30),
        rows("John", "CA", "Ontario", 4, 2023, 25),
        rows("Jane", "CA", "Quebec", 4, 2023, 20));
  }

  @Test
  public void testMultiplePairsSequentialApplication() throws IOException {
    // Test that replacements are applied sequentially (order matters)
    // If we have "Ontario" WITH "ON", "ON" WITH "Ontario Province"
    // then "Ontario" becomes "ON" first, then that "ON" becomes "Ontario Province"
    JSONObject result =
        executeQuery(
            String.format(
                "source = %s | replace 'Ontario' WITH 'ON', 'ON' WITH 'Ontario Province' IN state"
                    + " | fields name, state",
                TEST_INDEX_STATE_COUNTRY));

    verifySchema(result, schema("name", "string"), schema("state", "string"));

    verifyDataRows(
        result,
        rows("Jake", "California"),
        rows("Hello", "New York"),
        rows("John", "Ontario Province"),
        rows("Jane", "Quebec"));
  }

  @Test
  public void testWildcardReplace_suffixMatch() throws IOException {
    // Pattern "*ada" should match "Canada" and replace with "CA"
    JSONObject result =
        executeQuery(
            String.format(
                "source = %s | replace '*ada' WITH 'CA' IN country | fields name, country",
                TEST_INDEX_STATE_COUNTRY));

    verifySchema(result, schema("name", "string"), schema("country", "string"));

    verifyDataRows(
        result, rows("Jake", "USA"), rows("Hello", "USA"), rows("John", "CA"), rows("Jane", "CA"));
  }

  @Test
  public void testWildcardReplace_prefixMatch() throws IOException {
    // Pattern "US*" should match "USA" and replace with "United States"
    JSONObject result =
        executeQuery(
            String.format(
                "source = %s | replace 'US*' WITH 'United States' IN country | fields name,"
                    + " country",
                TEST_INDEX_STATE_COUNTRY));

    verifySchema(result, schema("name", "string"), schema("country", "string"));

    verifyDataRows(
        result,
        rows("Jake", "United States"),
        rows("Hello", "United States"),
        rows("John", "Canada"),
        rows("Jane", "Canada"));
  }

  @Test
  public void testWildcardReplace_multipleWildcards() throws IOException {
    // Pattern "* *" with replacement "*_*" should replace spaces with underscores
    JSONObject result =
        executeQuery(
            String.format(
                "source = %s | replace '* *' WITH '*_*' IN state | fields name, state",
                TEST_INDEX_STATE_COUNTRY));

    verifySchema(result, schema("name", "string"), schema("state", "string"));

    verifyDataRows(
        result,
        rows("Jake", "California"),
        rows("Hello", "New_York"),
        rows("John", "Ontario"),
        rows("Jane", "Quebec"));
  }

  @Test
  public void testWildcardReplace_symmetryMismatch_shouldFail() {
    // Pattern has 2 wildcards, replacement has 1 - should fail
    Throwable e =
        assertThrowsWithReplace(
            IllegalArgumentException.class,
            () ->
                executeQuery(
                    String.format(
                        "source = %s | replace '* *' WITH '*' IN state",
                        TEST_INDEX_STATE_COUNTRY)));
    verifyErrorMessageContains(e, "Wildcard count mismatch");
  }

  @Test
  public void testEscapeSequence_literalAsterisk() throws IOException {
    // Test matching literal asterisks in data using \* escape sequence
    JSONObject result =
        executeQuery(
            String.format(
                "source = %s | eval note = 'price: *sale*' | replace 'price: \\\\*sale\\\\*' WITH"
                    + " 'DISCOUNTED' IN note | fields note | head 1",
                TEST_INDEX_STATE_COUNTRY));

    verifySchema(result, schema("note", "string"));
    // Pattern "price: \*sale\*" matches literal asterisks, result should be "DISCOUNTED"
    verifyDataRows(result, rows("DISCOUNTED"));
  }

  @Test
  public void testEscapeSequence_mixedEscapeAndWildcard() throws IOException {
    // Test combining escaped asterisks (literal) with wildcards (pattern matching)
    JSONObject result =
        executeQuery(
            String.format(
                "source = %s | eval label = 'file123.txt' | replace 'file*.*' WITH"
                    + " '\\\\**.*' IN label | fields label | head 1",
                TEST_INDEX_STATE_COUNTRY));

    verifySchema(result, schema("label", "string"));
    // Pattern "file*.*" captures "123" and "txt"
    // Replacement "\**.*" has escaped * (literal), then 2 wildcards, producing "*123.txt"
    verifyDataRows(result, rows("*123.txt"));
  }

  @Test
  public void testEscapeSequence_noMatchLiteral() throws IOException {
    // Test that escaped asterisk doesn't match as wildcard
    JSONObject result =
        executeQuery(
            String.format(
                "source = %s | eval test = 'fooXbar' | replace 'foo\\\\*bar' WITH 'matched' IN test"
                    + " | fields test | head 1",
                TEST_INDEX_STATE_COUNTRY));

    verifySchema(result, schema("test", "string"));
    // Pattern "foo\*bar" matches literal "foo*bar", not "fooXbar", so original value returned
    verifyDataRows(result, rows("fooXbar"));
  }
}
