/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.*;
import static org.opensearch.sql.util.MatcherUtils.*;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import org.hamcrest.Matcher;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.sql.common.antlr.SyntaxCheckException;
import org.opensearch.sql.legacy.TestUtils;
import org.opensearch.sql.ppl.PPLIntegTestCase;

public class CalciteReplaceCommandIT extends PPLIntegTestCase {
  private static final String WIDE_INDEX = "test_replace_wide";
  private static final int WIDE_ATTRIBUTES = 1_000;

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

    // Match by column name — analytics-engine and v2 paths return columns in different orders.
    verifyDataRowsByColumn(
        result,
        rowOf(
            "name",
            "Jake",
            "country",
            "United States",
            "state",
            "California",
            "month",
            4,
            "year",
            2023,
            "age",
            70),
        rowOf(
            "name",
            "Hello",
            "country",
            "United States",
            "state",
            "New York",
            "month",
            4,
            "year",
            2023,
            "age",
            30),
        rowOf(
            "name", "John", "country", "Canada", "state", "Ontario", "month", 4, "year", 2023,
            "age", 25),
        rowOf(
            "name", "Joseph", "country", "Canada", "state", "Quebec", "month", 4, "year", 2023,
            "age", 20));
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

    verifyDataRowsByColumn(
        result,
        rowOf(
            "name",
            "Jake",
            "country",
            "",
            "state",
            "California",
            "month",
            4,
            "year",
            2023,
            "age",
            70),
        rowOf(
            "name",
            "Hello",
            "country",
            "",
            "state",
            "New York",
            "month",
            4,
            "year",
            2023,
            "age",
            30),
        rowOf(
            "name", "John", "country", "Canada", "state", "Ontario", "month", 4, "year", 2023,
            "age", 25),
        rowOf(
            "name", "Jane", "country", "Canada", "state", "Quebec", "month", 4, "year", 2023, "age",
            20));
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

    verifyDataRowsByColumn(
        result,
        rowOf(
            "name",
            "Jake",
            "country",
            "United States",
            "state",
            "California",
            "month",
            4,
            "year",
            2023,
            "age",
            70),
        rowOf(
            "name",
            "Hello",
            "country",
            "United States",
            "state",
            "New York",
            "month",
            4,
            "year",
            2023,
            "age",
            30),
        rowOf(
            "name", "John", "country", "Canada", "state", "Ontario", "month", 4, "year", 2023,
            "age", 25),
        rowOf(
            "name", "Jane", "country", "Canada", "state", "Quebec", "month", 4, "year", 2023, "age",
            20));
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
    // Order-agnostic — analytics-engine and v2 paths emit the input-field list in different
    // orders (parquet preserves storage order, Lucene preserves _source iteration order).
    // Assert that the prefix and every expected field name appear somewhere in the message.
    verifyErrorMessageContains(e, "field [non_existent_field] not found; input fields are:");
    verifyErrorMessageContains(e, "name");
    verifyErrorMessageContains(e, "country");
    verifyErrorMessageContains(e, "state");
    verifyErrorMessageContains(e, "month");
    verifyErrorMessageContains(e, "year");
    verifyErrorMessageContains(e, "age");
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

    verifyDataRowsByColumn(
        result,
        rowOf(
            "name",
            "Jake",
            "country",
            "United States",
            "state",
            "California",
            "month",
            4,
            "year",
            2023,
            "age",
            70),
        rowOf(
            "name",
            "Hello",
            "country",
            "United States",
            "state",
            "New York",
            "month",
            4,
            "year",
            2023,
            "age",
            30),
        rowOf(
            "name", "John", "country", "CA", "state", "Ontario", "month", 4, "year", 2023, "age",
            25),
        rowOf(
            "name", "Jane", "country", "CA", "state", "Quebec", "month", 4, "year", 2023, "age",
            20));
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

  /** Replace on an index with many dynamic attribute fields keeps every other column's value. */
  @Test
  public void testReplaceOnWideIndex() throws IOException {
    createWideIndexIfAbsent();
    String last = "attributes.f" + (WIDE_ATTRIBUTES - 1);
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | replace 's1' WITH 'S1' IN status | replace 'x*' WITH 'y*' IN %s |"
                    + " fields code, status, attributes.f1, %s",
                WIDE_INDEX, last, last));
    verifySchema(
        result,
        schema("code", null, "bigint"),
        schema("status", null, "string"),
        schema("attributes.f1", null, "string"),
        schema(last, null, "string"));
    verifyDataRows(result, rows(201, "S1", "v1", "y" + (WIDE_ATTRIBUTES - 1)));
  }

  private void createWideIndexIfAbsent() throws IOException {
    if (TestUtils.isIndexExist(client(), WIDE_INDEX)) {
      return;
    }
    JSONObject attributes = new JSONObject();
    for (int i = 0; i < WIDE_ATTRIBUTES; i++) {
      attributes.put("f" + i, new JSONObject().put("type", "keyword"));
    }
    JSONObject properties =
        new JSONObject()
            .put("@timestamp", new JSONObject().put("type", "date"))
            .put("status", new JSONObject().put("type", "keyword"))
            .put("code", new JSONObject().put("type", "long"))
            .put("attributes", new JSONObject().put("properties", attributes));
    JSONObject fieldLimit =
        new JSONObject().put("total_fields", new JSONObject().put("limit", WIDE_ATTRIBUTES + 100));
    JSONObject body =
        new JSONObject()
            .put(
                "settings",
                new JSONObject().put("index", new JSONObject().put("mapping", fieldLimit)))
            .put("mappings", new JSONObject().put("properties", properties));
    TestUtils.createIndexByRestClient(client(), WIDE_INDEX, body.toString());

    Request doc = TestUtils.seedDocRequest(WIDE_INDEX, "1");
    doc.setJsonEntity(
        String.format(
            "{\"@timestamp\": \"2026-11-09T00:00:00Z\", \"status\": \"s1\", \"code\": 201,"
                + " \"attributes\": {\"f1\": \"v1\", \"f%d\": \"x%d\"}}",
            WIDE_ATTRIBUTES - 1, WIDE_ATTRIBUTES - 1));
    client().performRequest(doc);
  }

  /**
   * Build a {@code column -> value} map from interleaved varargs ({@code key1, val1, key2, val2,
   * ...}). Preserves insertion order so the expected-row mapping reads naturally at the call site.
   */
  private static Map<String, Object> rowOf(Object... pairs) {
    if (pairs.length % 2 != 0) {
      throw new IllegalArgumentException("rowOf expects an even number of args (key, value, ...)");
    }
    Map<String, Object> row = new LinkedHashMap<>();
    for (int i = 0; i < pairs.length; i += 2) {
      row.put((String) pairs[i], pairs[i + 1]);
    }
    return row;
  }

  /**
   * Match expected rows against the response by column name, ignoring the response's column
   * emission order. The two paths the analytics-engine route can take return columns in different
   * orders (parquet preserves storage order, the v2 / Lucene path preserves {@code _source}
   * iteration order), and either is valid given the contract {@code verifySchema} declares (set
   * equality on column names). To avoid baking either order into the test, this helper reorders
   * each expected row to match whatever column order the response actually returned.
   *
   * <p>Mirrors the helper in {@code CalcitePPLRenameIT} (commit 59c728b) — same pattern applied to
   * PPL {@code replace} command tests.
   */
  @SafeVarargs
  @SuppressWarnings("varargs")
  private final void verifyDataRowsByColumn(
      JSONObject result, Map<String, Object>... expectedRows) {
    JSONArray schema = result.getJSONArray("schema");
    int n = schema.length();
    String[] columnOrder = new String[n];
    for (int i = 0; i < n; i++) {
      columnOrder[i] = schema.getJSONObject(i).getString("name");
    }
    @SuppressWarnings({"unchecked", "rawtypes"})
    Matcher<JSONArray>[] rowMatchers = new Matcher[expectedRows.length];
    for (int r = 0; r < expectedRows.length; r++) {
      Object[] reordered = new Object[n];
      for (int c = 0; c < n; c++) {
        if (!expectedRows[r].containsKey(columnOrder[c])) {
          throw new IllegalArgumentException(
              "Expected row at index "
                  + r
                  + " is missing canonical value for response column ["
                  + columnOrder[c]
                  + "]; provided keys: "
                  + expectedRows[r].keySet());
        }
        reordered[c] = expectedRows[r].get(columnOrder[c]);
      }
      rowMatchers[r] = rows(reordered);
    }
    verifyDataRows(result, rowMatchers);
  }
}
