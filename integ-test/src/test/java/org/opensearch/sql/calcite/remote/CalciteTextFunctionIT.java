/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_STRINGS;
import static org.opensearch.sql.util.MatcherUtils.*;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ppl.TextFunctionIT;

public class CalciteTextFunctionIT extends TextFunctionIT {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
  }

  @Test
  public void testRegexpReplace() throws Exception {
    // Test regexp_replace with pattern that matches substring
    String query1 =
        String.format(
            "source=%s | eval f=regexp_replace(name, 'ell', '\1') | fields f", TEST_INDEX_STRINGS);
    JSONObject result1 = executeQuery(query1);
    verifySchema(result1, schema("f", null, "string"));
    verifyDataRows(result1, rows("h\1o"), rows("world"), rows("h\1oworld"));

    // Test regexp_replace with pattern for beginning of string
    String query2 =
        String.format(
            "source=%s | eval f=regexp_replace(name, '^he', '\1') | fields f", TEST_INDEX_STRINGS);
    JSONObject result2 = executeQuery(query2);
    verifySchema(result2, schema("f", null, "string"));
    verifyDataRows(result2, rows("\1llo"), rows("world"), rows("\1lloworld"));

    // Test regexp_replace with pattern for end of string
    String query3 =
        String.format(
            "source=%s | eval f=regexp_replace(name, 'ld$', '\1') | fields f", TEST_INDEX_STRINGS);
    JSONObject result3 = executeQuery(query3);
    verifySchema(result3, schema("f", null, "string"));
    verifyDataRows(result3, rows("hello"), rows("wor\1"), rows("hellowor\1"));

    // Test regexp_replace with complex pattern
    String query4 =
        String.format(
            "source=%s | eval f=regexp_replace(name, '[hw]o.*d', '\1') | fields f",
            TEST_INDEX_STRINGS);
    JSONObject result4 = executeQuery(query4);
    verifySchema(result4, schema("f", null, "string"));
    verifyDataRows(result4, rows("hello"), rows("\1"), rows("hello\1"));
  }

  @Test
  public void testRegexMatch() throws IOException {
    // be compatible with old one
    String query =
        String.format("source=%s | eval f=regex_match(name, 'ell') | fields f", TEST_INDEX_STRINGS);
    JSONObject result1 = executeQuery(query);
    verifySchema(result1, schema("f", null, "boolean"));
    verifyDataRows(result1, rows(true), rows(false), rows(true));
  }

  @Test
  public void testRegexpMatch() throws IOException {
    // Test regexp_match with pattern that matches substring
    String query1 =
        String.format(
            "source=%s | eval f=regexp_match(name, 'ell') | fields f", TEST_INDEX_STRINGS);
    JSONObject result1 = executeQuery(query1);
    verifySchema(result1, schema("f", null, "boolean"));
    verifyDataRows(result1, rows(true), rows(false), rows(true));

    // Test regexp_match with pattern for beginning of string
    String query2 =
        String.format(
            "source=%s | eval f=regexp_match(name, '^he') | fields f", TEST_INDEX_STRINGS);
    JSONObject result2 = executeQuery(query2);
    verifySchema(result2, schema("f", null, "boolean"));
    verifyDataRows(result2, rows(true), rows(false), rows(true));

    // Test regexp_match with pattern for end of string
    String query3 =
        String.format(
            "source=%s | eval f=regexp_match(name, 'ld$') | fields f", TEST_INDEX_STRINGS);
    JSONObject result3 = executeQuery(query3);
    verifySchema(result3, schema("f", null, "boolean"));
    verifyDataRows(result3, rows(false), rows(true), rows(true));

    // Test regexp_match with complex pattern
    String query4 =
        String.format(
            "source=%s | eval f=regexp_match(name, '[hw]o.*d') | fields f", TEST_INDEX_STRINGS);
    JSONObject result4 = executeQuery(query4);
    verifySchema(result4, schema("f", null, "boolean"));
    verifyDataRows(result4, rows(false), rows(true), rows(true));
  }

  @Test
  public void testRegexpMatchWithWhereClause() throws IOException {
    // Test filtering with regexp_match - find strings containing 'ell'
    String query1 =
        String.format(
            "source=%s | where regexp_match(name, 'ell') | fields name", TEST_INDEX_STRINGS);
    JSONObject result1 = executeQuery(query1);
    verifySchema(result1, schema("name", null, "string"));
    verifyDataRows(result1, rows("hello"), rows("helloworld"));

    // Test filtering with regexp_match - find strings starting with 'h'
    String query2 =
        String.format(
            "source=%s | where regexp_match(name, '^h') | fields name", TEST_INDEX_STRINGS);
    JSONObject result2 = executeQuery(query2);
    verifySchema(result2, schema("name", null, "string"));
    verifyDataRows(result2, rows("hello"), rows("helloworld"));

    // Test filtering with regexp_match - find strings ending with 'ld'
    String query3 =
        String.format(
            "source=%s | where regexp_match(name, 'ld$') | fields name", TEST_INDEX_STRINGS);
    JSONObject result3 = executeQuery(query3);
    verifySchema(result3, schema("name", null, "string"));
    verifyDataRows(result3, rows("world"), rows("helloworld"));

    // Test NOT regexp_match - find strings NOT containing 'o'
    String query4 =
        String.format(
            "source=%s | where NOT regexp_match(name, 'o') | fields name", TEST_INDEX_STRINGS);
    JSONObject result4 = executeQuery(query4);
    verifySchema(result4, schema("name", null, "string"));
    // No rows should match since all strings contain 'o'
    verifyDataRows(result4);
  }

  @Test
  public void testRegexpMatchWithComplexPatterns() throws IOException {
    // Test regex with alternation - match strings containing either 'hello' or 'world'
    String query1 =
        String.format(
            "source=%s | where regexp_match(name, '(hello|world)') | fields name | head 3",
            TEST_INDEX_STRINGS);
    JSONObject result1 = executeQuery(query1);
    verifySchema(result1, schema("name", null, "string"));
    verifyDataRows(result1, rows("hello"), rows("world"), rows("helloworld"));

    // Test regex with word boundary - exact word match
    String query2 =
        String.format(
            "source=%s | where regexp_match(name, '\\\\bhello\\\\b') | fields name",
            TEST_INDEX_STRINGS);
    JSONObject result2 = executeQuery(query2);
    verifySchema(result2, schema("name", null, "string"));
    verifyDataRows(result2, rows("hello"));

    // Test regex with quantifiers - at least 5 characters
    String query3 =
        String.format(
            "source=%s | where regexp_match(name, '^.{5,}$') | fields name", TEST_INDEX_STRINGS);
    JSONObject result3 = executeQuery(query3);
    verifySchema(result3, schema("name", null, "string"));
    verifyDataRows(result3, rows("hello"), rows("world"), rows("helloworld"));
  }

  @Test
  public void testRegexpMatchInEvalWithConditions() throws IOException {
    // Test regexp_match in IF condition
    String query1 =
        String.format(
            "source=%s | eval category = if(regexp_match(name, '^h'), 'starts_with_h', 'other') |"
                + " fields name, category",
            TEST_INDEX_STRINGS);
    JSONObject result1 = executeQuery(query1);
    verifySchema(result1, schema("name", null, "string"), schema("category", null, "string"));
    verifyDataRows(
        result1,
        rows("hello", "starts_with_h"),
        rows("world", "other"),
        rows("helloworld", "starts_with_h"));

    // Test combining regexp_match results
    String query2 =
        String.format(
            "source=%s | eval has_hello = regexp_match(name, 'hello'), has_world ="
                + " regexp_match(name, 'world') | where has_hello OR has_world | fields name,"
                + " has_hello, has_world",
            TEST_INDEX_STRINGS);
    JSONObject result2 = executeQuery(query2);
    verifySchema(
        result2,
        schema("name", null, "string"),
        schema("has_hello", null, "boolean"),
        schema("has_world", null, "boolean"));
    verifyDataRows(
        result2,
        rows("hello", true, false),
        rows("world", false, true),
        rows("helloworld", true, true));
  }
}
