/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_STRINGS;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

public class TextFunctionIT extends PPLIntegTestCase {
  @Override
  public void init() throws IOException {
    loadIndex(Index.BANK);
    loadIndex(Index.BANK_WITH_STRING_VALUES);
  }

  void verifyQuery(
      String command,
      String initialArgs,
      String additionalArgs,
      String outputRow1,
      String outputRow2,
      String outputRow3)
      throws IOException {
    String query =
        String.format(
            "source=%s | eval f=%s(%sname%s) | fields f",
            TEST_INDEX_STRINGS, command, initialArgs, additionalArgs);
    JSONObject result = executeQuery(query);
    verifySchema(result, schema("f", null, "string"));
    verifyDataRows(result, rows(outputRow1), rows(outputRow2), rows(outputRow3));
  }

  void verifyQuery(
      String command,
      String initialArgs,
      String additionalArgs,
      Integer outputRow1,
      Integer outputRow2,
      Integer outputRow3)
      throws IOException {
    String query =
        String.format(
            "source=%s | eval f=%s(%sname%s) | fields f",
            TEST_INDEX_STRINGS, command, initialArgs, additionalArgs);
    JSONObject result = executeQuery(query);
    verifySchema(result, schema("f", null, "integer"));
    verifyDataRows(result, rows(outputRow1), rows(outputRow2), rows(outputRow3));
  }

  void verifyRegexQuery(String pattern, Integer outputRow1, Integer outputRow2, Integer outputRow3)
      throws IOException {
    String query =
        String.format(
            "source=%s | eval f=name regexp '%s' | fields f", TEST_INDEX_STRINGS, pattern);
    JSONObject result = executeQuery(query);
    verifySchema(result, schema("f", null, "integer"));
    verifyDataRows(result, rows(outputRow1), rows(outputRow2), rows(outputRow3));
  }

  @Test
  public void testRegexp() throws IOException {
    verifyRegexQuery("hello", 1, 0, 0);
    verifyRegexQuery(".*", 1, 1, 1);
  }

  @Test
  public void testSubstr() throws IOException {
    verifyQuery("substr", "", ", 2", "ello", "orld", "elloworld");
    verifyQuery("substr", "", ", 2, 2", "el", "or", "el");
  }

  @Test
  public void testSubstring() throws IOException {
    verifyQuery("substring", "", ", 2", "ello", "orld", "elloworld");
    verifyQuery("substring", "", ", 2, 2", "el", "or", "el");
  }

  @Test
  public void testUpper() throws IOException {
    verifyQuery("upper", "", "", "HELLO", "WORLD", "HELLOWORLD");
  }

  @Test
  public void testLower() throws IOException {
    verifyQuery("lower", "", "", "hello", "world", "helloworld");
  }

  @Test
  public void testTrim() throws IOException {
    verifyQuery("trim", "", "", "hello", "world", "helloworld");
  }

  @Test
  public void testRight() throws IOException {
    verifyQuery("right", "", ", 3", "llo", "rld", "rld");
  }

  @Test
  public void testRtrim() throws IOException {
    verifyQuery("rtrim", "", "", "hello", "world", "helloworld");
  }

  @Test
  public void testLtrim() throws IOException {
    verifyQuery("ltrim", "", "", "hello", "world", "helloworld");
  }

  @Test
  public void testConcat() throws IOException {
    verifyQuery(
        "concat",
        "",
        ", 'there', 'all', '!'",
        "hellothereall!",
        "worldthereall!",
        "helloworldthereall!");
  }

  @Test
  public void testConcat_ws() throws IOException {
    verifyQuery(
        "concat_ws", "',', ", ", 'there'", "hello,there", "world,there", "helloworld,there");
  }

  @Test
  public void testLength() throws IOException {
    verifyQuery("length", "", "", 5, 5, 10);
  }

  @Test
  public void testStrcmp() throws IOException {
    verifyQuery("strcmp", "", ", 'world'", -1, 0, -1);
  }

  @Test
  public void testLeft() throws IOException {
    verifyQuery("left", "", ", 3", "hel", "wor", "hel");
  }

  @Test
  public void testAscii() throws IOException {
    verifyQuery("ascii", "", "", 104, 119, 104);
  }

  @Test
  public void testLocate() throws IOException {
    verifyQuery("locate", "'world', ", "", 0, 1, 6);
    verifyQuery("locate", "'world', ", ", 2", 0, 0, 6);
  }

  @Test
  public void testReplace() throws IOException {
    verifyQuery(
        "replace", "", ", 'world', ' opensearch'", "hello", " opensearch", "hello opensearch");
  }

  @Test
  public void testReverse() throws IOException {
    verifyQuery("reverse", "", "", "olleh", "dlrow", "dlrowolleh");
  }
}
