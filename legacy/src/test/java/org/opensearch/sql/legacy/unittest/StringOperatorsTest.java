/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.unittest;

import static org.junit.Assert.assertTrue;

import org.junit.BeforeClass;
import org.junit.Test;
import org.opensearch.search.builder.SearchSourceBuilder.ScriptField;
import org.opensearch.sql.legacy.parser.ScriptFilter;
import org.opensearch.sql.legacy.parser.SqlParser;
import org.opensearch.sql.legacy.util.CheckScriptContents;

public class StringOperatorsTest {

  private static SqlParser parser;

  @BeforeClass
  public static void init() {
    parser = new SqlParser();
  }

  @Test
  public void substringTest() {
    String query =
        "SELECT substring(lastname, 2, 1) FROM accounts WHERE substring(lastname, 2, 1) = 'a'";

    ScriptField scriptField = CheckScriptContents.getScriptFieldFromQuery(query);
    assertTrue(
        CheckScriptContents.scriptContainsString(
            scriptField, "doc['lastname'].value.substring(1, end)"));

    ScriptFilter scriptFilter = CheckScriptContents.getScriptFilterFromQuery(query, parser);
    assertTrue(
        CheckScriptContents.scriptContainsString(
            scriptFilter, "doc['lastname'].value.substring(1, end)"));
  }

  @Test
  public void substringIndexOutOfBoundTest() {
    String query = "SELECT substring('sampleName', 0, 20) FROM accounts";
    ScriptField scriptField = CheckScriptContents.getScriptFieldFromQuery(query);
    assertTrue(
        CheckScriptContents.scriptContainsString(
            scriptField, "def end = (int) Math.min(0 + 20, 'sampleName'.length())"));
  }

  @Test
  public void lengthTest() {
    String query =
        "SELECT length(lastname) FROM accounts WHERE length(lastname) = 5";

    ScriptField scriptField = CheckScriptContents.getScriptFieldFromQuery(query);
    assertTrue(
        CheckScriptContents.scriptContainsString(scriptField, "doc['lastname'].value.length()"));

    ScriptFilter scriptFilter = CheckScriptContents.getScriptFilterFromQuery(query, parser);
    assertTrue(
        CheckScriptContents.scriptContainsString(scriptFilter, "doc['lastname'].value.length()"));
  }

  @Test
  public void replaceTest() {
    String query =
        "SELECT replace(lastname, 'a', 'A') FROM accounts WHERE replace(lastname, 'a', 'A') = 'aba'";

    ScriptField scriptField = CheckScriptContents.getScriptFieldFromQuery(query);
    assertTrue(
        CheckScriptContents.scriptContainsString(
            scriptField, "doc['lastname'].value.replace('a','A')"));

    ScriptFilter scriptFilter = CheckScriptContents.getScriptFilterFromQuery(query, parser);
    assertTrue(
        CheckScriptContents.scriptContainsString(
            scriptFilter, "doc['lastname'].value.replace('a','A')"));
  }

  @Test
  public void locateTest() {
    String query =
        "SELECT locate('a', lastname, 1) FROM accounts WHERE locate('a', lastname, 1) = 4";

    ScriptField scriptField = CheckScriptContents.getScriptFieldFromQuery(query);
    assertTrue(
        CheckScriptContents.scriptContainsString(
            scriptField, "doc['lastname'].value.indexOf('a',0)+1"));

    ScriptFilter scriptFilter = CheckScriptContents.getScriptFilterFromQuery(query, parser);
    assertTrue(
        CheckScriptContents.scriptContainsString(
            scriptFilter, "doc['lastname'].value.indexOf('a',0)+1"));
  }

  @Test
  public void ltrimTest() {
    String query =
        "SELECT ltrim(lastname) FROM accounts WHERE ltrim(lastname) = 'abc'";

    ScriptField scriptField = CheckScriptContents.getScriptFieldFromQuery(query);
    assertTrue(
        CheckScriptContents.scriptContainsString(
            scriptField, "Character.isWhitespace(doc['lastname'].value.charAt(pos))"));

    ScriptFilter scriptFilter = CheckScriptContents.getScriptFilterFromQuery(query, parser);
    assertTrue(
        CheckScriptContents.scriptContainsString(
            scriptFilter, "Character.isWhitespace(doc['lastname'].value.charAt(pos))"));
  }

  @Test
  public void rtrimTest() {
    String query =
        "SELECT rtrim(lastname) FROM accounts WHERE rtrim(lastname) = 'cba'";

    ScriptField scriptField = CheckScriptContents.getScriptFieldFromQuery(query);
    assertTrue(
        CheckScriptContents.scriptContainsString(
            scriptField, "Character.isWhitespace(doc['lastname'].value.charAt(pos))"));

    ScriptFilter scriptFilter = CheckScriptContents.getScriptFilterFromQuery(query, parser);
    assertTrue(
        CheckScriptContents.scriptContainsString(
            scriptFilter, "Character.isWhitespace(doc['lastname'].value.charAt(pos))"));
  }

  @Test
  public void asciiTest() {
    String query =
        "SELECT ascii(lastname) FROM accounts WHERE ascii(lastname) = 108";

    ScriptField scriptField = CheckScriptContents.getScriptFieldFromQuery(query);
    assertTrue(
        CheckScriptContents.scriptContainsString(
            scriptField, "(int) doc['lastname'].value.charAt(0)"));

    ScriptFilter scriptFilter = CheckScriptContents.getScriptFilterFromQuery(query, parser);
    assertTrue(
        CheckScriptContents.scriptContainsString(
            scriptFilter, "(int) doc['lastname'].value.charAt(0)"));
  }

  @Test
  public void left() {
    String query = "SELECT left(lastname, 1) FROM accounts";
    ScriptField scriptField = CheckScriptContents.getScriptFieldFromQuery(query);
    assertTrue(
        CheckScriptContents.scriptContainsString(
            scriptField, "doc['lastname'].value.substring(0, len)"));
  }

  @Test
  public void right() {
    String query = "SELECT right(lastname, 2) FROM accounts";
    ScriptField scriptField = CheckScriptContents.getScriptFieldFromQuery(query);
    assertTrue(
        CheckScriptContents.scriptContainsString(
            scriptField, "doc['lastname'].value.substring(start)"));
  }
}
