/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.*;
import static org.opensearch.sql.util.MatcherUtils.*;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ppl.PPLIntegTestCase;

/**
 * Calcite integration tests for table command. Tests space-delimited syntax and wildcard pattern
 * matching with Calcite engine.
 */
public class CalciteTableCommandIT extends PPLIntegTestCase {
  @Override
  public void init() throws Exception {
    super.init();
    loadIndex(Index.ACCOUNT);
    enableCalcite(); // Enable Calcite execution engine
    disallowCalciteFallback(); // Ensure tests run on Calcite, not fallback
  }

  /** Tests table command equivalence with fields command using Calcite engine. */
  @Test
  public void testCalciteTableEquivalentToFields() throws IOException {
    JSONObject fieldsResult =
        executeQuery(
            String.format("source=%s | fields firstname, lastname | head 3", TEST_INDEX_ACCOUNT));
    JSONObject tableResult =
        executeQuery(
            String.format("source=%s | table firstname, lastname | head 3", TEST_INDEX_ACCOUNT));

    verifySchema(fieldsResult, schema("firstname", "string"), schema("lastname", "string"));
    verifySchema(tableResult, schema("firstname", "string"), schema("lastname", "string"));

    verifyDataRows(
        fieldsResult, rows("Amber", "Duke"), rows("Hattie", "Bond"), rows("Nanette", "Bates"));
    verifyDataRows(
        tableResult, rows("Amber", "Duke"), rows("Hattie", "Bond"), rows("Nanette", "Bates"));
  }

  /** Tests space-delimited field selection with table command. */
  @Test
  public void testCalciteTableSpaceDelimited() throws IOException {
    JSONObject result =
        executeQuery(
            String.format("source=%s | table firstname lastname age | head 3", TEST_INDEX_ACCOUNT));
    verifyColumn(result, columnName("firstname"), columnName("lastname"), columnName("age"));
    verifySchema(
        result,
        schema("firstname", "string"),
        schema("lastname", "string"),
        schema("age", "bigint"));
  }

  /** Tests equivalence between space-delimited and comma-delimited table syntax. */
  @Test
  public void testCalciteTableSpaceDelimitedEquivalentToCommaDelimited() throws IOException {
    JSONObject commaResult =
        executeQuery(
            String.format(
                "source=%s | table firstname, lastname, age | head 3", TEST_INDEX_ACCOUNT));
    JSONObject spaceResult =
        executeQuery(
            String.format("source=%s | table firstname lastname age | head 3", TEST_INDEX_ACCOUNT));

    verifySchema(
        commaResult,
        schema("firstname", "string"),
        schema("lastname", "string"),
        schema("age", "bigint"));
    verifySchema(
        spaceResult,
        schema("firstname", "string"),
        schema("lastname", "string"),
        schema("age", "bigint"));

    verifyDataRows(
        commaResult,
        rows("Amber", "Duke", 32),
        rows("Hattie", "Bond", 36),
        rows("Nanette", "Bates", 28));
    verifyDataRows(
        spaceResult,
        rows("Amber", "Duke", 32),
        rows("Hattie", "Bond", 36),
        rows("Nanette", "Bates", 28));
  }

  /** Tests prefix wildcard pattern matching with table command. */
  @Test
  public void testCalciteTableWithPrefixWildcard() throws IOException {
    JSONObject result =
        executeQuery(String.format("source=%s | table account*", TEST_INDEX_ACCOUNT));
    verifyColumn(result, columnName("account_number"));
    verifySchema(result, schema("account_number", "bigint"));
  }

  /** Tests suffix wildcard pattern matching with table command. */
  @Test
  public void testCalciteTableWithSuffixWildcard() throws IOException {
    JSONObject result = executeQuery(String.format("source=%s | table *name", TEST_INDEX_ACCOUNT));
    verifyColumn(result, columnName("firstname"), columnName("lastname"));
    verifySchema(result, schema("firstname", "string"), schema("lastname", "string"));
  }

  /** Tests mixed wildcard patterns with table command. */
  @Test
  public void testCalciteTableWithMixedWildcards() throws IOException {
    JSONObject result =
        executeQuery(
            String.format("source=%s | table firstname, account*, *name", TEST_INDEX_ACCOUNT));
    verifyColumn(
        result, columnName("firstname"), columnName("account_number"), columnName("lastname"));
    verifySchema(
        result,
        schema("firstname", "string"),
        schema("account_number", "bigint"),
        schema("lastname", "string"));
  }

  /** Tests space-delimited wildcards with table command. */
  @Test
  public void testCalciteTableWithSpaceDelimitedWildcards() throws IOException {
    JSONObject result =
        executeQuery(
            String.format("source=%s | table firstname account* *name", TEST_INDEX_ACCOUNT));
    verifyColumn(
        result, columnName("firstname"), columnName("account_number"), columnName("lastname"));
    verifySchema(
        result,
        schema("firstname", "string"),
        schema("account_number", "bigint"),
        schema("lastname", "string"));
  }

  /** Tests table command with field exclusion using space-delimited syntax. */
  @Test
  public void testCalciteTableSpaceDelimitedWithRemoval() throws IOException {
    JSONObject result =
        executeQuery(
            String.format("source=%s | table - firstname lastname | head 3", TEST_INDEX_ACCOUNT));
    verifyColumn(
        result,
        columnName("account_number"),
        columnName("age"),
        columnName("gender"),
        columnName("address"),
        columnName("employer"),
        columnName("email"),
        columnName("city"),
        columnName("state"),
        columnName("balance"));
  }
}
