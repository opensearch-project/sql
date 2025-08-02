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
import org.opensearch.sql.ppl.FieldsCommandIT;

public class CalciteFieldsCommandIT extends FieldsCommandIT {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    disallowCalciteFallback();
  }

  /** Tests space-delimited field selection with Calcite engine. */
  @Test
  public void testCalciteFieldsWithSpaceDelimitedSyntax() throws IOException {
    JSONObject result =
        executeQuery(
            String.format("source=%s | fields firstname lastname age", TEST_INDEX_ACCOUNT));
    verifyColumn(result, columnName("firstname"), columnName("lastname"), columnName("age"));
    verifySchema(
        result,
        schema("firstname", "string"),
        schema("lastname", "string"),
        schema("age", "bigint"));
  }

  /** Tests equivalence between space-delimited and comma-delimited field syntax. */
  @Test
  public void testCalciteFieldsSpaceDelimitedEquivalentToCommaDelimited() throws IOException {
    JSONObject commaResult =
        executeQuery(
            String.format(
                "source=%s | fields firstname, lastname, age | head 3", TEST_INDEX_ACCOUNT));
    JSONObject spaceResult =
        executeQuery(
            String.format(
                "source=%s | fields firstname lastname age | head 3", TEST_INDEX_ACCOUNT));

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

  /** Tests field exclusion using space-delimited syntax. */
  @Test
  public void testCalciteFieldsSpaceDelimitedWithRemoval() throws IOException {
    JSONObject result =
        executeQuery(
            String.format("source=%s | fields - firstname lastname | head 3", TEST_INDEX_ACCOUNT));
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
    verifySchema(
        result,
        schema("account_number", "bigint"),
        schema("age", "bigint"),
        schema("gender", "string"),
        schema("address", "string"),
        schema("employer", "string"),
        schema("email", "string"),
        schema("city", "string"),
        schema("state", "string"),
        schema("balance", "bigint"));
  }

  /** Tests prefix wildcard pattern matching with Calcite. */
  @Test
  public void testCalciteFieldsWithPrefixWildcard() throws IOException {
    JSONObject result =
        executeQuery(String.format("source=%s | fields account*", TEST_INDEX_ACCOUNT));
    verifyColumn(result, columnName("account_number"));
    verifySchema(result, schema("account_number", "bigint"));
  }

  /** Tests suffix wildcard pattern matching with Calcite. */
  @Test
  public void testCalciteFieldsWithSuffixWildcard() throws IOException {
    JSONObject result = executeQuery(String.format("source=%s | fields *name", TEST_INDEX_ACCOUNT));
    verifyColumn(result, columnName("firstname"), columnName("lastname"));
    verifySchema(result, schema("firstname", "string"), schema("lastname", "string"));
  }

  /** Tests mixed wildcard patterns with Calcite. */
  @Test
  public void testCalciteFieldsWithMixedWildcards() throws IOException {
    JSONObject result =
        executeQuery(
            String.format("source=%s | fields firstname, account*, *name", TEST_INDEX_ACCOUNT));
    verifyColumn(
        result, columnName("firstname"), columnName("account_number"), columnName("lastname"));
    verifySchema(
        result,
        schema("firstname", "string"),
        schema("account_number", "bigint"),
        schema("lastname", "string"));
  }

  /** Tests mixed comma and space delimiters with fields command. */
  @Test
  public void testCalciteFieldsWithMixedDelimiters() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | fields firstname lastname, age | head 3", TEST_INDEX_ACCOUNT));
    verifyColumn(result, columnName("firstname"), columnName("lastname"), columnName("age"));
    verifySchema(
        result,
        schema("firstname", "string"),
        schema("lastname", "string"),
        schema("age", "bigint"));
  }

  /** Tests mixed delimiters with wildcards. */
  @Test
  public void testCalciteFieldsWithMixedDelimitersAndWildcards() throws IOException {
    JSONObject result =
        executeQuery(
            String.format("source=%s | fields firstname account*, *name", TEST_INDEX_ACCOUNT));
    verifyColumn(
        result, columnName("firstname"), columnName("account_number"), columnName("lastname"));
    verifySchema(
        result,
        schema("firstname", "string"),
        schema("account_number", "bigint"),
        schema("lastname", "string"));
  }
}
