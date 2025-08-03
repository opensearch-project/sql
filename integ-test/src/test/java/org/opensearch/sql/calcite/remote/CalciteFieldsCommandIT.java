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
 * Comprehensive Calcite integration tests for both fields and table commands. Tests wildcard
 * patterns, delimiter syntax, field exclusion, and command equivalence.
 */
public class CalciteFieldsCommandIT extends PPLIntegTestCase {
  @Override
  public void init() throws Exception {
    super.init();
    loadIndex(Index.ACCOUNT);
    loadIndex(Index.BANK);
    enableCalcite();
    disallowCalciteFallback();
  }

  // Basic field selection tests - table command only (fields covered in FieldsCommandIT)

  @Test
  public void testTableBasic() throws IOException {
    JSONObject result =
        executeQuery(String.format("source=%s | table firstname, lastname", TEST_INDEX_ACCOUNT));
    verifyColumn(result, columnName("firstname"), columnName("lastname"));
    verifySchema(result, schema("firstname", "string"), schema("lastname", "string"));
  }

  // Space-delimited syntax tests - table command only

  @Test
  public void testTableSpaceDelimited() throws IOException {
    JSONObject result =
        executeQuery(String.format("source=%s | table firstname lastname age", TEST_INDEX_ACCOUNT));
    verifyColumn(result, columnName("firstname"), columnName("lastname"), columnName("age"));
    verifySchema(
        result,
        schema("firstname", "string"),
        schema("lastname", "string"),
        schema("age", "bigint"));
  }

  // Wildcard pattern tests - table command only

  @Test
  public void testTableWithPrefixWildcard() throws IOException {
    JSONObject result =
        executeQuery(String.format("source=%s | table account*", TEST_INDEX_ACCOUNT));
    verifyColumn(result, columnName("account_number"));
    verifySchema(result, schema("account_number", "bigint"));
  }

  @Test
  public void testFieldsWithSuffixWildcard() throws IOException {
    JSONObject result = executeQuery(String.format("source=%s | fields *name", TEST_INDEX_ACCOUNT));
    verifyColumn(result, columnName("firstname"), columnName("lastname"));
    verifySchema(result, schema("firstname", "string"), schema("lastname", "string"));
  }

  @Test
  public void testTableWithSuffixWildcard() throws IOException {
    JSONObject result = executeQuery(String.format("source=%s | table *name", TEST_INDEX_ACCOUNT));
    verifyColumn(result, columnName("firstname"), columnName("lastname"));
    verifySchema(result, schema("firstname", "string"), schema("lastname", "string"));
  }

  @Test
  public void testFieldsWithContainsWildcard() throws IOException {
    JSONObject result =
        executeQuery(String.format("source=%s | fields *a* | head 1", TEST_INDEX_ACCOUNT));
    // Matches fields containing 'a'
    verifySchema(
        result,
        schema("account_number", "bigint"),
        schema("balance", "bigint"),
        schema("firstname", "string"),
        schema("lastname", "string"),
        schema("age", "bigint"),
        schema("address", "string"),
        schema("email", "string"),
        schema("state", "string"));
  }

  @Test
  public void testTableWithContainsWildcard() throws IOException {
    JSONObject result =
        executeQuery(String.format("source=%s | table *a* | head 1", TEST_INDEX_ACCOUNT));
    verifySchema(
        result,
        schema("account_number", "bigint"),
        schema("balance", "bigint"),
        schema("firstname", "string"),
        schema("lastname", "string"),
        schema("age", "bigint"),
        schema("address", "string"),
        schema("email", "string"),
        schema("state", "string"));
  }

  @Test
  public void testFieldsWithComplexWildcardPattern() throws IOException {
    JSONObject result = executeQuery(String.format("source=%s | fields *a*e", TEST_INDEX_ACCOUNT));
    // Matches fields containing 'a' and ending with 'e'
    verifyColumn(
        result,
        columnName("balance"),
        columnName("firstname"),
        columnName("lastname"),
        columnName("age"),
        columnName("state"));
    verifySchema(
        result,
        schema("balance", "bigint"),
        schema("firstname", "string"),
        schema("lastname", "string"),
        schema("age", "bigint"),
        schema("state", "string"));
  }

  @Test
  public void testTableWithComplexWildcardPattern() throws IOException {
    JSONObject result = executeQuery(String.format("source=%s | table *a*e", TEST_INDEX_ACCOUNT));
    verifyColumn(
        result,
        columnName("balance"),
        columnName("firstname"),
        columnName("lastname"),
        columnName("age"),
        columnName("state"));
    verifySchema(
        result,
        schema("balance", "bigint"),
        schema("firstname", "string"),
        schema("lastname", "string"),
        schema("age", "bigint"),
        schema("state", "string"));
  }

  // Mixed wildcard tests
  @Test
  public void testFieldsWithMixedWildcards() throws IOException {
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

  @Test
  public void testTableWithMixedWildcards() throws IOException {
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

  @Test
  public void testFieldsWithComplexMixedWildcards() throws IOException {
    JSONObject result =
        executeQuery(String.format("source=%s | fields *a*, *e*, *r*", TEST_INDEX_ACCOUNT));
    // Matches fields containing 'a', 'e', or 'r'
    verifyColumn(
        result,
        columnName("account_number"),
        columnName("balance"),
        columnName("firstname"),
        columnName("lastname"),
        columnName("age"),
        columnName("gender"),
        columnName("address"),
        columnName("employer"),
        columnName("email"),
        columnName("state"));
    verifySchema(
        result,
        schema("account_number", "bigint"),
        schema("balance", "bigint"),
        schema("firstname", "string"),
        schema("lastname", "string"),
        schema("age", "bigint"),
        schema("gender", "string"),
        schema("address", "string"),
        schema("employer", "string"),
        schema("email", "string"),
        schema("state", "string"));
  }

  @Test
  public void testTableWithComplexMixedWildcards() throws IOException {
    JSONObject result =
        executeQuery(String.format("source=%s | table *a*, *e*, *r*", TEST_INDEX_ACCOUNT));
    verifyColumn(
        result,
        columnName("account_number"),
        columnName("balance"),
        columnName("firstname"),
        columnName("lastname"),
        columnName("age"),
        columnName("gender"),
        columnName("address"),
        columnName("employer"),
        columnName("email"),
        columnName("state"));
    verifySchema(
        result,
        schema("account_number", "bigint"),
        schema("balance", "bigint"),
        schema("firstname", "string"),
        schema("lastname", "string"),
        schema("age", "bigint"),
        schema("gender", "string"),
        schema("address", "string"),
        schema("employer", "string"),
        schema("email", "string"),
        schema("state", "string"));
  }

  // Field exclusion tests
  @Test
  public void testFieldsMinusSpaceDelimited() throws IOException {
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

  @Test
  public void testTableMinusSpaceDelimited() throws IOException {
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

  @Test
  public void testFieldsMinusWithWildcards() throws IOException {
    JSONObject result =
        executeQuery(String.format("source=%s | fields - *name | head 1", TEST_INDEX_ACCOUNT));
    // Excludes firstname and lastname
    verifySchema(
        result,
        schema("account_number", "bigint"),
        schema("balance", "bigint"),
        schema("age", "bigint"),
        schema("gender", "string"),
        schema("address", "string"),
        schema("employer", "string"),
        schema("email", "string"),
        schema("city", "string"),
        schema("state", "string"));
  }

  @Test
  public void testTableMinusWithWildcards() throws IOException {
    JSONObject result =
        executeQuery(String.format("source=%s | table - *name | head 1", TEST_INDEX_ACCOUNT));
    verifySchema(
        result,
        schema("account_number", "bigint"),
        schema("balance", "bigint"),
        schema("age", "bigint"),
        schema("gender", "string"),
        schema("address", "string"),
        schema("employer", "string"),
        schema("email", "string"),
        schema("city", "string"),
        schema("state", "string"));
  }

  @Test
  public void testFieldsMinusMultipleFields() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | fields - firstname, lastname, age | head 1", TEST_INDEX_ACCOUNT));
    verifySchema(
        result,
        schema("account_number", "bigint"),
        schema("balance", "bigint"),
        schema("gender", "string"),
        schema("address", "string"),
        schema("employer", "string"),
        schema("email", "string"),
        schema("city", "string"),
        schema("state", "string"));
  }

  @Test
  public void testTableMinusMultipleFields() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | table - firstname, lastname, age | head 1", TEST_INDEX_ACCOUNT));
    verifySchema(
        result,
        schema("account_number", "bigint"),
        schema("balance", "bigint"),
        schema("gender", "string"),
        schema("address", "string"),
        schema("employer", "string"),
        schema("email", "string"),
        schema("city", "string"),
        schema("state", "string"));
  }

  // Mixed delimiter syntax tests
  @Test
  public void testFieldsWithMixedDelimiters() throws IOException {
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

  @Test
  public void testTableWithMixedDelimiters() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | table firstname lastname, age | head 3", TEST_INDEX_ACCOUNT));
    verifyColumn(result, columnName("firstname"), columnName("lastname"), columnName("age"));
    verifySchema(
        result,
        schema("firstname", "string"),
        schema("lastname", "string"),
        schema("age", "bigint"));
  }

  @Test
  public void testFieldsCommaDelimitedWithWildcards() throws IOException {
    JSONObject result =
        executeQuery(
            String.format("source=%s | fields firstname,*a*,employer", TEST_INDEX_ACCOUNT));
    verifyColumn(
        result,
        columnName("firstname"),
        columnName("account_number"),
        columnName("balance"),
        columnName("lastname"),
        columnName("age"),
        columnName("address"),
        columnName("email"),
        columnName("state"),
        columnName("employer"));
    verifySchema(
        result,
        schema("firstname", "string"),
        schema("account_number", "bigint"),
        schema("balance", "bigint"),
        schema("lastname", "string"),
        schema("age", "bigint"),
        schema("address", "string"),
        schema("email", "string"),
        schema("state", "string"),
        schema("employer", "string"));
  }

  @Test
  public void testTableCommaDelimitedWithWildcards() throws IOException {
    JSONObject result =
        executeQuery(String.format("source=%s | table firstname,*a*,employer", TEST_INDEX_ACCOUNT));
    verifyColumn(
        result,
        columnName("firstname"),
        columnName("account_number"),
        columnName("balance"),
        columnName("lastname"),
        columnName("age"),
        columnName("address"),
        columnName("email"),
        columnName("state"),
        columnName("employer"));
    verifySchema(
        result,
        schema("firstname", "string"),
        schema("account_number", "bigint"),
        schema("balance", "bigint"),
        schema("lastname", "string"),
        schema("age", "bigint"),
        schema("address", "string"),
        schema("email", "string"),
        schema("state", "string"),
        schema("employer", "string"));
  }

  // Field ordering tests
  @Test
  public void testFieldsOrdering() throws IOException {
    JSONObject result =
        executeQuery(
            String.format("source=%s | fields age, firstname, balance", TEST_INDEX_ACCOUNT));
    verifyColumn(result, columnName("age"), columnName("firstname"), columnName("balance"));
    verifySchema(
        result,
        schema("age", "bigint"),
        schema("firstname", "string"),
        schema("balance", "bigint"));
  }

  @Test
  public void testTableOrdering() throws IOException {
    JSONObject result =
        executeQuery(
            String.format("source=%s | table age, firstname, balance", TEST_INDEX_ACCOUNT));
    verifyColumn(result, columnName("age"), columnName("firstname"), columnName("balance"));
    verifySchema(
        result,
        schema("age", "bigint"),
        schema("firstname", "string"),
        schema("balance", "bigint"));
  }

  @Test
  public void testFieldsOrderingWithWildcards() throws IOException {
    JSONObject result =
        executeQuery(
            String.format("source=%s | fields balance, account*, firstname", TEST_INDEX_ACCOUNT));
    verifyColumn(
        result, columnName("balance"), columnName("account_number"), columnName("firstname"));
    verifySchema(
        result,
        schema("balance", "bigint"),
        schema("account_number", "bigint"),
        schema("firstname", "string"));
  }

  @Test
  public void testTableOrderingWithWildcards() throws IOException {
    JSONObject result =
        executeQuery(
            String.format("source=%s | table balance, account*, firstname", TEST_INDEX_ACCOUNT));
    verifyColumn(
        result, columnName("balance"), columnName("account_number"), columnName("firstname"));
    verifySchema(
        result,
        schema("balance", "bigint"),
        schema("account_number", "bigint"),
        schema("firstname", "string"));
  }

  // Explicit include with + prefix (fields only)
  @Test
  public void testFieldsExplicitIncludeWithPlusPrefix() throws IOException {
    JSONObject result =
        executeQuery(String.format("source=%s | fields + firstname, age", TEST_INDEX_ACCOUNT));
    verifyColumn(result, columnName("firstname"), columnName("age"));
    verifySchema(result, schema("firstname", "string"), schema("age", "bigint"));
  }

  @Test
  public void testFieldsExplicitIncludeMultiple() throws IOException {
    JSONObject result =
        executeQuery(
            String.format("source=%s | fields + firstname, balance, employer", TEST_INDEX_ACCOUNT));
    verifyColumn(result, columnName("firstname"), columnName("balance"), columnName("employer"));
    verifySchema(
        result,
        schema("firstname", "string"),
        schema("balance", "bigint"),
        schema("employer", "string"));
  }

  // Deduplication tests
  @Test
  public void testFieldsWithDuplicateFields() throws IOException {
    JSONObject result =
        executeQuery(
            String.format("source=%s | fields firstname, age, firstname", TEST_INDEX_ACCOUNT));
    // Deduplicates repeated firstname
    verifyColumn(result, columnName("firstname"), columnName("age"));
    verifySchema(result, schema("firstname", "string"), schema("age", "bigint"));
  }

  @Test
  public void testTableWithDuplicateFields() throws IOException {
    JSONObject result =
        executeQuery(
            String.format("source=%s | table firstname, age, firstname", TEST_INDEX_ACCOUNT));
    verifyColumn(result, columnName("firstname"), columnName("age"));
    verifySchema(result, schema("firstname", "string"), schema("age", "bigint"));
  }

  @Test
  public void testFieldsWithDuplicateWildcardMatches() throws IOException {
    JSONObject result =
        executeQuery(
            String.format("source=%s | fields account*, account_number", TEST_INDEX_ACCOUNT));
    // account* matches account_number, deduplicates explicit account_number
    verifyColumn(result, columnName("account_number"));
    verifySchema(result, schema("account_number", "bigint"));
  }

  @Test
  public void testTableWithDuplicateWildcardMatches() throws IOException {
    JSONObject result =
        executeQuery(
            String.format("source=%s | table account*, account_number", TEST_INDEX_ACCOUNT));
    verifyColumn(result, columnName("account_number"));
    verifySchema(result, schema("account_number", "bigint"));
  }

  // Command equivalence tests
  @Test
  public void testFieldsAndTableEquivalence() throws IOException {
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

  @Test
  public void testSpaceDelimitedEquivalentToCommaDelimited() throws IOException {
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

  // Performance tests with many fields
  @Test
  public void testFieldsWithManyFields() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | fields firstname, lastname, age, balance, address, employer",
                TEST_INDEX_ACCOUNT));
    verifyColumn(
        result,
        columnName("firstname"),
        columnName("lastname"),
        columnName("age"),
        columnName("balance"),
        columnName("address"),
        columnName("employer"));
    verifySchema(
        result,
        schema("firstname", "string"),
        schema("lastname", "string"),
        schema("age", "bigint"),
        schema("balance", "bigint"),
        schema("address", "string"),
        schema("employer", "string"));
  }

  @Test
  public void testTableWithManyFields() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | table firstname, lastname, age, balance, address, employer",
                TEST_INDEX_ACCOUNT));
    verifyColumn(
        result,
        columnName("firstname"),
        columnName("lastname"),
        columnName("age"),
        columnName("balance"),
        columnName("address"),
        columnName("employer"));
    verifySchema(
        result,
        schema("firstname", "string"),
        schema("lastname", "string"),
        schema("age", "bigint"),
        schema("balance", "bigint"),
        schema("address", "string"),
        schema("employer", "string"));
  }

  // Error condition tests
  @Test
  public void testFieldsWithNoMatchingWildcard() {
    Exception e =
        assertThrows(
            Exception.class,
            () -> executeQuery(String.format("source=%s | fields XYZ*", TEST_INDEX_ACCOUNT)));
    // No fields match XYZ*
    verifyErrorMessageContains(e, "wildcard pattern [XYZ*] matches no fields");
  }

  @Test
  public void testTableWithNoMatchingWildcard() {
    Exception e =
        assertThrows(
            Exception.class,
            () -> executeQuery(String.format("source=%s | table *XYZ", TEST_INDEX_ACCOUNT)));
    // No fields match *XYZ
    verifyErrorMessageContains(e, "wildcard pattern [*XYZ] matches no fields");
  }
}
