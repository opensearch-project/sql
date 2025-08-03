/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestsConstants.*;
import static org.opensearch.sql.util.MatcherUtils.columnName;
import static org.opensearch.sql.util.MatcherUtils.columnPattern;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyColumn;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifyErrorMessageContains;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Ignore;
import org.junit.jupiter.api.Test;

public class FieldsCommandIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    loadIndex(Index.ACCOUNT);
    loadIndex(Index.BANK);
    loadIndex(Index.MERGE_TEST_1);
    loadIndex(Index.MERGE_TEST_2);
  }

  @Test
  public void testFieldsWithOneField() throws IOException {
    JSONObject result =
        executeQuery(String.format("source=%s | fields firstname", TEST_INDEX_ACCOUNT));
    verifyColumn(result, columnName("firstname"));
    verifySchema(result, schema("firstname", "string"));
  }

  @Test
  public void testFieldsWithMultiFields() throws IOException {
    JSONObject result =
        executeQuery(String.format("source=%s | fields firstname, lastname", TEST_INDEX_ACCOUNT));
    verifyColumn(result, columnName("firstname"), columnName("lastname"));
    verifySchema(result, schema("firstname", "string"), schema("lastname", "string"));
  }

  @Ignore(
      "Cannot resolve wildcard yet. Enable once"
          + " https://github.com/opensearch-project/sql/issues/787 is resolved.")
  @Test
  public void testFieldsWildCard() throws IOException {
    JSONObject result =
        executeQuery(String.format("source=%s | fields ", TEST_INDEX_ACCOUNT) + "firstnam%");
    verifyColumn(result, columnPattern("^firstnam.*"));
  }

  @Test
  public void testSelectDateTypeField() throws IOException {
    JSONObject result =
        executeQuery(String.format("source=%s | fields birthdate", TEST_INDEX_BANK));
    verifySchema(result, schema("birthdate", null, "timestamp"));

    verifyDataRows(
        result,
        rows("2017-10-23 00:00:00"),
        rows("2017-11-20 00:00:00"),
        rows("2018-06-23 00:00:00"),
        rows("2018-11-13 23:33:20"),
        rows("2018-06-27 00:00:00"),
        rows("2018-08-19 00:00:00"),
        rows("2018-08-11 00:00:00"));
  }

  @Test
  public void testMetadataFields() throws IOException {
    JSONObject result =
        executeQuery(String.format("source=%s | fields firstname, _index", TEST_INDEX_ACCOUNT));
    verifyColumn(result, columnName("firstname"), columnName("_index"));
    verifySchema(result, schema("firstname", "string"), schema("_index", "string"));
  }

  @Test
  public void testDelimitedMetadataFields() throws IOException {
    JSONObject result =
        executeQuery(
            String.format("source=%s | fields firstname, `_id`, `_index`", TEST_INDEX_ACCOUNT));
    verifyColumn(result, columnName("firstname"), columnName("_id"), columnName("_index"));
    verifySchema(
        result, schema("firstname", "string"), schema("_id", "string"), schema("_index", "string"));
  }

  @Test
  public void testMetadataFieldsWithEval() throws IOException {
    JSONObject result =
        executeQuery(
            String.format("source=%s | eval a = 1 | fields firstname, _index", TEST_INDEX_ACCOUNT));
    verifyColumn(result, columnName("firstname"), columnName("_index"));
    verifySchema(result, schema("firstname", "string"), schema("_index", "string"));
  }

  @Test
  public void testMetadataFieldsWithEvalMetaField() {
    Exception e =
        assertThrows(
            Exception.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s | eval _id = 1 | fields firstname, _id", TEST_INDEX_ACCOUNT)));
    verifyErrorMessageContains(e, "Cannot use metadata field [_id] as the eval field.");
  }

  @Test
  public void testFieldsMergedObject() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | fields machine.os1,  machine.os2, machine_array.os1, "
                    + " machine_array.os2, machine_deep.attr1, machine_deep.attr2,"
                    + " machine_deep.layer.os1, machine_deep.layer.os2",
                TEST_INDEX_MERGE_TEST_WILDCARD));
    verifySchema(
        result,
        schema("machine.os1", "string"),
        schema("machine.os2", "string"),
        schema("machine_array.os1", "string"),
        schema("machine_array.os2", "string"),
        schema("machine_deep.attr1", "bigint"),
        schema("machine_deep.attr2", "bigint"),
        schema("machine_deep.layer.os1", "string"),
        schema("machine_deep.layer.os2", "string"));
    verifyDataRows(
        result,
        rows("linux", null, "linux", null, 1, null, "os1", null),
        rows(null, "linux", null, "linux", null, 2, null, "os2"));
  }

  /** Tests space-delimited field selection syntax. */
  @Test
  public void testFieldsWithSpaceDelimitedSyntax() throws IOException {
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
  public void testFieldsSpaceDelimitedEquivalentToCommaDelimited() throws IOException {
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

  /** Tests prefix wildcard pattern matching. */
  @Test
  public void testFieldsWithPrefixWildcard() throws IOException {
    JSONObject result =
        executeQuery(String.format("source=%s | fields account*", TEST_INDEX_ACCOUNT));
    verifyColumn(result, columnName("account_number"));
    verifySchema(result, schema("account_number", "bigint"));
  }

  /** Tests suffix wildcard pattern matching. */
  @Test
  public void testFieldsWithSuffixWildcard() throws IOException {
    JSONObject result = executeQuery(String.format("source=%s | fields *name", TEST_INDEX_ACCOUNT));
    verifyColumn(result, columnName("firstname"), columnName("lastname"));
    verifySchema(result, schema("firstname", "string"), schema("lastname", "string"));
  }

  /** Tests contains wildcard pattern matching. */
  @Test
  public void testFieldsWithContainsWildcard() throws IOException {
    JSONObject result =
        executeQuery(String.format("source=%s | fields *a* | head 1", TEST_INDEX_ACCOUNT));
    // Verify schema contains all 8 fields with 'a' in their names
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

  /** Tests mixed explicit fields and wildcard patterns. */
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

  /** Tests mixed comma and space delimiters with fields command. */
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

  /** Tests complex wildcard patterns with multiple asterisks. */
  @Test
  public void testFieldsWithComplexWildcardPattern() throws IOException {
    JSONObject result = executeQuery(String.format("source=%s | fields *a*e", TEST_INDEX_ACCOUNT));
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

  /** Tests field exclusion using wildcard patterns with minus operator. */
  @Test
  public void testFieldsMinusWithWildcards() throws IOException {
    JSONObject result =
        executeQuery(String.format("source=%s | fields - *name | head 1", TEST_INDEX_ACCOUNT));
    // Should exclude firstname and lastname
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

  /** Tests excluding multiple specific fields. */
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

  /** Tests comma-delimited field syntax with wildcard patterns. */
  @Test
  public void testFieldsCommaDelimitedWithWildcards() throws IOException {
    JSONObject result =
        executeQuery(
            String.format("source=%s | fields firstname,*a*,employer", TEST_INDEX_ACCOUNT));
    // Should include firstname, fields containing 'a', and employer
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

  /** Tests that field ordering is preserved as specified in the query. */
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

  /** Tests field ordering when wildcards are mixed with explicit fields. */
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

  /** Tests explicit field inclusion using plus prefix syntax. */
  @Test
  public void testFieldsExplicitIncludeWithPlusPrefix() throws IOException {
    JSONObject result =
        executeQuery(String.format("source=%s | fields + firstname, age", TEST_INDEX_ACCOUNT));
    verifyColumn(result, columnName("firstname"), columnName("age"));
    verifySchema(result, schema("firstname", "string"), schema("age", "bigint"));
  }

  /** Tests explicit inclusion of multiple fields with plus prefix. */
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

  /** Tests automatic deduplication when same field is specified multiple times. */
  @Test
  public void testFieldsWithDuplicateFields() throws IOException {
    JSONObject result =
        executeQuery(
            String.format("source=%s | fields firstname, age, firstname", TEST_INDEX_ACCOUNT));
    // Should automatically deduplicate the repeated firstname field
    verifyColumn(result, columnName("firstname"), columnName("age"));
    verifySchema(result, schema("firstname", "string"), schema("age", "bigint"));
  }

  /** Tests deduplication when wildcard and explicit field reference same column. */
  @Test
  public void testFieldsWithDuplicateWildcardMatches() throws IOException {
    JSONObject result =
        executeQuery(
            String.format("source=%s | fields account*, account_number", TEST_INDEX_ACCOUNT));
    // account* matches account_number, so explicit account_number should be deduplicated
    verifyColumn(result, columnName("account_number"));
    verifySchema(result, schema("account_number", "bigint"));
  }

  /** Tests performance with selection of many fields simultaneously. */
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

  /** Tests complex combinations of multiple overlapping wildcard patterns. */
  @Test
  public void testFieldsWithComplexWildcardCombinations() throws IOException {
    JSONObject result =
        executeQuery(String.format("source=%s | fields *a*, *e*, *r*", TEST_INDEX_ACCOUNT));
    // Should match fields containing 'a', 'e', or 'r'
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

  /** Tests error handling when wildcard pattern matches no existing fields. */
  @Test
  public void testFieldsWithNoMatchingWildcard() throws IOException {
    JSONObject result = executeQuery(String.format("source=%s | fields XYZ*", TEST_INDEX_ACCOUNT));
    // Should return empty result when no fields match
    verifyColumn(result);
    verifySchema(result);
  }

  /** Tests multiple wildcard exclusion patterns. */
  @Test
  public void testFieldsMinusMultipleWildcards() throws IOException {
    JSONObject result =
        executeQuery(String.format("source=%s | fields - *name, *e | head 1", TEST_INDEX_ACCOUNT));
    // Should exclude firstname, lastname, balance, age, state, email
    verifyColumn(
        result,
        columnName("account_number"),
        columnName("gender"),
        columnName("address"),
        columnName("employer"),
        columnName("email"),
        columnName("city"));
    verifySchema(
        result,
        schema("account_number", "bigint"),
        schema("gender", "string"),
        schema("address", "string"),
        schema("employer", "string"),
        schema("email", "string"),
        schema("city", "string"));
  }
}
