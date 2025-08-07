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
  public void testBasicFieldSelection() throws IOException {
    JSONObject result =
        executeQuery(String.format("source=%s | fields firstname, lastname", TEST_INDEX_ACCOUNT));
    verifyColumn(result, columnName("firstname"), columnName("lastname"));
    verifySchema(result, schema("firstname", "string"), schema("lastname", "string"));
  }

  @Test
  public void testDelimiterEquivalence() throws IOException {
    JSONObject commaResult =
        executeQuery(
            String.format(
                "source=%s | fields firstname, lastname, age | head 3", TEST_INDEX_ACCOUNT));
    JSONObject spaceResult =
        executeQuery(
            String.format(
                "source=%s | fields firstname lastname age | head 3", TEST_INDEX_ACCOUNT));
    JSONObject mixedResult =
        executeQuery(
            String.format(
                "source=%s | fields firstname lastname, age | head 3", TEST_INDEX_ACCOUNT));

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
    verifySchema(
        mixedResult,
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
    verifyDataRows(
        mixedResult,
        rows("Amber", "Duke", 32),
        rows("Hattie", "Bond", 36),
        rows("Nanette", "Bates", 28));
  }

  @Test
  public void testWildcardPatterns() throws IOException {
    // Test prefix wildcard
    JSONObject prefixResult =
        executeQuery(String.format("source=%s | fields account*", TEST_INDEX_ACCOUNT));
    verifyColumn(prefixResult, columnName("account_number"));
    verifySchema(prefixResult, schema("account_number", "bigint"));

    // Test suffix wildcard
    JSONObject suffixResult =
        executeQuery(String.format("source=%s | fields *name", TEST_INDEX_ACCOUNT));
    verifyColumn(suffixResult, columnName("firstname"), columnName("lastname"));
    verifySchema(suffixResult, schema("firstname", "string"), schema("lastname", "string"));

    // Test contains wildcard
    JSONObject containsResult =
        executeQuery(String.format("source=%s | fields *a* | head 1", TEST_INDEX_ACCOUNT));
    verifySchema(
        containsResult,
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
  public void testMixedWildcards() throws IOException {
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
  public void testSpecialDataTypes() throws IOException {
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
    // Test basic metadata fields
    JSONObject basicResult =
        executeQuery(String.format("source=%s | fields firstname, _index", TEST_INDEX_ACCOUNT));
    verifyColumn(basicResult, columnName("firstname"), columnName("_index"));
    verifySchema(basicResult, schema("firstname", "string"), schema("_index", "string"));

    // Test delimited metadata fields
    JSONObject delimitedResult =
        executeQuery(
            String.format("source=%s | fields firstname, `_id`, `_index`", TEST_INDEX_ACCOUNT));
    verifyColumn(delimitedResult, columnName("firstname"), columnName("_id"), columnName("_index"));
    verifySchema(
        delimitedResult,
        schema("firstname", "string"),
        schema("_id", "string"),
        schema("_index", "string"));

    // Test metadata fields with eval
    JSONObject evalResult =
        executeQuery(
            String.format("source=%s | eval a = 1 | fields firstname, _index", TEST_INDEX_ACCOUNT));
    verifyColumn(evalResult, columnName("firstname"), columnName("_index"));
    verifySchema(evalResult, schema("firstname", "string"), schema("_index", "string"));
  }

  @Test
  public void testMetadataFieldsWithEvalError() {
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
  public void testMergedObjectFields() throws IOException {
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

  @Test
  public void testFieldsExcludeAllScenarios() {
    String[] excludeAllQueries = {
      "source=%s | fields - *",
      "source=%s | fields - account* firstname lastname, *",
      "source=%s | fields account* firstname lastname, * | fields - *",
      "source=%s | fields account* firstname lastname, * | fields + * | fields - *",
      "source=%s | fields account* firstname lastname, * | fields - * | fields + *"
    };

    for (String query : excludeAllQueries) {
      Exception e =
          assertThrows(
              Exception.class, () -> executeQuery(String.format(query, TEST_INDEX_ACCOUNT)));
      verifyErrorMessageContains(
          e, "Invalid field exclusion: operation would exclude all fields from the result set");
    }
  }

  @Test
  public void testFieldsIncludeAllVariations() throws IOException {
    String[] includeQueries = {
      "source=%s | fields + * | head 1",
      "source=%s | fields account* firstname lastname, * | fields + * | fields + * | head 1"
    };

    for (String query : includeQueries) {
      JSONObject result = executeQuery(String.format(query, TEST_INDEX_ACCOUNT));
      verifySchema(
          result,
          schema("account_number", "bigint"),
          schema("firstname", "string"),
          schema("gender", "string"),
          schema("city", "string"),
          schema("balance", "bigint"),
          schema("employer", "string"),
          schema("state", "string"),
          schema("age", "bigint"),
          schema("email", "string"),
          schema("address", "string"),
          schema("lastname", "string"));
    }
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
}
