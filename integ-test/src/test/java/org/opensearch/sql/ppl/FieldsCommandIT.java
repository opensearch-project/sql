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
    JSONObject result = executeQuery(Index.ACCOUNT.ppl("fields firstname, lastname"));
    verifyColumn(result, columnName("firstname"), columnName("lastname"));
    verifySchema(result, schema("firstname", "string"), schema("lastname", "string"));
  }

  @Test
  public void testMultipleFieldSelection() throws IOException {
    JSONObject result = executeQuery(Index.ACCOUNT.ppl("fields firstname, lastname, age | head 3"));
    verifySchema(
        result,
        schema("firstname", "string"),
        schema("lastname", "string"),
        schema("age", "bigint"));
    verifyDataRows(
        result,
        rows("Amber", "Duke", 32),
        rows("Hattie", "Bond", 36),
        rows("Nanette", "Bates", 28));
  }

  @Test
  public void testSpecialDataTypes() throws IOException {
    JSONObject result = executeQuery(Index.BANK.ppl("fields birthdate"));
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
    JSONObject basicResult = executeQuery(Index.ACCOUNT.ppl("fields firstname, _index"));
    verifyColumn(basicResult, columnName("firstname"), columnName("_index"));
    verifySchema(basicResult, schema("firstname", "string"), schema("_index", "string"));

    // Test delimited metadata fields
    JSONObject delimitedResult =
        executeQuery(Index.ACCOUNT.ppl("fields firstname, `_id`, `_index`"));
    verifyColumn(delimitedResult, columnName("firstname"), columnName("_id"), columnName("_index"));
    verifySchema(
        delimitedResult,
        schema("firstname", "string"),
        schema("_id", "string"),
        schema("_index", "string"));

    // Test metadata fields with eval
    JSONObject evalResult =
        executeQuery(Index.ACCOUNT.ppl("eval a = 1 | fields firstname, _index"));
    verifyColumn(evalResult, columnName("firstname"), columnName("_index"));
    verifySchema(evalResult, schema("firstname", "string"), schema("_index", "string"));
  }

  @Test
  public void testMetadataFieldsWithEvalError() {
    Exception e =
        assertThrows(
            Exception.class,
            () -> executeQuery(Index.ACCOUNT.ppl("eval _id = 1 | fields firstname, _id")));
    verifyErrorMessageContains(e, "Cannot use metadata field [_id] as the eval field.");
  }

  @Test
  public void testMergedObjectFields() throws IOException {
    JSONObject result =
        executeQuery(
            withSource(
                TEST_INDEX_MERGE_TEST_WILDCARD,
                "fields machine.os1,  machine.os2, machine_array.os1, "
                    + " machine_array.os2, machine_deep.attr1, machine_deep.attr2,"
                    + " machine_deep.layer.os1, machine_deep.layer.os2"));
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
  public void testEnhancedFieldsWhenCalciteDisabled() {
    verifyQueryThrowsCalciteError("source=%s | fields *");
    verifyQueryThrowsCalciteError("source=%s | fields account_*");
    verifyQueryThrowsCalciteError("source=%s | fields account_number balance firstname");
    verifyQueryThrowsCalciteError("source=%s | fields account_number balance, firstname");
  }

  private void verifyQueryThrowsCalciteError(String query) {
    Exception e =
        assertThrows(Exception.class, () -> executeQuery(String.format(query, TEST_INDEX_ACCOUNT)));
    verifyErrorMessageContains(
        e, "Enhanced fields feature is supported only when plugins.calcite.enabled=true");
  }

  @Ignore(
      "Cannot resolve wildcard yet. Enable once"
          + " https://github.com/opensearch-project/sql/issues/787 is resolved.")
  @Test
  public void testFieldsWildCard() throws IOException {
    JSONObject result = executeQuery(Index.ACCOUNT.ppl("fields ") + "firstnam%");
    verifyColumn(result, columnPattern("^firstnam.*"));
  }
}
