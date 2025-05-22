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
  }

  @Test
  public void testFieldsWithMultiFields() throws IOException {
    JSONObject result =
        executeQuery(String.format("source=%s | fields firstname, lastname", TEST_INDEX_ACCOUNT));
    verifyColumn(result, columnName("firstname"), columnName("lastname"));
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
  }

  @Test
  public void testDelimitedMetadataFields() throws IOException {
    JSONObject result =
        executeQuery(
            String.format("source=%s | fields firstname, `_id`, `_index`", TEST_INDEX_ACCOUNT));
    verifyColumn(result, columnName("firstname"), columnName("_id"), columnName("_index"));
  }

  @Test
  public void testMetadataFieldsWithEval() throws IOException {
    JSONObject result =
        executeQuery(
            String.format("source=%s | eval a = 1 | fields firstname, _index", TEST_INDEX_ACCOUNT));
    verifyColumn(result, columnName("firstname"), columnName("_index"));
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
  public void testFieldsTwoMergedObject() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | fields machine.os1,  machine.os2, machine_array.os1, "
                    + " machine_array.os2",
                TEST_INDEX_MERGE_TEST_WILDCARD));
    verifySchema(
        result,
        schema("machine.os1", "string"),
        schema("machine.os2", "string"),
        schema("machine_array.os1", "string"),
        schema("machine_array.os2", "string"));
    verifyDataRows(result, rows("linux", null, "linux", null), rows(null, "linux", null, "linux"));
  }
}
