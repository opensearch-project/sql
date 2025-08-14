/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ACCOUNT;
import static org.opensearch.sql.util.MatcherUtils.columnName;
import static org.opensearch.sql.util.MatcherUtils.columnPattern;
import static org.opensearch.sql.util.MatcherUtils.verifyColumn;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Ignore;
import org.junit.jupiter.api.Test;

public class RenameCommandIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    loadIndex(Index.ACCOUNT);
  }

  @Test
  public void testRenameOneField() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | fields firstname | rename firstname as first_name",
                TEST_INDEX_ACCOUNT));
    verifyColumn(result, columnName("first_name"));
  }

  @Test
  public void testRenameMultiField() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | fields firstname, age | rename firstname as FIRSTNAME, age as AGE",
                TEST_INDEX_ACCOUNT));
    verifyColumn(result, columnName("FIRSTNAME"), columnName("AGE"));
  }

  @Test
  public void testRenameWildcardFields() throws IOException {
    JSONObject result = executeQuery("source=" + TEST_INDEX_ACCOUNT + " | fields firstname, lastname | rename *name as *NAME");
    verifyColumn(result, columnName("firstNAME"), columnName("lastNAME"));
  }

  @Test
  public void testRenameMultipleWildcardFields() throws IOException {
    JSONObject result = executeQuery(
        "source=" + TEST_INDEX_ACCOUNT + " | fields firstname, lastname, age | rename *name as new_*");
    verifyColumn(result, columnName("new_first"), columnName("new_last"), columnName("age"));
  }

  @Test  
  public void testRenameWildcardPrefix() throws IOException {
    JSONObject result = executeQuery(
        "source=" + TEST_INDEX_ACCOUNT + " | fields firstname, lastname, age | rename first* as FIRST*");
    verifyColumn(result, columnName("FIRSTname"), columnName("lastname"), columnName("age"));
  }

  @Test
  public void testRenameFullWildcard() throws IOException {
    JSONObject result = executeQuery(
        "source=" + TEST_INDEX_ACCOUNT + " | fields firstname, lastname | rename * as old_*");
    verifyColumn(result, columnName("old_firstname"), columnName("old_lastname"));
  }

  @Test
  public void testRenameWildcardWithMultipleCaptures() throws IOException {
    JSONObject result = executeQuery(
        "source=" + TEST_INDEX_ACCOUNT + " | fields firstname, lastname | rename *first* as *FIRST*");
    verifyColumn(result, columnName("FIRSTname"), columnName("lastname"));
  }
}
