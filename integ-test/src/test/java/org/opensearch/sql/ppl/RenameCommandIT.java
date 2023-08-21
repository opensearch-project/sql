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
  public void init() throws IOException {
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

  @Ignore(
      "Wildcard is unsupported yet. Enable once"
          + " https://github.com/opensearch-project/sql/issues/787 is resolved.")
  @Test
  public void testRenameWildcardFields() throws IOException {
    JSONObject result = executeQuery("source=" + TEST_INDEX_ACCOUNT + " | rename %name as %NAME");
    verifyColumn(result, columnPattern(".*name$"));
  }
}
