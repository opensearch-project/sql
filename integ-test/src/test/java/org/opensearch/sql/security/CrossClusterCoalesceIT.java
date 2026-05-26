/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.security;

import static org.opensearch.sql.util.MatcherUtils.columnName;
import static org.opensearch.sql.util.MatcherUtils.verifyColumn;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Test;

public class CrossClusterCoalesceIT extends CrossClusterTestBase {

  @Override
  protected void init() throws Exception {
    super.init();
    loadIndex(Index.DOG);
    loadIndex(Index.DOG, remoteClient());
    enableCalcite();
  }

  @Test
  public void testCrossClusterCoalesceBasic() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | eval result = coalesce(holdersName, dog_name, 'unknown') |"
                    + " fields dog_name, holdersName, result | head 1",
                TEST_INDEX_DOG_REMOTE));
    verifyColumn(result, columnName("dog_name"), columnName("holdersName"), columnName("result"));
  }

  @Test
  public void testCrossClusterCoalesceMixedTypes() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | eval result = coalesce(age, holdersName, 'fallback') | fields"
                    + " age, holdersName, result | head 1",
                TEST_INDEX_DOG_REMOTE));
    verifyColumn(result, columnName("age"), columnName("holdersName"), columnName("result"));
  }

  @Test
  public void testCrossClusterCoalesceNonExistentFields() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | eval result = coalesce(missing_field, dog_name) | fields"
                    + " dog_name, result | head 1",
                TEST_INDEX_DOG_REMOTE));
    verifyColumn(result, columnName("dog_name"), columnName("result"));
  }

  @Test
  public void testCrossClusterCoalesceEmptyString() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | eval result = coalesce('', dog_name) | fields dog_name, result"
                    + " | head 1",
                TEST_INDEX_DOG_REMOTE));
    verifyColumn(result, columnName("dog_name"), columnName("result"));
  }
}
