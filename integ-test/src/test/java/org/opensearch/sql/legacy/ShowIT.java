/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.opensearch.sql.legacy.TestUtils.createIndexByRestClient;
import static org.opensearch.sql.legacy.TestUtils.isIndexExist;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

public class ShowIT extends SQLIntegTestCase {

  @Override
  protected void init() {
    // Note: not using the existing TEST_INDEX_* indices, since underscore in the names causes
    // issues
    createEmptyIndexIfNotExist("abcdefg");
    createEmptyIndexIfNotExist("abcdefghijk");
    createEmptyIndexIfNotExist("abcdijk");
  }

  @Test
  public void showAllMatchAll() throws IOException {

    showIndexTest("%", 3, false);
  }

  @Test
  public void showIndexMatchPrefix() throws IOException {

    showIndexTest("abcdefg" + "%", 2, true);
  }

  @Test
  public void showIndexMatchSuffix() throws IOException {

    showIndexTest("%ijk", 2, true);
  }

  @Test
  public void showIndexMatchExact() throws IOException {

    showIndexTest("abcdefg", 1, true);
  }

  private void showIndexTest(String querySuffix, int expectedMatches, boolean exactMatch)
      throws IOException {

    final String query = "SHOW TABLES LIKE " + querySuffix;
    JSONObject result = executeQuery(query);

    if (exactMatch) {
      Assert.assertThat(result.length(), equalTo(expectedMatches));
    } else {
      Assert.assertThat(result.length(), greaterThanOrEqualTo(expectedMatches));
    }
    for (String indexName : result.keySet()) {
      Assert.assertTrue(result.getJSONObject(indexName).has("mappings"));
    }
  }

  private void createEmptyIndexIfNotExist(String indexName) {
    if (!isIndexExist(client(), indexName)) {
      createIndexByRestClient(client(), indexName, null);
    }
  }
}
