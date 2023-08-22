/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql;

import static org.hamcrest.Matchers.equalTo;
import static org.opensearch.sql.util.MatcherUtils.assertJsonEquals;

import com.google.common.io.Resources;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.sql.common.utils.StringUtils;
import org.opensearch.sql.legacy.SQLIntegTestCase;
import org.opensearch.sql.legacy.TestsConstants;

public class AdminIT extends SQLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    loadIndex(Index.ACCOUNT);
  }

  @Test
  public void showSingleIndexAlias() throws IOException {
    String alias = "acc";
    addAlias(TestsConstants.TEST_INDEX_ACCOUNT, alias);
    JSONObject response = new JSONObject(executeQuery("SHOW TABLES LIKE acc", "jdbc"));

    /*
     * Assumed indices of fields in dataRows based on "schema" output for SHOW given above:
     * "TABLE_NAME" : 2
     */
    JSONArray row = response.getJSONArray("datarows").getJSONArray(0);
    assertThat(row.get(2), equalTo(alias));
  }

  @Test
  public void describeSingleIndexAlias() throws IOException {
    String alias = "acc";
    addAlias(TestsConstants.TEST_INDEX_ACCOUNT, alias);
    JSONObject response = new JSONObject(executeQuery("DESCRIBE TABLES LIKE acc", "jdbc"));

    /*
     * Assumed indices of fields in dataRows based on "schema" output for DESCRIBE given above:
     * "TABLE_NAME"  : 2
     */
    JSONArray row = response.getJSONArray("datarows").getJSONArray(0);
    assertThat(row.get(2), equalTo(alias));
  }

  @Test
  public void explainShow() throws Exception {
    String expected = loadFromFile("expectedOutput/sql/explain_show.json");

    final String actual = explainQuery("SHOW TABLES LIKE %");
    assertJsonEquals(expected, explainQuery("SHOW TABLES LIKE %"));
  }

  private void addAlias(String index, String alias) throws IOException {
    client().performRequest(new Request("PUT", StringUtils.format("%s/_alias/%s", index, alias)));
  }

  private String loadFromFile(String filename) throws Exception {
    URI uri = Resources.getResource(filename).toURI();
    return new String(Files.readAllBytes(Paths.get(uri)));
  }
}
