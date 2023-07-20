/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.sql;

import static org.hamcrest.Matchers.equalTo;
import static org.opensearch.sql.legacy.plugin.RestSqlAction.QUERY_API_ENDPOINT;
import static org.opensearch.sql.util.TestUtils.getResponseBody;

import com.google.common.io.Resources;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Locale;

import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
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
    JSONObject response = new JSONObject(executeQuery("SHOW TABLES LIKE 'acc'", "jdbc"));

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
    JSONObject response = new JSONObject(executeQuery("DESCRIBE TABLES LIKE 'acc'", "jdbc"));

    /*
     * Assumed indices of fields in dataRows based on "schema" output for DESCRIBE given above:
     * "TABLE_NAME"  : 2
     */
    JSONArray row = response.getJSONArray("datarows").getJSONArray(0);
    assertThat(row.get(2), equalTo(alias));
  }

  @Test
  public void describeSingleIndexWildcard() throws IOException {
    JSONObject response1 = executeQuery("DESCRIBE TABLES LIKE \\\"%account\\\"");
    JSONObject response2 = executeQuery("DESCRIBE TABLES LIKE '%account'");
    JSONObject response3 = executeQuery("DESCRIBE TABLES LIKE '%account' COLUMNS LIKE \\\"%name\\\"");
    JSONObject response4 = executeQuery("DESCRIBE TABLES LIKE \\\"%account\\\" COLUMNS LIKE '%name'");
    // 11 rows in the output, each corresponds to a column in the table
    assertEquals(11, response1.getJSONArray("datarows").length());
    assertTrue(response1.similar(response2));
    // 2 columns should match the wildcard
    assertEquals(2, response3.getJSONArray("datarows").length());
    assertTrue(response3.similar(response4));
  }

  @Test
  public void explainShow() throws Exception {
    String expected = loadFromFile("expectedOutput/sql/explain_show.json");
    String actual = explainQuery("SHOW TABLES LIKE '%'");
    assertTrue(new JSONObject(expected).similar(new JSONObject(actual)));
  }

  private void addAlias(String index, String alias) throws IOException {
    client().performRequest(new Request("PUT", StringUtils.format("%s/_alias/%s", index, alias)));
  }

  private String loadFromFile(String filename) throws Exception {
    URI uri = Resources.getResource(filename).toURI();
    return new String(Files.readAllBytes(Paths.get(uri)));
  }

  protected JSONObject executeQuery(String query) throws IOException {
    Request request = new Request("POST", QUERY_API_ENDPOINT);
    request.setJsonEntity(String.format(Locale.ROOT, "{\n" + "  \"query\": \"%s\"\n" + "}", query));

    RequestOptions.Builder restOptionsBuilder = RequestOptions.DEFAULT.toBuilder();
    restOptionsBuilder.addHeader("Content-Type", "application/json");
    request.setOptions(restOptionsBuilder);

    Response response = client().performRequest(request);
    return new JSONObject(getResponseBody(response));
  }
}
