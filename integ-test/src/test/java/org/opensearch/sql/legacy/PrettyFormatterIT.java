/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy;

import static org.hamcrest.Matchers.equalTo;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ACCOUNT;

import com.google.common.io.Files;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.sql.legacy.utils.StringUtils;

public class PrettyFormatterIT extends SQLIntegTestCase {

  @Override
  protected void init() throws Exception {
    loadIndex(Index.ACCOUNT);
  }

  @Test
  public void assertExplainPrettyFormatted() throws IOException {
    String query = StringUtils.format("SELECT firstname FROM %s", TEST_INDEX_ACCOUNT);

    String notPrettyExplainOutputFilePath =
        TestUtils.getResourceFilePath(
            "src/test/resources/expectedOutput/explainIT_format_not_pretty.json");
    String notPrettyExplainOutput =
        Files.toString(new File(notPrettyExplainOutputFilePath), StandardCharsets.UTF_8);

    assertThat(executeExplainRequest(query, ""), equalTo(notPrettyExplainOutput));
    assertThat(executeExplainRequest(query, "pretty=false"), equalTo(notPrettyExplainOutput));

    String prettyExplainOutputFilePath =
        TestUtils.getResourceFilePath(
            "src/test/resources/expectedOutput/explainIT_format_pretty.json");
    String prettyExplainOutput =
        Files.toString(new File(prettyExplainOutputFilePath), StandardCharsets.UTF_8);

    assertThat(executeExplainRequest(query, "pretty"), equalTo(prettyExplainOutput));
    assertThat(executeExplainRequest(query, "pretty=true"), equalTo(prettyExplainOutput));
  }

  private String executeExplainRequest(String query, String explainParam) throws IOException {
    String endpoint = "/_plugins/_sql/_explain?" + explainParam;
    String request = makeRequest(query);

    Request sqlRequest = new Request("POST", endpoint);
    sqlRequest.setJsonEntity(request);

    Response response = client().performRequest(sqlRequest);
    String responseString = TestUtils.getResponseBody(response, true);

    return responseString;
  }
}
