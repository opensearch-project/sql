/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy;

import static org.hamcrest.Matchers.equalTo;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_DOG;
import static org.opensearch.sql.legacy.plugin.RestSqlStatsAction.STATS_API_ENDPOINT;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.concurrent.TimeUnit;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.sql.legacy.metrics.MetricName;

@Ignore
public class MetricsIT extends SQLIntegTestCase {

  @Override
  protected void init() throws Exception {
    loadIndex(Index.ACCOUNT);
    loadIndex(Index.DOG);
  }

  @Test
  public void requestCount() throws IOException, InterruptedException {
    multiQueries(3);
    TimeUnit.SECONDS.sleep(2L);
    JSONObject jsonObject = new JSONObject(executeStatRequest(makeStatRequest()));
    assertThat(jsonObject.getInt(MetricName.REQ_COUNT_TOTAL.getName()), equalTo(3));
  }

  private void multiQueries(int n) throws IOException {
    for (int i = 0; i < n; ++i) {
      executeQuery(String.format("SELECT COUNT(*) FROM %s/dog", TEST_INDEX_DOG));
    }
  }

  private Request makeStatRequest() {
    return new Request("GET", STATS_API_ENDPOINT);
  }

  private String executeStatRequest(final Request request) throws IOException {

    Response sqlResponse = client().performRequest(request);

    Assert.assertTrue(sqlResponse.getStatusLine().getStatusCode() == 200);

    InputStream is = sqlResponse.getEntity().getContent();
    StringBuilder sb = new StringBuilder();
    try (BufferedReader br = new BufferedReader(new InputStreamReader(is))) {
      String line = null;
      while ((line = br.readLine()) != null) {
        sb.append(line);
      }
    }

    return sb.toString();
  }
}
