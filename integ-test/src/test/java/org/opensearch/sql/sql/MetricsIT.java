/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.legacy.plugin.RestSqlStatsAction.STATS_API_ENDPOINT;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.sql.legacy.SQLIntegTestCase;
import org.opensearch.sql.legacy.metrics.MetricName;

public class MetricsIT extends SQLIntegTestCase {

  @Override
  protected void init() throws Exception {
    loadIndex(Index.BANK);
  }

  @Test
  public void requestCount() throws IOException, InterruptedException {
    int beforeQueries = requestTotal();
    executeQuery(String.format(Locale.ROOT, "select age from %s", TEST_INDEX_BANK));
    TimeUnit.SECONDS.sleep(2L);

    assertEquals(beforeQueries + 1, requestTotal());
  }

  @Test
  public void queryLatencyCountsOnlySuccessfulQueries() throws IOException {
    JSONObject before = new JSONObject(executeStatRequest(makeStatRequest()));
    executeQuery(String.format(Locale.ROOT, "select age from %s", TEST_INDEX_BANK));
    executeQuery(String.format(Locale.ROOT, "select count(*) from %s", TEST_INDEX_BANK));
    try {
      executeQuery("select * from no_such_index_for_latency");
    } catch (Exception expected) {
      // failed queries are not timed
    }
    JSONObject after = new JSONObject(executeStatRequest(makeStatRequest()));

    String count = MetricName.SQL_QUERY_TIMED_COUNT.getName();
    String time = MetricName.SQL_QUERY_TIME.getName();
    assertEquals(before.getLong(count) + 2, after.getLong(count));
    Assert.assertTrue(after.getLong(time) >= before.getLong(time));
  }

  private Request makeStatRequest() {
    return new Request("GET", STATS_API_ENDPOINT);
  }

  private int requestTotal() throws IOException {
    JSONObject jsonObject = new JSONObject(executeStatRequest(makeStatRequest()));
    return jsonObject.getInt(MetricName.REQ_TOTAL.getName());
  }

  private String executeStatRequest(final Request request) throws IOException {
    Response response = client().performRequest(request);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    InputStream is = response.getEntity().getContent();
    StringBuilder sb = new StringBuilder();
    try (BufferedReader br = new BufferedReader(new InputStreamReader(is))) {
      String line;
      while ((line = br.readLine()) != null) {
        sb.append(line);
      }
    }
    return sb.toString();
  }
}
