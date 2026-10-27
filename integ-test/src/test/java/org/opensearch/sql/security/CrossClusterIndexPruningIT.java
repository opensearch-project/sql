/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.security;

import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.ResponseException;
import org.opensearch.sql.common.setting.Settings;

/** Verifies that remote data stream pruning passes concrete backing names to PIT creation. */
public class CrossClusterIndexPruningIT extends CrossClusterTestBase {

  private static final String STREAM_A = "ccs_pruning_ds-a";
  private static final String STREAM_B = "ccs_pruning_ds-b";
  private static final String TEMPLATE = "ccs_pruning_ds_template";

  @Override
  protected void init() throws Exception {
    super.init();
    enableCalcite();
    updateClusterSettings(
        new ClusterSetting("persistent", Settings.Key.QUERY_PRUNING_ENABLED.getKeyValue(), "true"));
  }

  @Before
  public void setUpDataStreams() throws IOException {
    Request template = new Request("PUT", "/_index_template/" + TEMPLATE);
    template.setJsonEntity(
        """
        {
          "index_patterns": ["ccs_pruning_ds-*"],
          "priority": 500,
          "data_stream": {},
          "template": {
            "settings": {
              "number_of_shards": 1,
              "number_of_replicas": 0,
              "max_result_window": 2
            },
            "mappings": {
              "properties": {
                "@timestamp": {"type": "date"},
                "body": {"type": "keyword"}
              }
            }
          }
        }
        """);
    remoteClient().performRequest(template);
    remoteClient().performRequest(new Request("PUT", "/_data_stream/" + STREAM_A));
    remoteClient().performRequest(new Request("PUT", "/_data_stream/" + STREAM_B));
    createRemoteDoc(STREAM_A, "2026-01-15T12:05:00Z", "january-a");
    remoteClient().performRequest(new Request("POST", "/" + STREAM_A + "/_rollover"));
    createRemoteDoc(STREAM_A, "2026-02-15T12:05:00Z", "february-a");
    createRemoteDoc(STREAM_B, "2026-02-15T12:05:00Z", "february-b");
    remoteClient().performRequest(new Request("POST", "/" + STREAM_A + "/_rollover"));
    remoteClient().performRequest(new Request("POST", "/" + STREAM_B + "/_rollover"));
    createRemoteDoc(STREAM_A, "2026-03-15T12:05:00Z", "march-a");
    createRemoteDoc(STREAM_B, "2026-03-15T12:05:00Z", "march-b");
  }

  @After
  public void tearDownDataStreams() throws IOException {
    try {
      deleteRemote("/_data_stream/ccs_pruning_ds-*");
      deleteRemote("/_index_template/" + TEMPLATE);
    } finally {
      updateClusterSettings(
          new ClusterSetting("persistent", Settings.Key.QUERY_PRUNING_ENABLED.getKeyValue(), null));
    }
  }

  @Test
  public void prunesRemoteDataStreamWildcardWithRequestBounds() throws IOException {
    long pitCount = pitTotal();
    String query =
        """
        source=%s:ccs_pruning_ds-*
        | where @timestamp >= '2026-03-15 12:00:00' and @timestamp <= '2026-03-15 12:15:00'
        | sort body | fields body | head 5
        """
            .formatted(REMOTE_CLUSTER)
            .replace('\n', ' ');
    Request request = new Request("POST", "/_plugins/_ppl");
    request.setJsonEntity(
        new JSONObject()
            .put("query", query)
            .put("time_field", "@timestamp")
            .put("start_time", "2026-03-15 12:00:00")
            .put("end_time", "2026-03-15 12:15:00")
            .toString());
    JSONObject response = new JSONObject(executeRequest(request));
    verifyDataRows(response, rows("march-a"), rows("march-b"));
    assertEquals(pitCount + 2, pitTotalAfterRelease());
  }

  @Test
  public void prunesRemoteDataStreamWildcard() throws IOException {
    long pitCount = pitTotal();
    String query =
        """
        source=%s:ccs_pruning_ds-*
        | where @timestamp >= '2026-03-15 12:00:00' and @timestamp <= '2026-03-15 12:15:00'
        | sort body | fields body | head 5
        """
            .formatted(REMOTE_CLUSTER)
            .replace('\n', ' ');
    JSONObject response = executeQuery(query);
    verifyDataRows(response, rows("march-a"), rows("march-b"));
    assertEquals(pitCount + 2, pitTotalAfterRelease());
  }

  @Test
  public void prunesRemoteExactDataStreamWithRequestBounds() throws IOException {
    long pitCount = pitTotal();
    String query =
        """
        source=%s:%s
        | where @timestamp >= '2026-03-15 12:00:00' and @timestamp <= '2026-03-15 12:15:00'
        | sort body | fields body | head 5
        """
            .formatted(REMOTE_CLUSTER, STREAM_A)
            .replace('\n', ' ');
    Request request = new Request("POST", "/_plugins/_ppl");
    request.setJsonEntity(
        new JSONObject()
            .put("query", query)
            .put("time_field", "@timestamp")
            .put("start_time", "2026-03-15 12:00:00")
            .put("end_time", "2026-03-15 12:15:00")
            .toString());
    JSONObject response = new JSONObject(executeRequest(request));
    verifyDataRows(response, rows("march-a"));
    assertEquals(pitCount + 1, pitTotalAfterRelease());
  }

  @Test
  public void prunesRemoteExactDataStream() throws IOException {
    long pitCount = pitTotal();
    String query =
        """
        source=%s:%s
        | where @timestamp >= '2026-03-15 12:00:00' and @timestamp <= '2026-03-15 12:15:00'
        | sort body | fields body | head 5
        """
            .formatted(REMOTE_CLUSTER, STREAM_A)
            .replace('\n', ' ');
    JSONObject response = executeQuery(query);
    verifyDataRows(response, rows("march-a"));
    assertEquals(pitCount + 1, pitTotalAfterRelease());
  }

  @Test
  public void prunesRemoteStatsValuesWithRequestBounds() throws IOException {
    long pitCount = pitTotal();
    String query =
        """
        source=%s:ccs_pruning_ds-*
        | where @timestamp >= '2026-03-15 12:00:00' and @timestamp <= '2026-03-15 12:15:00'
        | stats values(body) as bodies
        | eval n = array_length(bodies) | fields n
        """
            .formatted(REMOTE_CLUSTER)
            .replace('\n', ' ');
    Request request = new Request("POST", "/_plugins/_ppl");
    request.setJsonEntity(
        new JSONObject()
            .put("query", query)
            .put("time_field", "@timestamp")
            .put("start_time", "2026-03-15 12:00:00")
            .put("end_time", "2026-03-15 12:15:00")
            .toString());
    JSONObject response = new JSONObject(executeRequest(request));
    verifyDataRows(response, rows(2));
    assertEquals(pitCount + 2, pitTotalAfterRelease());
  }

  @Test
  public void prunesRemoteStatsValues() throws IOException {
    long pitCount = pitTotal();
    String query =
        """
        source=%s:ccs_pruning_ds-*
        | where @timestamp >= '2026-03-15 12:00:00' and @timestamp <= '2026-03-15 12:15:00'
        | stats values(body) as bodies
        | eval n = array_length(bodies) | fields n
        """
            .formatted(REMOTE_CLUSTER)
            .replace('\n', ' ');
    JSONObject response = executeQuery(query);
    verifyDataRows(response, rows(2));
    assertEquals(pitCount + 2, pitTotalAfterRelease());
  }

  private void createRemoteDoc(String stream, String timestamp, String body) throws IOException {
    Request request = new Request("POST", "/" + stream + "/_bulk?refresh=true");
    request.setJsonEntity(
        "{\"create\":{}}\n"
            + new JSONObject().put("@timestamp", timestamp).put("body", body)
            + "\n");
    remoteClient().performRequest(request);
  }

  private long pitTotal() throws IOException {
    JSONObject response =
        new JSONObject(
            executeRequest(new Request("GET", "/ccs_pruning_ds-*/_stats/search"), remoteClient()));
    return ((Number) response.query("/_all/total/search/point_in_time_total")).longValue();
  }

  private long pitTotalAfterRelease() throws IOException {
    client().performRequest(new Request("DELETE", "/_search/point_in_time/_all"));
    remoteClient().performRequest(new Request("DELETE", "/_search/point_in_time/_all"));
    return pitTotal();
  }

  private void deleteRemote(String path) throws IOException {
    try {
      remoteClient().performRequest(new Request("DELETE", path));
    } catch (ResponseException e) {
      if (e.getResponse().getStatusLine().getStatusCode() != 404) {
        throw e;
      }
    }
  }
}
