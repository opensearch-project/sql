package com.amazon.opendistroforelasticsearch.sql.ppl;

import static com.amazon.opendistroforelasticsearch.sql.plugin.rest.RestPPLQueryAction.LEGACY_EXPLAIN_API_ENDPOINT;
import static com.amazon.opendistroforelasticsearch.sql.plugin.rest.RestPPLQueryAction.LEGACY_QUERY_API_ENDPOINT;
import static com.amazon.opendistroforelasticsearch.sql.plugin.rest.RestPPLStatsAction.PPL_LEGACY_STATS_API_ENDPOINT;

import java.io.IOException;
import org.junit.Assert;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.Response;

/**
 * For backward compatibility, check if legacy API endpoints are accessible.
 */
public class LegacyAPICompatibilityIT extends PPLIntegTestCase {

  @Override
  public void init() throws IOException {
    loadIndex(Index.ACCOUNT);
  }

  @Test
  public void query() throws IOException {
    String query = "source=opensearch-sql_test_index_account | where age > 30";
    Request request = buildRequest(query, LEGACY_QUERY_API_ENDPOINT);
    Response response = client().performRequest(request);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
  }

  @Test
  public void explain() throws IOException {
    String query = "source=opensearch-sql_test_index_account | where age > 30";
    Request request = buildRequest(query, LEGACY_EXPLAIN_API_ENDPOINT);
    Response response = client().performRequest(request);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
  }

  @Test
  public void stats() throws IOException {
    Request request = new Request("GET", PPL_LEGACY_STATS_API_ENDPOINT);
    Response response = client().performRequest(request);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
  }

}
