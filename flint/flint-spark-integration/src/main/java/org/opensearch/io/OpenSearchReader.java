/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.io;

import org.apache.http.HttpHost;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

/** OpenSearch Reader. Todo. add scroll support. */
public class OpenSearchReader {

  private final String indexName;

  private final OpenSearchOptions options;

  private RestHighLevelClient client;

  private SearchSourceBuilder sourceBuilder;

  private Iterator<SearchHit> iterator = null;

  public OpenSearchReader(String indexName, OpenSearchOptions options) {
    this.indexName = indexName;
    this.options = options;
    sourceBuilder = new SearchSourceBuilder();
    sourceBuilder.from(0);
    sourceBuilder.size(10);
  }

  public void open() {
    client = new RestHighLevelClient(RestClient.builder(new HttpHost(options.getHost(), options.getPort(), "http")));
  }

  public boolean hasNext() {
    try {
      if (iterator == null) {
        SearchResponse
            response =
            client.search(new SearchRequest().indices(indexName).source(sourceBuilder), RequestOptions.DEFAULT);
        iterator = Arrays.asList(response.getHits().getHits()).iterator();
      }
      return iterator.hasNext();
    } catch (IOException e) {
      // todo, log.error
      throw new RuntimeException(e);
    }
  }

  /** Return each hit doc. */
  public String next() {
    return iterator.next().getSourceAsString();
  }

  public void close() {
    try {
      if (client != null) {
        client.close();
      }
    } catch (Exception e) {
      // todo, log.error
      throw new RuntimeException(e);
    }
  }
}
