/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.storage;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.search.SearchHit;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Abstract OpenSearch Reader.
 */
public abstract class OpenSearchReader implements FlintReader {

  /** Search request source builder. */
  private final SearchRequest searchRequest;

  protected final RestHighLevelClient client;

  /**
   * iterator of one-shot search result.
   */
  private Iterator<SearchHit> iterator = null;

  public OpenSearchReader(RestHighLevelClient client, SearchRequest searchRequest) {
    this.client = client;
    this.searchRequest = searchRequest;
  }

  @Override public boolean hasNext() {
    try {
      if (iterator == null || !iterator.hasNext()) {
        SearchResponse response = search(searchRequest);
        List<SearchHit> searchHits = Arrays.asList(response.getHits().getHits());
        iterator = searchHits.iterator();
      }
      return iterator.hasNext();
    } catch (IOException e) {
      // todo. log error.
      throw new RuntimeException(e);
    }
  }

  @Override public String next() {
    return iterator.next().getSourceAsString();
  }

  @Override public void close() {
    try {
      clean();
    } catch (IOException e) {
      // todo. log error.
    } finally {
      if (client != null) {
        try {
          client.close();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  /**
   * search.
   */
  abstract SearchResponse search(SearchRequest request) throws IOException;

  /**
   * clean.
   */
  abstract void clean() throws IOException;
}
