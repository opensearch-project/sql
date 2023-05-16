/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core;

import org.apache.http.HttpHost;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.flint.core.storage.FlintReader;

public interface FlintClient {

  static FlintClient create(FlintOptions options) {
    return new OpenSearchRestHighLevelClient(new RestHighLevelClient(RestClient.builder(new HttpHost(
        options.getHost(),
        options.getPort(),
        "http"))));
  }

  /**
   * Create {@link FlintReader}.
   *
   * @param indexName index name.
   * @param query DSL query. DSL query is null means match_all
   * @return {@link FlintReader}.
   */
  FlintReader createReader(String indexName, String query, FlintOptions option);
}
