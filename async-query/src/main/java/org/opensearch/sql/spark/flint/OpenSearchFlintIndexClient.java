/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.flint;

import lombok.RequiredArgsConstructor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.client.Client;

@RequiredArgsConstructor
public class OpenSearchFlintIndexClient implements FlintIndexClient {
  private static final Logger LOG = LogManager.getLogger();

  private final Client client;

  @Override
  public void deleteIndex(String indexName) {
    DeleteIndexRequest request = new DeleteIndexRequest().indices(indexName);
    AcknowledgedResponse response = client.admin().indices().delete(request).actionGet();
    LOG.info("OpenSearch index delete result: {}", response.isAcknowledged());
  }
}
