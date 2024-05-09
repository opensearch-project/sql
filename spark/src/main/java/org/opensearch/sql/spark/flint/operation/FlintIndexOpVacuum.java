/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.flint.operation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.client.Client;
import org.opensearch.sql.spark.client.EMRServerlessClientFactory;
import org.opensearch.sql.spark.flint.FlintIndexMetadata;
import org.opensearch.sql.spark.flint.FlintIndexState;
import org.opensearch.sql.spark.flint.FlintIndexStateModel;
import org.opensearch.sql.spark.flint.FlintIndexStateModelService;

/** Flint index vacuum operation. */
public class FlintIndexOpVacuum extends FlintIndexOp {

  private static final Logger LOG = LogManager.getLogger();

  /** OpenSearch client. */
  private final Client client;

  public FlintIndexOpVacuum(
      FlintIndexStateModelService flintIndexStateModelService,
      String datasourceName,
      Client client,
      EMRServerlessClientFactory emrServerlessClientFactory) {
    super(flintIndexStateModelService, datasourceName, emrServerlessClientFactory);
    this.client = client;
  }

  @Override
  boolean validate(FlintIndexState state) {
    return state == FlintIndexState.DELETED;
  }

  @Override
  FlintIndexState transitioningState() {
    return FlintIndexState.VACUUMING;
  }

  @Override
  public void runOp(FlintIndexMetadata flintIndexMetadata, FlintIndexStateModel flintIndex) {
    LOG.info("Vacuuming Flint index {}", flintIndexMetadata.getOpensearchIndexName());
    DeleteIndexRequest request =
        new DeleteIndexRequest().indices(flintIndexMetadata.getOpensearchIndexName());
    AcknowledgedResponse response = client.admin().indices().delete(request).actionGet();
    LOG.info("OpenSearch index delete result: {}", response.isAcknowledged());
  }

  @Override
  FlintIndexState stableState() {
    // Instruct StateStore to purge the index state doc
    return FlintIndexState.NONE;
  }
}
