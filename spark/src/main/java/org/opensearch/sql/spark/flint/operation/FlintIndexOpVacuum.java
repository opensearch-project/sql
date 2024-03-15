/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.flint.operation;

import java.util.Base64;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.client.Client;
import org.opensearch.sql.spark.execution.statestore.StateStore;
import org.opensearch.sql.spark.flint.FlintIndexState;
import org.opensearch.sql.spark.flint.FlintIndexStateModel;

/** Flint index vacuum operation. */
public class FlintIndexOpVacuum extends FlintIndexOp {

  private static final Logger LOG = LogManager.getLogger();

  /** OpenSearch client. */
  private final Client client;

  public FlintIndexOpVacuum(StateStore stateStore, String datasourceName, Client client) {
    super(stateStore, datasourceName);
    this.client = client;
  }

  @Override
  boolean validate(FlintIndexState state) {
    return state == FlintIndexState.DELETED || state == FlintIndexState.VACUUMING;
  }

  @Override
  FlintIndexState transitioningState() {
    return FlintIndexState.VACUUMING;
  }

  @Override
  void runOp(FlintIndexStateModel flintIndex) {
    // Decode Flint index name from latest ID
    String flintIndexName = new String(Base64.getDecoder().decode(flintIndex.getId()));
    LOG.info("Vacuuming Flint index {}", flintIndexName);

    // Delete OpenSearch index
    DeleteIndexRequest request = new DeleteIndexRequest().indices(flintIndexName);
    AcknowledgedResponse response = client.admin().indices().delete(request).actionGet();
    LOG.info("OpenSearch index delete result: {}", response.isAcknowledged());
  }

  @Override
  FlintIndexState stableState() {
    // Instruct StateStore to purge the index state doc
    return FlintIndexState.NONE;
  }
}
