/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.flint.operation;

import lombok.RequiredArgsConstructor;
import org.opensearch.sql.spark.client.EMRServerlessClientFactory;
import org.opensearch.sql.spark.dispatcher.model.FlintIndexOptions;
import org.opensearch.sql.spark.flint.FlintIndexClient;
import org.opensearch.sql.spark.flint.FlintIndexMetadataService;
import org.opensearch.sql.spark.flint.FlintIndexStateModelService;
import org.opensearch.sql.spark.scheduler.AsyncQueryScheduler;

@RequiredArgsConstructor
public class FlintIndexOpFactory {
  private final FlintIndexStateModelService flintIndexStateModelService;
  private final FlintIndexClient flintIndexClient;
  private final FlintIndexMetadataService flintIndexMetadataService;
  private final EMRServerlessClientFactory emrServerlessClientFactory;
  private final AsyncQueryScheduler asyncQueryScheduler;

  public FlintIndexOpDrop getDrop(String datasource) {
    return new FlintIndexOpDrop(
        flintIndexStateModelService, datasource, emrServerlessClientFactory, asyncQueryScheduler);
  }

  public FlintIndexOpAlter getAlter(FlintIndexOptions flintIndexOptions, String datasource) {
    return new FlintIndexOpAlter(
        flintIndexOptions,
        flintIndexStateModelService,
        datasource,
        emrServerlessClientFactory,
        flintIndexMetadataService,
        asyncQueryScheduler);
  }

  public FlintIndexOpVacuum getVacuum(String datasource) {
    return new FlintIndexOpVacuum(
        flintIndexStateModelService,
        datasource,
        flintIndexClient,
        emrServerlessClientFactory,
        asyncQueryScheduler);
  }

  public FlintIndexOpCancel getCancel(String datasource) {
    return new FlintIndexOpCancel(
        flintIndexStateModelService, datasource, emrServerlessClientFactory);
  }
}
