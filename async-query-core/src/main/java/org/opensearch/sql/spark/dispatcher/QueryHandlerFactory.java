/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.dispatcher;

import lombok.RequiredArgsConstructor;
import org.opensearch.sql.spark.client.EMRServerlessClientFactory;
import org.opensearch.sql.spark.execution.session.SessionManager;
import org.opensearch.sql.spark.flint.FlintIndexMetadataService;
import org.opensearch.sql.spark.flint.IndexDMLResultStorageService;
import org.opensearch.sql.spark.flint.operation.FlintIndexOpFactory;
import org.opensearch.sql.spark.leasemanager.LeaseManager;
import org.opensearch.sql.spark.metrics.MetricsService;
import org.opensearch.sql.spark.parameter.SparkSubmitParametersBuilderProvider;
import org.opensearch.sql.spark.response.JobExecutionResponseReader;

@RequiredArgsConstructor
public class QueryHandlerFactory {

  private final JobExecutionResponseReader jobExecutionResponseReader;
  private final FlintIndexMetadataService flintIndexMetadataService;
  private final SessionManager sessionManager;
  private final LeaseManager leaseManager;
  private final IndexDMLResultStorageService indexDMLResultStorageService;
  private final FlintIndexOpFactory flintIndexOpFactory;
  private final EMRServerlessClientFactory emrServerlessClientFactory;
  private final MetricsService metricsService;
  protected final SparkSubmitParametersBuilderProvider sparkSubmitParametersBuilderProvider;

  public RefreshQueryHandler getRefreshQueryHandler(String accountId) {
    return new RefreshQueryHandler(
        emrServerlessClientFactory.getClient(accountId),
        jobExecutionResponseReader,
        flintIndexMetadataService,
        leaseManager,
        flintIndexOpFactory,
        metricsService,
        sparkSubmitParametersBuilderProvider);
  }

  public StreamingQueryHandler getStreamingQueryHandler(String accountId) {
    return new StreamingQueryHandler(
        emrServerlessClientFactory.getClient(accountId),
        jobExecutionResponseReader,
        leaseManager,
        metricsService,
        sparkSubmitParametersBuilderProvider);
  }

  public BatchQueryHandler getBatchQueryHandler(String accountId) {
    return new BatchQueryHandler(
        emrServerlessClientFactory.getClient(accountId),
        jobExecutionResponseReader,
        leaseManager,
        metricsService,
        sparkSubmitParametersBuilderProvider);
  }

  public InteractiveQueryHandler getInteractiveQueryHandler() {
    return new InteractiveQueryHandler(
        sessionManager,
        jobExecutionResponseReader,
        leaseManager,
        metricsService,
        sparkSubmitParametersBuilderProvider);
  }

  public IndexDMLHandler getIndexDMLHandler() {
    return new IndexDMLHandler(
        jobExecutionResponseReader,
        flintIndexMetadataService,
        indexDMLResultStorageService,
        flintIndexOpFactory);
  }
}
