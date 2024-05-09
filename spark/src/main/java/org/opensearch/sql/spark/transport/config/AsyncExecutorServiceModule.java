/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.transport.config;

import static org.opensearch.sql.spark.execution.statestore.StateStore.ALL_DATASOURCE;

import lombok.RequiredArgsConstructor;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.AbstractModule;
import org.opensearch.common.inject.Provides;
import org.opensearch.common.inject.Singleton;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.legacy.metrics.GaugeMetric;
import org.opensearch.sql.legacy.metrics.Metrics;
import org.opensearch.sql.spark.asyncquery.AsyncQueryExecutorService;
import org.opensearch.sql.spark.asyncquery.AsyncQueryExecutorServiceImpl;
import org.opensearch.sql.spark.asyncquery.AsyncQueryJobMetadataStorageService;
import org.opensearch.sql.spark.asyncquery.OpensearchAsyncQueryJobMetadataStorageService;
import org.opensearch.sql.spark.client.EMRServerlessClientFactory;
import org.opensearch.sql.spark.client.EMRServerlessClientFactoryImpl;
import org.opensearch.sql.spark.config.SparkExecutionEngineConfigSupplier;
import org.opensearch.sql.spark.config.SparkExecutionEngineConfigSupplierImpl;
import org.opensearch.sql.spark.dispatcher.QueryHandlerFactory;
import org.opensearch.sql.spark.dispatcher.SparkQueryDispatcher;
import org.opensearch.sql.spark.execution.session.SessionManager;
import org.opensearch.sql.spark.execution.statestore.StateStore;
import org.opensearch.sql.spark.flint.FlintIndexMetadataServiceImpl;
import org.opensearch.sql.spark.flint.FlintIndexStateModelService;
import org.opensearch.sql.spark.flint.IndexDMLResultStorageService;
import org.opensearch.sql.spark.flint.OpenSearchFlintIndexStateModelService;
import org.opensearch.sql.spark.flint.OpenSearchIndexDMLResultStorageService;
import org.opensearch.sql.spark.flint.operation.FlintIndexOpFactory;
import org.opensearch.sql.spark.leasemanager.DefaultLeaseManager;
import org.opensearch.sql.spark.response.JobExecutionResponseReader;

@RequiredArgsConstructor
public class AsyncExecutorServiceModule extends AbstractModule {

  @Override
  protected void configure() {}

  @Provides
  public AsyncQueryExecutorService asyncQueryExecutorService(
      AsyncQueryJobMetadataStorageService asyncQueryJobMetadataStorageService,
      SparkQueryDispatcher sparkQueryDispatcher,
      SparkExecutionEngineConfigSupplier sparkExecutionEngineConfigSupplier) {
    return new AsyncQueryExecutorServiceImpl(
        asyncQueryJobMetadataStorageService,
        sparkQueryDispatcher,
        sparkExecutionEngineConfigSupplier);
  }

  @Provides
  public AsyncQueryJobMetadataStorageService asyncQueryJobMetadataStorageService(
      StateStore stateStore) {
    return new OpensearchAsyncQueryJobMetadataStorageService(stateStore);
  }

  @Provides
  @Singleton
  public StateStore stateStore(NodeClient client, ClusterService clusterService) {
    StateStore stateStore = new StateStore(client, clusterService);
    registerStateStoreMetrics(stateStore);
    return stateStore;
  }

  @Provides
  public SparkQueryDispatcher sparkQueryDispatcher(
      DataSourceService dataSourceService,
      SessionManager sessionManager,
      QueryHandlerFactory queryHandlerFactory) {
    return new SparkQueryDispatcher(dataSourceService, sessionManager, queryHandlerFactory);
  }

  @Provides
  public QueryHandlerFactory queryhandlerFactory(
      JobExecutionResponseReader jobExecutionResponseReader,
      FlintIndexMetadataServiceImpl flintIndexMetadataReader,
      SessionManager sessionManager,
      DefaultLeaseManager defaultLeaseManager,
      IndexDMLResultStorageService indexDMLResultStorageService,
      FlintIndexOpFactory flintIndexOpFactory,
      EMRServerlessClientFactory emrServerlessClientFactory) {
    return new QueryHandlerFactory(
        jobExecutionResponseReader,
        flintIndexMetadataReader,
        sessionManager,
        defaultLeaseManager,
        indexDMLResultStorageService,
        flintIndexOpFactory,
        emrServerlessClientFactory);
  }

  @Provides
  public FlintIndexOpFactory flintIndexOpFactory(
      FlintIndexStateModelService flintIndexStateModelService,
      NodeClient client,
      FlintIndexMetadataServiceImpl flintIndexMetadataService,
      EMRServerlessClientFactory emrServerlessClientFactory) {
    return new FlintIndexOpFactory(
        flintIndexStateModelService, client, flintIndexMetadataService, emrServerlessClientFactory);
  }

  @Provides
  public FlintIndexStateModelService flintIndexStateModelService(StateStore stateStore) {
    return new OpenSearchFlintIndexStateModelService(stateStore);
  }

  @Provides
  public IndexDMLResultStorageService indexDMLResultStorageService(
      DataSourceService dataSourceService, StateStore stateStore) {
    return new OpenSearchIndexDMLResultStorageService(dataSourceService, stateStore);
  }

  @Provides
  public SessionManager sessionManager(
      StateStore stateStore,
      EMRServerlessClientFactory emrServerlessClientFactory,
      Settings settings) {
    return new SessionManager(stateStore, emrServerlessClientFactory, settings);
  }

  @Provides
  public DefaultLeaseManager defaultLeaseManager(Settings settings, StateStore stateStore) {
    return new DefaultLeaseManager(settings, stateStore);
  }

  @Provides
  public EMRServerlessClientFactory createEMRServerlessClientFactory(
      SparkExecutionEngineConfigSupplier sparkExecutionEngineConfigSupplier) {
    return new EMRServerlessClientFactoryImpl(sparkExecutionEngineConfigSupplier);
  }

  @Provides
  public SparkExecutionEngineConfigSupplier sparkExecutionEngineConfigSupplier(Settings settings) {
    return new SparkExecutionEngineConfigSupplierImpl(settings);
  }

  @Provides
  @Singleton
  public FlintIndexMetadataServiceImpl flintIndexMetadataReader(NodeClient client) {
    return new FlintIndexMetadataServiceImpl(client);
  }

  @Provides
  public JobExecutionResponseReader jobExecutionResponseReader(NodeClient client) {
    return new JobExecutionResponseReader(client);
  }

  private void registerStateStoreMetrics(StateStore stateStore) {
    GaugeMetric<Long> activeSessionMetric =
        new GaugeMetric<>(
            "active_async_query_sessions_count",
            StateStore.activeSessionsCount(stateStore, ALL_DATASOURCE));
    GaugeMetric<Long> activeStatementMetric =
        new GaugeMetric<>(
            "active_async_query_statements_count",
            StateStore.activeStatementsCount(stateStore, ALL_DATASOURCE));
    Metrics.getInstance().registerMetric(activeSessionMetric);
    Metrics.getInstance().registerMetric(activeStatementMetric);
  }
}
