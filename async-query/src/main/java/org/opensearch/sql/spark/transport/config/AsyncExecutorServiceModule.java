/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.transport.config;

import static org.opensearch.sql.spark.execution.statestore.StateStore.ALL_DATASOURCE;

import com.google.common.collect.ImmutableMap;
import lombok.RequiredArgsConstructor;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.AbstractModule;
import org.opensearch.common.inject.Provides;
import org.opensearch.common.inject.Singleton;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.legacy.metrics.GaugeMetric;
import org.opensearch.sql.legacy.metrics.Metrics;
import org.opensearch.sql.spark.asyncquery.AsyncQueryExecutorService;
import org.opensearch.sql.spark.asyncquery.AsyncQueryExecutorServiceImpl;
import org.opensearch.sql.spark.asyncquery.AsyncQueryJobMetadataStorageService;
import org.opensearch.sql.spark.asyncquery.OpenSearchAsyncQueryJobMetadataStorageService;
import org.opensearch.sql.spark.client.EMRServerlessClientFactory;
import org.opensearch.sql.spark.client.EMRServerlessClientFactoryImpl;
import org.opensearch.sql.spark.config.OpenSearchAsyncQuerySchedulerConfigComposer;
import org.opensearch.sql.spark.config.OpenSearchExtraParameterComposer;
import org.opensearch.sql.spark.config.SparkExecutionEngineConfigClusterSettingLoader;
import org.opensearch.sql.spark.config.SparkExecutionEngineConfigSupplier;
import org.opensearch.sql.spark.config.SparkExecutionEngineConfigSupplierImpl;
import org.opensearch.sql.spark.dispatcher.DatasourceEmbeddedQueryIdProvider;
import org.opensearch.sql.spark.dispatcher.QueryHandlerFactory;
import org.opensearch.sql.spark.dispatcher.QueryIdProvider;
import org.opensearch.sql.spark.dispatcher.SparkQueryDispatcher;
import org.opensearch.sql.spark.execution.session.DatasourceEmbeddedSessionIdProvider;
import org.opensearch.sql.spark.execution.session.OpenSearchSessionConfigSupplier;
import org.opensearch.sql.spark.execution.session.SessionConfigSupplier;
import org.opensearch.sql.spark.execution.session.SessionManager;
import org.opensearch.sql.spark.execution.statestore.OpenSearchSessionStorageService;
import org.opensearch.sql.spark.execution.statestore.OpenSearchStatementStorageService;
import org.opensearch.sql.spark.execution.statestore.SessionStorageService;
import org.opensearch.sql.spark.execution.statestore.StateStore;
import org.opensearch.sql.spark.execution.statestore.StatementStorageService;
import org.opensearch.sql.spark.execution.xcontent.AsyncQueryJobMetadataXContentSerializer;
import org.opensearch.sql.spark.execution.xcontent.FlintIndexStateModelXContentSerializer;
import org.opensearch.sql.spark.execution.xcontent.SessionModelXContentSerializer;
import org.opensearch.sql.spark.execution.xcontent.StatementModelXContentSerializer;
import org.opensearch.sql.spark.flint.FlintIndexClient;
import org.opensearch.sql.spark.flint.FlintIndexMetadataServiceImpl;
import org.opensearch.sql.spark.flint.FlintIndexStateModelService;
import org.opensearch.sql.spark.flint.IndexDMLResultStorageService;
import org.opensearch.sql.spark.flint.OpenSearchFlintIndexClient;
import org.opensearch.sql.spark.flint.OpenSearchFlintIndexStateModelService;
import org.opensearch.sql.spark.flint.OpenSearchIndexDMLResultStorageService;
import org.opensearch.sql.spark.flint.operation.FlintIndexOpFactory;
import org.opensearch.sql.spark.leasemanager.DefaultLeaseManager;
import org.opensearch.sql.spark.metrics.MetricsService;
import org.opensearch.sql.spark.metrics.OpenSearchMetricsService;
import org.opensearch.sql.spark.parameter.S3GlueDataSourceSparkParameterComposer;
import org.opensearch.sql.spark.parameter.SparkParameterComposerCollection;
import org.opensearch.sql.spark.parameter.SparkSubmitParametersBuilderProvider;
import org.opensearch.sql.spark.response.JobExecutionResponseReader;
import org.opensearch.sql.spark.response.OpenSearchJobExecutionResponseReader;
import org.opensearch.sql.spark.scheduler.AsyncQueryScheduler;
import org.opensearch.sql.spark.scheduler.OpenSearchAsyncQueryScheduler;
import org.opensearch.sql.spark.validator.DefaultGrammarElementValidator;
import org.opensearch.sql.spark.validator.GrammarElementValidatorProvider;
import org.opensearch.sql.spark.validator.PPLQueryValidator;
import org.opensearch.sql.spark.validator.S3GlueSQLGrammarElementValidator;
import org.opensearch.sql.spark.validator.SQLQueryValidator;
import org.opensearch.sql.spark.validator.SecurityLakeSQLGrammarElementValidator;
import org.opensearch.transport.client.node.NodeClient;

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
      StateStore stateStore, AsyncQueryJobMetadataXContentSerializer serializer) {
    return new OpenSearchAsyncQueryJobMetadataStorageService(stateStore, serializer);
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
      QueryHandlerFactory queryHandlerFactory,
      QueryIdProvider queryIdProvider,
      SQLQueryValidator sqlQueryValidator,
      PPLQueryValidator pplQueryValidator) {
    return new SparkQueryDispatcher(
        dataSourceService,
        sessionManager,
        queryHandlerFactory,
        queryIdProvider,
        sqlQueryValidator,
        pplQueryValidator);
  }

  @Provides
  public QueryIdProvider queryIdProvider() {
    return new DatasourceEmbeddedQueryIdProvider();
  }

  @Provides
  public QueryHandlerFactory queryhandlerFactory(
      JobExecutionResponseReader openSearchJobExecutionResponseReader,
      FlintIndexMetadataServiceImpl flintIndexMetadataReader,
      SessionManager sessionManager,
      DefaultLeaseManager defaultLeaseManager,
      IndexDMLResultStorageService indexDMLResultStorageService,
      FlintIndexOpFactory flintIndexOpFactory,
      EMRServerlessClientFactory emrServerlessClientFactory,
      MetricsService metricsService,
      SparkSubmitParametersBuilderProvider sparkSubmitParametersBuilderProvider) {
    return new QueryHandlerFactory(
        openSearchJobExecutionResponseReader,
        flintIndexMetadataReader,
        sessionManager,
        defaultLeaseManager,
        indexDMLResultStorageService,
        flintIndexOpFactory,
        emrServerlessClientFactory,
        metricsService,
        sparkSubmitParametersBuilderProvider);
  }

  @Provides
  public FlintIndexOpFactory flintIndexOpFactory(
      FlintIndexStateModelService flintIndexStateModelService,
      FlintIndexClient flintIndexClient,
      FlintIndexMetadataServiceImpl flintIndexMetadataService,
      EMRServerlessClientFactory emrServerlessClientFactory,
      AsyncQueryScheduler asyncQueryScheduler) {
    return new FlintIndexOpFactory(
        flintIndexStateModelService,
        flintIndexClient,
        flintIndexMetadataService,
        emrServerlessClientFactory,
        asyncQueryScheduler);
  }

  @Provides
  public FlintIndexClient flintIndexClient(NodeClient nodeClient) {
    return new OpenSearchFlintIndexClient(nodeClient);
  }

  @Provides
  public FlintIndexStateModelService flintIndexStateModelService(
      StateStore stateStore, FlintIndexStateModelXContentSerializer serializer) {
    return new OpenSearchFlintIndexStateModelService(stateStore, serializer);
  }

  @Provides
  public SparkSubmitParametersBuilderProvider sparkSubmitParametersBuilderProvider(
      Settings settings, SparkExecutionEngineConfigClusterSettingLoader clusterSettingLoader) {
    SparkParameterComposerCollection collection = new SparkParameterComposerCollection();
    collection.register(
        DataSourceType.S3GLUE, new S3GlueDataSourceSparkParameterComposer(clusterSettingLoader));
    collection.register(
        DataSourceType.SECURITY_LAKE,
        new S3GlueDataSourceSparkParameterComposer(clusterSettingLoader));
    collection.register(new OpenSearchAsyncQuerySchedulerConfigComposer(settings));
    collection.register(new OpenSearchExtraParameterComposer(clusterSettingLoader));
    return new SparkSubmitParametersBuilderProvider(collection);
  }

  @Provides
  public SQLQueryValidator sqlQueryValidator() {
    GrammarElementValidatorProvider validatorProvider =
        new GrammarElementValidatorProvider(
            ImmutableMap.of(
                DataSourceType.S3GLUE,
                new S3GlueSQLGrammarElementValidator(),
                DataSourceType.SECURITY_LAKE,
                new SecurityLakeSQLGrammarElementValidator()),
            new DefaultGrammarElementValidator());
    return new SQLQueryValidator(validatorProvider);
  }

  @Provides
  public PPLQueryValidator pplQueryValidator() {
    GrammarElementValidatorProvider validatorProvider =
        new GrammarElementValidatorProvider(
            ImmutableMap.of(), new DefaultGrammarElementValidator());
    return new PPLQueryValidator(validatorProvider);
  }

  @Provides
  public IndexDMLResultStorageService indexDMLResultStorageService(
      DataSourceService dataSourceService, StateStore stateStore) {
    return new OpenSearchIndexDMLResultStorageService(dataSourceService, stateStore);
  }

  @Provides
  public SessionManager sessionManager(
      SessionStorageService sessionStorageService,
      StatementStorageService statementStorageService,
      EMRServerlessClientFactory emrServerlessClientFactory,
      SessionConfigSupplier sessionConfigSupplier) {
    return new SessionManager(
        sessionStorageService,
        statementStorageService,
        emrServerlessClientFactory,
        sessionConfigSupplier,
        new DatasourceEmbeddedSessionIdProvider());
  }

  @Provides
  public SessionStorageService sessionStorageService(
      StateStore stateStore, SessionModelXContentSerializer serializer) {
    return new OpenSearchSessionStorageService(stateStore, serializer);
  }

  @Provides
  public StatementStorageService statementStorageService(
      StateStore stateStore, StatementModelXContentSerializer serializer) {
    return new OpenSearchStatementStorageService(stateStore, serializer);
  }

  @Provides
  public DefaultLeaseManager defaultLeaseManager(Settings settings, StateStore stateStore) {
    return new DefaultLeaseManager(settings, stateStore);
  }

  @Provides
  public EMRServerlessClientFactory createEMRServerlessClientFactory(
      SparkExecutionEngineConfigSupplier sparkExecutionEngineConfigSupplier,
      MetricsService metricsService) {
    return new EMRServerlessClientFactoryImpl(sparkExecutionEngineConfigSupplier, metricsService);
  }

  @Provides
  public MetricsService metricsService() {
    return new OpenSearchMetricsService();
  }

  @Provides
  public SparkExecutionEngineConfigSupplier sparkExecutionEngineConfigSupplier(
      Settings settings, SparkExecutionEngineConfigClusterSettingLoader clusterSettingLoader) {
    return new SparkExecutionEngineConfigSupplierImpl(settings, clusterSettingLoader);
  }

  @Provides
  public SparkExecutionEngineConfigClusterSettingLoader
      sparkExecutionEngineConfigClusterSettingLoader(Settings settings) {
    return new SparkExecutionEngineConfigClusterSettingLoader(settings);
  }

  @Provides
  @Singleton
  public FlintIndexMetadataServiceImpl flintIndexMetadataReader(NodeClient client) {
    return new FlintIndexMetadataServiceImpl(client);
  }

  @Provides
  public JobExecutionResponseReader jobExecutionResponseReader(NodeClient client) {
    return new OpenSearchJobExecutionResponseReader(client);
  }

  @Provides
  public SessionConfigSupplier sessionConfigSupplier(Settings settings) {
    return new OpenSearchSessionConfigSupplier(settings);
  }

  @Provides
  @Singleton
  public AsyncQueryScheduler asyncQueryScheduler(NodeClient client, ClusterService clusterService) {
    OpenSearchAsyncQueryScheduler scheduler =
        new OpenSearchAsyncQueryScheduler(client, clusterService);
    return scheduler;
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
