/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.asyncquery;

import static org.opensearch.sql.opensearch.setting.OpenSearchSettings.DATASOURCE_URI_HOSTS_DENY_LIST;
import static org.opensearch.sql.opensearch.setting.OpenSearchSettings.SPARK_EXECUTION_REFRESH_JOB_LIMIT_SETTING;
import static org.opensearch.sql.opensearch.setting.OpenSearchSettings.SPARK_EXECUTION_SESSION_LIMIT_SETTING;
import static org.opensearch.sql.spark.execution.statestore.OpenSearchStateStoreUtil.getIndexName;

import com.amazonaws.services.emrserverless.model.CancelJobRunResult;
import com.amazonaws.services.emrserverless.model.GetJobRunResult;
import com.amazonaws.services.emrserverless.model.JobRun;
import com.amazonaws.services.emrserverless.model.JobRunState;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Resources;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import java.net.URL;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.junit.After;
import org.junit.Before;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.plugins.Plugin;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.sql.common.setting.Settings.Key;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.datasources.encryptor.EncryptorImpl;
import org.opensearch.sql.datasources.glue.GlueDataSourceFactory;
import org.opensearch.sql.datasources.service.DataSourceMetadataStorage;
import org.opensearch.sql.datasources.service.DataSourceServiceImpl;
import org.opensearch.sql.datasources.storage.OpenSearchDataSourceMetadataStorage;
import org.opensearch.sql.legacy.esdomain.LocalClusterState;
import org.opensearch.sql.legacy.metrics.Metrics;
import org.opensearch.sql.opensearch.setting.OpenSearchSettings;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryRequestContext;
import org.opensearch.sql.spark.client.EMRServerlessClient;
import org.opensearch.sql.spark.client.EMRServerlessClientFactory;
import org.opensearch.sql.spark.client.StartJobRequest;
import org.opensearch.sql.spark.config.OpenSearchSparkSubmitParameterModifier;
import org.opensearch.sql.spark.config.SparkExecutionEngineConfig;
import org.opensearch.sql.spark.config.SparkExecutionEngineConfigClusterSettingLoader;
import org.opensearch.sql.spark.dispatcher.DatasourceEmbeddedQueryIdProvider;
import org.opensearch.sql.spark.dispatcher.QueryHandlerFactory;
import org.opensearch.sql.spark.dispatcher.SparkQueryDispatcher;
import org.opensearch.sql.spark.execution.session.DatasourceEmbeddedSessionIdProvider;
import org.opensearch.sql.spark.execution.session.OpenSearchSessionConfigSupplier;
import org.opensearch.sql.spark.execution.session.SessionConfigSupplier;
import org.opensearch.sql.spark.execution.session.SessionIdProvider;
import org.opensearch.sql.spark.execution.session.SessionManager;
import org.opensearch.sql.spark.execution.session.SessionModel;
import org.opensearch.sql.spark.execution.session.SessionState;
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
import org.opensearch.sql.spark.flint.FlintIndexMetadataService;
import org.opensearch.sql.spark.flint.FlintIndexMetadataServiceImpl;
import org.opensearch.sql.spark.flint.FlintIndexStateModelService;
import org.opensearch.sql.spark.flint.FlintIndexType;
import org.opensearch.sql.spark.flint.OpenSearchFlintIndexClient;
import org.opensearch.sql.spark.flint.OpenSearchFlintIndexStateModelService;
import org.opensearch.sql.spark.flint.OpenSearchIndexDMLResultStorageService;
import org.opensearch.sql.spark.flint.operation.FlintIndexOpFactory;
import org.opensearch.sql.spark.leasemanager.DefaultLeaseManager;
import org.opensearch.sql.spark.metrics.OpenSearchMetricsService;
import org.opensearch.sql.spark.parameter.S3GlueDataSourceSparkParameterComposer;
import org.opensearch.sql.spark.parameter.SparkParameterComposerCollection;
import org.opensearch.sql.spark.parameter.SparkSubmitParametersBuilderProvider;
import org.opensearch.sql.spark.response.JobExecutionResponseReader;
import org.opensearch.sql.spark.response.OpenSearchJobExecutionResponseReader;
import org.opensearch.sql.spark.scheduler.AsyncQueryScheduler;
import org.opensearch.sql.spark.scheduler.OpenSearchAsyncQueryScheduler;
import org.opensearch.sql.spark.validator.GrammarElementValidatorFactory;
import org.opensearch.sql.spark.validator.SQLQueryValidator;
import org.opensearch.sql.storage.DataSourceFactory;
import org.opensearch.test.OpenSearchIntegTestCase;

public class AsyncQueryExecutorServiceSpec extends OpenSearchIntegTestCase {

  public static final String MYS3_DATASOURCE = "mys3";
  public static final String MYGLUE_DATASOURCE = "my_glue";
  public static final String ACCOUNT_ID = "accountId";
  public static final String APPLICATION_ID = "appId";
  public static final String REGION = "us-west-2";
  public static final String ROLE_ARN = "roleArn";

  protected ClusterService clusterService;
  protected org.opensearch.sql.common.setting.Settings pluginSettings;
  protected SessionConfigSupplier sessionConfigSupplier;
  protected NodeClient client;
  protected FlintIndexClient flintIndexClient;
  protected DataSourceServiceImpl dataSourceService;
  protected ClusterSettings clusterSettings;
  protected FlintIndexMetadataService flintIndexMetadataService;
  protected FlintIndexStateModelService flintIndexStateModelService;
  protected StateStore stateStore;
  protected SessionStorageService sessionStorageService;
  protected StatementStorageService statementStorageService;
  protected AsyncQueryScheduler asyncQueryScheduler;
  protected AsyncQueryRequestContext asyncQueryRequestContext;
  protected SessionIdProvider sessionIdProvider = new DatasourceEmbeddedSessionIdProvider();

  @Override
  protected Collection<Class<? extends Plugin>> nodePlugins() {
    return Arrays.asList(TestSettingPlugin.class);
  }

  public static class TestSettingPlugin extends Plugin {

    @Override
    public List<Setting<?>> getSettings() {
      return OpenSearchSettings.pluginSettings();
    }
  }

  @Before
  public void setup() {
    clusterService = clusterService();
    clusterSettings = clusterService.getClusterSettings();
    pluginSettings = new OpenSearchSettings(clusterSettings);
    LocalClusterState.state().setClusterService(clusterService);
    LocalClusterState.state().setPluginSettings((OpenSearchSettings) pluginSettings);
    sessionConfigSupplier = new OpenSearchSessionConfigSupplier(pluginSettings);
    Metrics.getInstance().registerDefaultMetrics();
    client = (NodeClient) cluster().client();
    client
        .admin()
        .cluster()
        .prepareUpdateSettings()
        .setTransientSettings(
            Settings.builder()
                .putList(DATASOURCE_URI_HOSTS_DENY_LIST.getKey(), Collections.emptyList())
                .build())
        .get();
    flintIndexClient = new OpenSearchFlintIndexClient(client);
    dataSourceService = createDataSourceService();
    DataSourceMetadata dm =
        new DataSourceMetadata.Builder()
            .setName(MYS3_DATASOURCE)
            .setConnector(DataSourceType.S3GLUE)
            .setProperties(
                ImmutableMap.of(
                    "glue.auth.type",
                    "iam_role",
                    "glue.auth.role_arn",
                    "arn:aws:iam::924196221507:role/FlintOpensearchServiceRole",
                    "glue.indexstore.opensearch.uri",
                    "http://localhost:9200",
                    "glue.indexstore.opensearch.auth",
                    "noauth"))
            .build();
    dataSourceService.createDataSource(dm);
    DataSourceMetadata otherDm =
        new DataSourceMetadata.Builder()
            .setName(MYGLUE_DATASOURCE)
            .setConnector(DataSourceType.S3GLUE)
            .setProperties(
                ImmutableMap.of(
                    "glue.auth.type",
                    "iam_role",
                    "glue.auth.role_arn",
                    "arn:aws:iam::924196221507:role/FlintOpensearchServiceRole",
                    "glue.indexstore.opensearch.uri",
                    "http://localhost:9200",
                    "glue.indexstore.opensearch.auth",
                    "noauth"))
            .build();
    dataSourceService.createDataSource(otherDm);
    stateStore = new StateStore(client, clusterService);
    createIndexWithMappings(dm.getResultIndex(), loadResultIndexMappings());
    createIndexWithMappings(otherDm.getResultIndex(), loadResultIndexMappings());
    flintIndexMetadataService = new FlintIndexMetadataServiceImpl(client);
    flintIndexStateModelService =
        new OpenSearchFlintIndexStateModelService(
            stateStore, new FlintIndexStateModelXContentSerializer());
    sessionStorageService =
        new OpenSearchSessionStorageService(stateStore, new SessionModelXContentSerializer());
    statementStorageService =
        new OpenSearchStatementStorageService(stateStore, new StatementModelXContentSerializer());
    asyncQueryScheduler = new OpenSearchAsyncQueryScheduler(client, clusterService);
  }

  protected FlintIndexOpFactory getFlintIndexOpFactory(
      EMRServerlessClientFactory emrServerlessClientFactory) {
    return new FlintIndexOpFactory(
        flintIndexStateModelService,
        flintIndexClient,
        flintIndexMetadataService,
        emrServerlessClientFactory,
        asyncQueryScheduler);
  }

  @After
  public void clean() {
    client
        .admin()
        .cluster()
        .prepareUpdateSettings()
        .setTransientSettings(
            Settings.builder().putNull(SPARK_EXECUTION_SESSION_LIMIT_SETTING.getKey()).build())
        .get();
    client
        .admin()
        .cluster()
        .prepareUpdateSettings()
        .setTransientSettings(
            Settings.builder().putNull(SPARK_EXECUTION_REFRESH_JOB_LIMIT_SETTING.getKey()).build())
        .get();
    client
        .admin()
        .cluster()
        .prepareUpdateSettings()
        .setTransientSettings(
            Settings.builder().putNull(DATASOURCE_URI_HOSTS_DENY_LIST.getKey()).build())
        .get();
  }

  private DataSourceServiceImpl createDataSourceService() {
    String masterKey = "a57d991d9b573f75b9bba1df";
    DataSourceMetadataStorage dataSourceMetadataStorage =
        new OpenSearchDataSourceMetadataStorage(
            client,
            clusterService,
            new EncryptorImpl(masterKey),
            (OpenSearchSettings) pluginSettings);
    return new DataSourceServiceImpl(
        new ImmutableSet.Builder<DataSourceFactory>()
            .add(new GlueDataSourceFactory(pluginSettings))
            .build(),
        dataSourceMetadataStorage,
        meta -> {});
  }

  protected AsyncQueryExecutorService createAsyncQueryExecutorService(
      EMRServerlessClientFactory emrServerlessClientFactory) {
    return createAsyncQueryExecutorService(
        emrServerlessClientFactory, new OpenSearchJobExecutionResponseReader(client));
  }

  /** Pass a custom response reader which can mock interaction between PPL plugin and EMR-S job. */
  protected AsyncQueryExecutorService createAsyncQueryExecutorService(
      EMRServerlessClientFactory emrServerlessClientFactory,
      JobExecutionResponseReader jobExecutionResponseReader) {
    StateStore stateStore = new StateStore(client, clusterService);
    AsyncQueryJobMetadataStorageService asyncQueryJobMetadataStorageService =
        new OpenSearchAsyncQueryJobMetadataStorageService(
            stateStore, new AsyncQueryJobMetadataXContentSerializer());
    SparkParameterComposerCollection sparkParameterComposerCollection =
        new SparkParameterComposerCollection();
    sparkParameterComposerCollection.register(
        DataSourceType.S3GLUE,
        new S3GlueDataSourceSparkParameterComposer(
            getSparkExecutionEngineConfigClusterSettingLoader()));
    sparkParameterComposerCollection.register(
        DataSourceType.SECURITY_LAKE,
        new S3GlueDataSourceSparkParameterComposer(
            getSparkExecutionEngineConfigClusterSettingLoader()));
    SparkSubmitParametersBuilderProvider sparkSubmitParametersBuilderProvider =
        new SparkSubmitParametersBuilderProvider(sparkParameterComposerCollection);
    QueryHandlerFactory queryHandlerFactory =
        new QueryHandlerFactory(
            jobExecutionResponseReader,
            new FlintIndexMetadataServiceImpl(client),
            new SessionManager(
                sessionStorageService,
                statementStorageService,
                emrServerlessClientFactory,
                sessionConfigSupplier,
                sessionIdProvider),
            new DefaultLeaseManager(pluginSettings, stateStore),
            new OpenSearchIndexDMLResultStorageService(dataSourceService, stateStore),
            new FlintIndexOpFactory(
                flintIndexStateModelService,
                flintIndexClient,
                new FlintIndexMetadataServiceImpl(client),
                emrServerlessClientFactory,
                asyncQueryScheduler),
            emrServerlessClientFactory,
            new OpenSearchMetricsService(),
            sparkSubmitParametersBuilderProvider);
    SQLQueryValidator sqlQueryValidator =
        new SQLQueryValidator(new GrammarElementValidatorFactory());
    SparkQueryDispatcher sparkQueryDispatcher =
        new SparkQueryDispatcher(
            this.dataSourceService,
            new SessionManager(
                sessionStorageService,
                statementStorageService,
                emrServerlessClientFactory,
                sessionConfigSupplier,
                sessionIdProvider),
            queryHandlerFactory,
            new DatasourceEmbeddedQueryIdProvider(),
            sqlQueryValidator);
    return new AsyncQueryExecutorServiceImpl(
        asyncQueryJobMetadataStorageService,
        sparkQueryDispatcher,
        this::sparkExecutionEngineConfig);
  }

  public static class LocalEMRSClient implements EMRServerlessClient {

    private int startJobRunCalled = 0;
    private int cancelJobRunCalled = 0;
    private int getJobResult = 0;
    private JobRunState jobState = JobRunState.RUNNING;

    @Getter private StartJobRequest jobRequest;

    @Override
    public String startJobRun(StartJobRequest startJobRequest) {
      jobRequest = startJobRequest;
      startJobRunCalled++;
      return "jobId";
    }

    @Override
    public GetJobRunResult getJobRunResult(String applicationId, String jobId) {
      getJobResult++;
      JobRun jobRun = new JobRun();
      jobRun.setState(jobState.toString());
      return new GetJobRunResult().withJobRun(jobRun);
    }

    @Override
    public CancelJobRunResult cancelJobRun(
        String applicationId, String jobId, boolean allowExceptionPropagation) {
      cancelJobRunCalled++;
      return new CancelJobRunResult().withJobRunId(jobId);
    }

    public void startJobRunCalled(int expectedTimes) {
      assertEquals(expectedTimes, startJobRunCalled);
    }

    public void cancelJobRunCalled(int expectedTimes) {
      assertEquals(expectedTimes, cancelJobRunCalled);
    }

    public void getJobRunResultCalled(int expectedTimes) {
      assertEquals(expectedTimes, getJobResult);
    }

    public void setJobState(JobRunState jobState) {
      this.jobState = jobState;
    }
  }

  protected LocalEMRSClient getCancelledLocalEmrsClient() {
    return new LocalEMRSClient() {
      public GetJobRunResult getJobRunResult(String applicationId, String jobId) {
        super.getJobRunResult(applicationId, jobId);
        JobRun jobRun = new JobRun();
        jobRun.setState("cancelled");
        return new GetJobRunResult().withJobRun(jobRun);
      }
    };
  }

  public static class LocalEMRServerlessClientFactory implements EMRServerlessClientFactory {

    @Override
    public EMRServerlessClient getClient(String accountId) {
      return new LocalEMRSClient();
    }
  }

  public SparkExecutionEngineConfig sparkExecutionEngineConfig(
      AsyncQueryRequestContext asyncQueryRequestContext) {
    return SparkExecutionEngineConfig.builder()
        .applicationId(APPLICATION_ID)
        .region(REGION)
        .executionRoleARN(ROLE_ARN)
        .sparkSubmitParameterModifier(new OpenSearchSparkSubmitParameterModifier(""))
        .clusterName("myCluster")
        .build();
  }

  public static class TestSettings extends org.opensearch.sql.common.setting.Settings {
    final Map<Key, Object> values;

    public TestSettings() {
      values = new HashMap<>();
    }

    /** Get Setting Value. */
    @Override
    public <T> T getSettingValue(Key key) {
      return (T) values.get(key);
    }

    @Override
    public List<String> getSettings() {
      return values.keySet().stream().map(Key::getKeyValue).collect(Collectors.toList());
    }

    public <T> void putSettingValue(Key key, T value) {
      values.put(key, value);
    }
  }

  public SparkExecutionEngineConfigClusterSettingLoader
      getSparkExecutionEngineConfigClusterSettingLoader() {
    Gson gson = new Gson();
    JsonObject jsonObject = new JsonObject();
    jsonObject.addProperty("accountId", ACCOUNT_ID);
    jsonObject.addProperty("applicationId", APPLICATION_ID);
    jsonObject.addProperty("region", REGION);
    jsonObject.addProperty("executionRoleARN", ROLE_ARN);
    jsonObject.addProperty("sparkSubmitParameters", "");

    // Convert JsonObject to JSON string
    final String jsonString = gson.toJson(jsonObject);

    final TestSettings settings = new TestSettings();
    settings.putSettingValue(Key.SPARK_EXECUTION_ENGINE_CONFIG, jsonString);

    return new SparkExecutionEngineConfigClusterSettingLoader(settings);
  }

  public void enableSession(boolean enabled) {
    // doNothing
  }

  public void setSessionLimit(long limit) {
    client
        .admin()
        .cluster()
        .prepareUpdateSettings()
        .setTransientSettings(
            Settings.builder().put(SPARK_EXECUTION_SESSION_LIMIT_SETTING.getKey(), limit).build())
        .get();
  }

  public void setConcurrentRefreshJob(long limit) {
    client
        .admin()
        .cluster()
        .prepareUpdateSettings()
        .setTransientSettings(
            Settings.builder()
                .put(SPARK_EXECUTION_REFRESH_JOB_LIMIT_SETTING.getKey(), limit)
                .build())
        .get();
  }

  int search(QueryBuilder query) {
    SearchRequest searchRequest = new SearchRequest();
    searchRequest.indices(getIndexName(MYS3_DATASOURCE));
    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    searchSourceBuilder.query(query);
    searchRequest.source(searchSourceBuilder);
    SearchResponse searchResponse = client.search(searchRequest).actionGet();

    return searchResponse.getHits().getHits().length;
  }

  void setSessionState(String sessionId, SessionState sessionState) {
    Optional<SessionModel> model = sessionStorageService.getSession(sessionId, MYS3_DATASOURCE);
    SessionModel updated = sessionStorageService.updateSessionState(model.get(), sessionState);
    assertEquals(sessionState, updated.getSessionState());
  }

  @SneakyThrows
  public String loadResultIndexMappings() {
    URL url = Resources.getResource("query_execution_result_mapping.json");
    return Resources.toString(url, Charsets.UTF_8);
  }

  @RequiredArgsConstructor
  public class FlintDatasetMock {

    final String query;
    final String refreshQuery;
    final FlintIndexType indexType;
    final String indexName;
    boolean isLegacy = false;
    boolean isSpecialCharacter = false;
    String latestId;

    public FlintDatasetMock isLegacy(boolean isLegacy) {
      this.isLegacy = isLegacy;
      return this;
    }

    FlintDatasetMock isSpecialCharacter(boolean isSpecialCharacter) {
      this.isSpecialCharacter = isSpecialCharacter;
      return this;
    }

    public FlintDatasetMock latestId(String latestId) {
      this.latestId = latestId;
      return this;
    }

    public void createIndex() {
      String pathPrefix = isLegacy ? "flint-index-mappings" : "flint-index-mappings/0.1.1";
      if (isSpecialCharacter) {
        createIndexWithMappings(
            indexName, loadMappings(pathPrefix + "/" + "flint_special_character_index.json"));
        return;
      }
      switch (indexType) {
        case SKIPPING:
          createIndexWithMappings(
              indexName, loadMappings(pathPrefix + "/" + "flint_skipping_index.json"));
          break;
        case COVERING:
          createIndexWithMappings(
              indexName, loadMappings(pathPrefix + "/" + "flint_covering_index.json"));
          break;
        case MATERIALIZED_VIEW:
          createIndexWithMappings(indexName, loadMappings(pathPrefix + "/" + "flint_mv.json"));
          break;
      }
    }

    @SneakyThrows
    public void deleteIndex() {
      client().admin().indices().delete(new DeleteIndexRequest().indices(indexName)).get();
    }
  }

  @SneakyThrows
  public static String loadMappings(String path) {
    URL url = Resources.getResource(path);
    return Resources.toString(url, Charsets.UTF_8);
  }

  public void createIndexWithMappings(String indexName, String metadata) {
    CreateIndexRequest request = new CreateIndexRequest(indexName);
    request.mapping(metadata, XContentType.JSON);
    client().admin().indices().create(request).actionGet();
  }
}
