/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.asyncquery;

import static org.opensearch.sql.opensearch.setting.OpenSearchSettings.DATASOURCE_URI_HOSTS_DENY_LIST;
import static org.opensearch.sql.opensearch.setting.OpenSearchSettings.SPARK_EXECUTION_REFRESH_JOB_LIMIT_SETTING;
import static org.opensearch.sql.opensearch.setting.OpenSearchSettings.SPARK_EXECUTION_SESSION_LIMIT_SETTING;
import static org.opensearch.sql.spark.execution.statestore.StateStore.DATASOURCE_TO_REQUEST_INDEX;
import static org.opensearch.sql.spark.execution.statestore.StateStore.getSession;
import static org.opensearch.sql.spark.execution.statestore.StateStore.updateSessionState;

import com.amazonaws.services.emrserverless.model.CancelJobRunResult;
import com.amazonaws.services.emrserverless.model.GetJobRunResult;
import com.amazonaws.services.emrserverless.model.JobRun;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Resources;
import java.net.URL;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
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
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.datasources.auth.DataSourceUserAuthorizationHelperImpl;
import org.opensearch.sql.datasources.encryptor.EncryptorImpl;
import org.opensearch.sql.datasources.glue.GlueDataSourceFactory;
import org.opensearch.sql.datasources.service.DataSourceMetadataStorage;
import org.opensearch.sql.datasources.service.DataSourceServiceImpl;
import org.opensearch.sql.datasources.storage.OpenSearchDataSourceMetadataStorage;
import org.opensearch.sql.legacy.esdomain.LocalClusterState;
import org.opensearch.sql.legacy.metrics.Metrics;
import org.opensearch.sql.opensearch.setting.OpenSearchSettings;
import org.opensearch.sql.spark.client.EMRServerlessClient;
import org.opensearch.sql.spark.client.StartJobRequest;
import org.opensearch.sql.spark.config.SparkExecutionEngineConfig;
import org.opensearch.sql.spark.dispatcher.SparkQueryDispatcher;
import org.opensearch.sql.spark.execution.session.SessionManager;
import org.opensearch.sql.spark.execution.session.SessionModel;
import org.opensearch.sql.spark.execution.session.SessionState;
import org.opensearch.sql.spark.execution.statestore.StateStore;
import org.opensearch.sql.spark.flint.FlintIndexMetadataReaderImpl;
import org.opensearch.sql.spark.leasemanager.DefaultLeaseManager;
import org.opensearch.sql.spark.response.JobExecutionResponseReader;
import org.opensearch.sql.storage.DataSourceFactory;
import org.opensearch.test.OpenSearchIntegTestCase;

public class AsyncQueryExecutorServiceSpec extends OpenSearchIntegTestCase {
  public static final String DATASOURCE = "mys3";
  public static final String DSOTHER = "mytest";

  protected ClusterService clusterService;
  protected org.opensearch.sql.common.setting.Settings pluginSettings;
  protected NodeClient client;
  protected DataSourceServiceImpl dataSourceService;
  protected StateStore stateStore;
  protected ClusterSettings clusterSettings;

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
    dataSourceService = createDataSourceService();
    DataSourceMetadata dm =
        new DataSourceMetadata(
            DATASOURCE,
            StringUtils.EMPTY,
            DataSourceType.S3GLUE,
            ImmutableList.of(),
            ImmutableMap.of(
                "glue.auth.type",
                "iam_role",
                "glue.auth.role_arn",
                "arn:aws:iam::924196221507:role/FlintOpensearchServiceRole",
                "glue.indexstore.opensearch.uri",
                "http://localhost:9200",
                "glue.indexstore.opensearch.auth",
                "noauth"),
            null);
    dataSourceService.createDataSource(dm);
    DataSourceMetadata otherDm =
        new DataSourceMetadata(
            DSOTHER,
            StringUtils.EMPTY,
            DataSourceType.S3GLUE,
            ImmutableList.of(),
            ImmutableMap.of(
                "glue.auth.type",
                "iam_role",
                "glue.auth.role_arn",
                "arn:aws:iam::924196221507:role/FlintOpensearchServiceRole",
                "glue.indexstore.opensearch.uri",
                "http://localhost:9200",
                "glue.indexstore.opensearch.auth",
                "noauth"),
            null);
    dataSourceService.createDataSource(otherDm);
    stateStore = new StateStore(client, clusterService);
    createIndexWithMappings(dm.getResultIndex(), loadResultIndexMappings());
    createIndexWithMappings(otherDm.getResultIndex(), loadResultIndexMappings());
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
            client, clusterService, new EncryptorImpl(masterKey));
    return new DataSourceServiceImpl(
        new ImmutableSet.Builder<DataSourceFactory>()
            .add(new GlueDataSourceFactory(pluginSettings))
            .build(),
        dataSourceMetadataStorage,
        meta -> {});
  }

  protected AsyncQueryExecutorService createAsyncQueryExecutorService(
      EMRServerlessClient emrServerlessClient) {
    StateStore stateStore = new StateStore(client, clusterService);
    AsyncQueryJobMetadataStorageService asyncQueryJobMetadataStorageService =
        new OpensearchAsyncQueryJobMetadataStorageService(stateStore);
    JobExecutionResponseReader jobExecutionResponseReader = createJobExecutionResponseReader();
    SparkQueryDispatcher sparkQueryDispatcher =
        new SparkQueryDispatcher(
            emrServerlessClient,
            this.dataSourceService,
            new DataSourceUserAuthorizationHelperImpl(client),
            jobExecutionResponseReader,
            new FlintIndexMetadataReaderImpl(client),
            client,
            new SessionManager(stateStore, emrServerlessClient, pluginSettings),
            new DefaultLeaseManager(pluginSettings, stateStore),
            stateStore);
    return new AsyncQueryExecutorServiceImpl(
        asyncQueryJobMetadataStorageService,
        sparkQueryDispatcher,
        this::sparkExecutionEngineConfig);
  }

  protected JobExecutionResponseReader createJobExecutionResponseReader() {
    return new JobExecutionResponseReader(client);
  }

  public static class LocalEMRSClient implements EMRServerlessClient {

    private int startJobRunCalled = 0;
    private int cancelJobRunCalled = 0;
    private int getJobResult = 0;

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
      jobRun.setState("RUNNING");
      return new GetJobRunResult().withJobRun(jobRun);
    }

    @Override
    public CancelJobRunResult cancelJobRun(String applicationId, String jobId) {
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
  }

  public SparkExecutionEngineConfig sparkExecutionEngineConfig() {
    return new SparkExecutionEngineConfig("appId", "us-west-2", "roleArn", "", "myCluster");
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
    searchRequest.indices(DATASOURCE_TO_REQUEST_INDEX.apply(DATASOURCE));
    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    searchSourceBuilder.query(query);
    searchRequest.source(searchSourceBuilder);
    SearchResponse searchResponse = client.search(searchRequest).actionGet();

    return searchResponse.getHits().getHits().length;
  }

  void setSessionState(String sessionId, SessionState sessionState) {
    Optional<SessionModel> model = getSession(stateStore, DATASOURCE).apply(sessionId);
    SessionModel updated =
        updateSessionState(stateStore, DATASOURCE).apply(model.get(), sessionState);
    assertEquals(sessionState, updated.getSessionState());
  }

  @SneakyThrows
  public String loadResultIndexMappings() {
    URL url = Resources.getResource("query_execution_result_mapping.json");
    return Resources.toString(url, Charsets.UTF_8);
  }

  public void createIndexWithMappings(String indexName, String metadata) {
    CreateIndexRequest request = new CreateIndexRequest(indexName);
    request.mapping(metadata, XContentType.JSON);
    client().admin().indices().create(request).actionGet();
  }
}
