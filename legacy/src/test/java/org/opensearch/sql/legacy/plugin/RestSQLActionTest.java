package org.opensearch.sql.legacy.plugin;

import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Injector;
import org.opensearch.common.inject.ModulesBuilder;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.http.HttpChannel;
import org.opensearch.http.HttpRequest;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.sql.executor.QueryManager;
import org.opensearch.sql.executor.execution.QueryPlanFactory;
import org.opensearch.sql.legacy.esdomain.LocalClusterState;
import org.opensearch.sql.legacy.metrics.MetricFactory;
import org.opensearch.sql.legacy.metrics.MetricName;
import org.opensearch.sql.legacy.metrics.Metrics;
import org.opensearch.sql.opensearch.setting.OpenSearchSettings;
import org.opensearch.sql.sql.SQLService;
import org.opensearch.sql.sql.antlr.SQLSyntaxParser;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.node.NodeClient;

public class RestSQLActionTest {

  private RestSqlAction sqlAction;
  private NodeClient nodeClient;
  private RestChannel restChannel;
  private Injector injector;

  @Before
  public void setup() {
    ThreadPool threadPool = mock(ThreadPool.class);
    when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
    nodeClient = new NodeClient(Settings.EMPTY, threadPool);

    ModulesBuilder modules = new ModulesBuilder();
    modules.add(
        b ->
            b.bind(SQLService.class)
                .toInstance(
                    new SQLService(
                        new SQLSyntaxParser(),
                        mock(QueryManager.class),
                        mock(QueryPlanFactory.class))));
    injector = modules.createInjector();
    sqlAction = new RestSqlAction(Settings.EMPTY, injector);

    restChannel = mock(RestChannel.class);

    Settings coreSettings =
        Settings.builder()
            .put(org.opensearch.sql.common.setting.Settings.Key.SQL_ENABLED.getKeyValue(), true)
            .put(ClusterName.CLUSTER_NAME_SETTING.getKey(), "test-cluster")
            .build();

    Set<org.opensearch.common.settings.Setting<?>> settingsSet =
        new HashSet<>(OpenSearchSettings.pluginSettings());
    settingsSet.add(ClusterName.CLUSTER_NAME_SETTING);

    ClusterSettings clusterSettings = new ClusterSettings(coreSettings, settingsSet);

    ClusterService clusterService = mock(ClusterService.class);
    when(clusterService.getClusterSettings()).thenReturn(clusterSettings);

    OpenSearchSettings pluginSettings = new OpenSearchSettings(clusterSettings);
    LocalClusterState.state().setClusterService(clusterService);
    LocalClusterState.state().setPluginSettings(pluginSettings);

    Metrics.getInstance().clear();
    Metrics.getInstance().registerMetric(MetricFactory.createMetric(MetricName.REQ_TOTAL));
    Metrics.getInstance().registerMetric(MetricFactory.createMetric(MetricName.REQ_COUNT_TOTAL));
    Metrics.getInstance()
        .registerMetric(MetricFactory.createMetric(MetricName.FAILED_REQ_COUNT_CUS));
    Metrics.getInstance()
        .registerMetric(MetricFactory.createMetric(MetricName.FAILED_REQ_COUNT_SYS));
  }

  @Test
  public void arrayQueryTriggersBadRequest() throws Exception {
    String invalidPayload = "{ \"query\": [\"SELECT * FROM index LIMIT 50\"] }";

    HttpRequest httpRequest = mock(HttpRequest.class);
    when(httpRequest.uri()).thenReturn(RestSqlAction.QUERY_API_ENDPOINT);
    when(httpRequest.method()).thenReturn(RestRequest.Method.POST);
    when(httpRequest.getHeaders()).thenReturn(Collections.emptyMap());
    when(httpRequest.content()).thenReturn(new BytesArray(invalidPayload));

    HttpChannel httpChannel = mock(HttpChannel.class);

    RestRequest badRequest =
        RestRequest.request(
            org.opensearch.core.xcontent.NamedXContentRegistry.EMPTY, httpRequest, httpChannel);

    TestableRestSqlAction testableSqlAction = new TestableRestSqlAction(Settings.EMPTY, injector);
    testableSqlAction.runRequest(badRequest, nodeClient, restChannel);

    verify(restChannel)
        .sendResponse(
            argThat(
                response -> {
                  if (response instanceof BytesRestResponse) {
                    String body = ((BytesRestResponse) response).content().utf8ToString();
                    System.out.println("Actual Response Body: " + body);
                    return response.status().getStatus() == 400
                        && body.contains("query")
                        && body.contains("not a string");
                  }
                  return false;
                }));
  }

  private static class TestableRestSqlAction extends RestSqlAction {
    public TestableRestSqlAction(Settings settings, Injector injector) {
      super(settings, injector);
    }

    public void runRequest(RestRequest request, NodeClient client, RestChannel channel)
        throws Exception {
      prepareRequest(request, client).accept(channel);
    }
  }
}
