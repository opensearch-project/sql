/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.unittest.planner;

import static java.util.Collections.emptyList;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLQueryExpr;
import com.alibaba.druid.sql.parser.ParserException;
import com.alibaba.druid.sql.parser.SQLExprParser;
import com.alibaba.druid.sql.parser.Token;
import java.util.Arrays;
import java.util.List;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.TotalHits.Relation;
import org.junit.Before;
import org.junit.Ignore;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.opensearch.action.search.CreatePitAction;
import org.opensearch.action.search.CreatePitRequest;
import org.opensearch.action.search.CreatePitResponse;
import org.opensearch.action.search.DeletePitAction;
import org.opensearch.action.search.DeletePitRequest;
import org.opensearch.action.search.DeletePitResponse;
import org.opensearch.action.search.SearchAction;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchScrollRequestBuilder;
import org.opensearch.cluster.ClusterName;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.sql.legacy.domain.JoinSelect;
import org.opensearch.sql.legacy.esdomain.LocalClusterState;
import org.opensearch.sql.legacy.exception.SqlParseException;
import org.opensearch.sql.legacy.metrics.Metrics;
import org.opensearch.sql.legacy.parser.ElasticSqlExprParser;
import org.opensearch.sql.legacy.parser.SqlParser;
import org.opensearch.sql.legacy.query.QueryAction;
import org.opensearch.sql.legacy.query.SqlElasticRequestBuilder;
import org.opensearch.sql.legacy.query.join.BackOffRetryStrategy;
import org.opensearch.sql.legacy.query.join.OpenSearchJoinQueryActionFactory;
import org.opensearch.sql.legacy.query.planner.HashJoinQueryPlanRequestBuilder;
import org.opensearch.sql.legacy.query.planner.core.QueryPlanner;
import org.opensearch.sql.legacy.request.SqlRequest;
import org.opensearch.sql.opensearch.setting.OpenSearchSettings;
import org.opensearch.transport.client.Client;

/** Test base class for all query planner tests. */
@Ignore
public abstract class QueryPlannerTest {

  @Mock protected Client client;

  @Mock private SearchResponse response1;
  private static final String PIT_ID1 = "1";

  @Mock private SearchResponse response2;
  private static final String PIT_ID2 = "2";

  @Mock private ClusterSettings clusterSettings;

  /*
  @BeforeClass
  public static void initLogger() {
      ConfigurationBuilder<BuiltConfiguration> builder = newConfigurationBuilder();
      AppenderComponentBuilder appender = builder.newAppender("stdout", "Console");

      LayoutComponentBuilder standard = builder.newLayout("PatternLayout");
      standard.addAttribute("pattern", "%d [%t] %-5level: %msg%n%throwable");
      appender.add(standard);

      RootLoggerComponentBuilder rootLogger = builder.newRootLogger(Level.ERROR);
      rootLogger.add(builder.newAppenderRef("stdout"));

      LoggerComponentBuilder logger = builder.newLogger("org.nlpcn.es4sql.query.planner", Level.TRACE);
      logger.add(builder.newAppenderRef("stdout"));
      //logger.addAttribute("additivity", false);

      builder.add(logger);

      Configurator.initialize(builder.build());
  }
  */

  @Before
  public void init() throws Exception {
    MockitoAnnotations.initMocks(this);
    when(clusterSettings.get(ClusterName.CLUSTER_NAME_SETTING)).thenReturn(ClusterName.DEFAULT);
    OpenSearchSettings settings = spy(new OpenSearchSettings(clusterSettings));

    // Force return empty list to avoid ClusterSettings be invoked which is a final class and hard
    // to mock.
    // In this case, default value in Setting will be returned all the time.
    doReturn(emptyList()).when(settings).getSettings();
    LocalClusterState.state().setPluginSettings(settings);

    Metrics.getInstance().registerDefaultMetrics();
  }

  protected SearchHits query(String sql, MockSearchResponse mockResponse1, MockSearchResponse mockResponse2) {
    when(client.execute(eq(SearchAction.INSTANCE), any())).thenAnswer(invocation -> {
      SearchRequest request = invocation.getArgument(1, SearchRequest.class);
      ActionFuture mockFuture = mock(ActionFuture.class);
      if (request.source().pointInTimeBuilder().getId().equals(PIT_ID1)) {
        when(mockFuture.actionGet()).thenAnswer(mockResponse1);
      } else {
        when(mockFuture.actionGet()).thenAnswer(mockResponse2);
      }
      return mockFuture;
    });

    try (MockedStatic<BackOffRetryStrategy> backOffRetryStrategyMocked =
        Mockito.mockStatic(BackOffRetryStrategy.class)) {
      backOffRetryStrategyMocked.when(BackOffRetryStrategy::isHealthy).thenReturn(true);

      mockCreatePit(PIT_ID1, PIT_ID2);
      mockDeletePit(mockResponse1, mockResponse2);

      List<SearchHit> hits = plan(sql).execute();
      return new SearchHits(
          hits.toArray(new SearchHit[0]), new TotalHits(hits.size(), Relation.EQUAL_TO), 0);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void mockCreatePit(String pitId1, String pitId2) throws Exception {
    ActionFuture<CreatePitResponse> actionFuture1 = mockCreatePitResponse(pitId1);
    ActionFuture<CreatePitResponse> actionFuture2 = mockCreatePitResponse(pitId2);
    when(client.execute(eq(CreatePitAction.INSTANCE), any(CreatePitRequest.class)))
        .thenReturn(actionFuture1)
        .thenReturn(actionFuture2);
  }

  private ActionFuture<CreatePitResponse> mockCreatePitResponse(String pitId) throws Exception {
    ActionFuture<CreatePitResponse> actionFuture = mock(ActionFuture.class);
    CreatePitResponse createPitResponse = mock(CreatePitResponse.class);
    when(createPitResponse.getId()).thenReturn(pitId);
    when(actionFuture.get()).thenReturn(createPitResponse);
    return actionFuture;
  }

  private void mockDeletePit(MockSearchResponse response1, MockSearchResponse response2) throws Exception {
    ActionFuture<DeletePitResponse> actionFuture = mock(ActionFuture.class);
    DeletePitResponse deletePitResponse = mock(DeletePitResponse.class);
    RestStatus restStatus = mock(RestStatus.class);
    when(client.execute(eq(DeletePitAction.INSTANCE), any()))
        .thenAnswer(
            instance -> {
              DeletePitRequest deletePitRequest = instance.getArgument(1, DeletePitRequest.class);
              if (deletePitRequest.getPitIds().getFirst().equals(PIT_ID1)) {
                response1.reset();
              } else if (deletePitRequest.getPitIds().getFirst().equals(PIT_ID2)) {
                response2.reset();
              }
              return actionFuture;
            });
    when(actionFuture.get()).thenReturn(deletePitResponse);
    when(deletePitResponse.status()).thenReturn(restStatus);
    when(restStatus.getStatus()).thenReturn(200);
  }

  protected QueryPlanner plan(String sql) {
    SqlElasticRequestBuilder request = createRequestBuilder(sql);
    if (request instanceof HashJoinQueryPlanRequestBuilder) {
      return ((HashJoinQueryPlanRequestBuilder) request).plan();
    }
    throw new IllegalStateException("Not a JOIN query: " + sql);
  }

  protected SqlElasticRequestBuilder createRequestBuilder(String sql) {
    try {
      SQLQueryExpr sqlExpr = (SQLQueryExpr) toSqlExpr(sql);
      JoinSelect joinSelect = new SqlParser().parseJoinSelect(sqlExpr); // Ignore handleSubquery()
      QueryAction queryAction =
          OpenSearchJoinQueryActionFactory.createJoinAction(client, joinSelect);
      queryAction.setSqlRequest(new SqlRequest(sql, null));
      return queryAction.explain();
    } catch (SqlParseException e) {
      throw new IllegalStateException("Invalid query: " + sql, e);
    }
  }

  private SQLExpr toSqlExpr(String sql) {
    SQLExprParser parser = new ElasticSqlExprParser(sql);
    SQLExpr expr = parser.expr();

    if (parser.getLexer().token() != Token.EOF) {
      throw new ParserException("illegal sql expr : " + sql);
    }
    return expr;
  }

  /** Mock SearchResponse and return each batch in sequence */
  protected static class MockSearchResponse implements Answer<SearchResponse> {

    private final SearchHit[] allHits;

    private final int batchSize; // TODO: should be inferred from mock object dynamically

    private int callCnt;

    MockSearchResponse(SearchHit[] allHits, int batchSize) {
      this.allHits = allHits;
      this.batchSize = batchSize;
    }

    @Override
    public SearchResponse answer(InvocationOnMock invocationOnMock) {
      SearchHit[] curBatch;
      if (isNoMoreBatch()) {
        curBatch = new SearchHit[0];
      } else {
        curBatch = currentBatch();
        callCnt++;
      }

      SearchResponse response = mock(SearchResponse.class);
      when(response.getFailedShards()).thenReturn(0);
      when(response.isTimedOut()).thenReturn(false);
      when(response.getTotalShards()).thenReturn(1);
      when(response.getHits()).thenReturn(new SearchHits(curBatch, new TotalHits(allHits.length, Relation.EQUAL_TO), 0));

      return response;
    }

    private boolean isNoMoreBatch() {
      return callCnt > allHits.length / batchSize;
    }

    private SearchHit[] currentBatch() {
      return Arrays.copyOfRange(allHits, startIndex(), endIndex());
    }

    private int startIndex() {
      return callCnt * batchSize;
    }

    private int endIndex() {
      return Math.min(startIndex() + batchSize, allHits.length);
    }

    private void reset() {
      callCnt = 0;
    }
  }

  protected MockSearchResponse employees(SearchHit... mockHits) {
    return employees(5, mockHits);
  }

  protected MockSearchResponse employees(int pageSize, SearchHit... mockHits) {
    return new MockSearchResponse(mockHits, pageSize);
  }

  protected MockSearchResponse departments(SearchHit... mockHits) {
    return departments(5, mockHits);
  }

  protected MockSearchResponse departments(int pageSize, SearchHit... mockHits) {
    return new MockSearchResponse(mockHits, pageSize);
  }

  protected SearchHit employee(int docId, String lastname, String departmentId) {
    SearchHit hit = new SearchHit(docId);
    if (lastname == null) {
      hit.sourceRef(new BytesArray("{\"departmentId\":\"" + departmentId + "\"}"));
    } else if (departmentId == null) {
      hit.sourceRef(new BytesArray("{\"lastname\":\"" + lastname + "\"}"));
    } else {
      hit.sourceRef(
          new BytesArray(
              "{\"lastname\":\"" + lastname + "\",\"departmentId\":\"" + departmentId + "\"}"));
    }
    hit.sortValues(new Object[] {docId}, new DocValueFormat[] {DocValueFormat.RAW});
    return hit;
  }

  protected SearchHit department(int docId, String id, String name) {
    SearchHit hit = new SearchHit(docId);
    if (id == null) {
      hit.sourceRef(new BytesArray("{\"name\":\"" + name + "\"}"));
    } else if (name == null) {
      hit.sourceRef(new BytesArray("{\"id\":\"" + id + "\"}"));
    } else {
      hit.sourceRef(new BytesArray("{\"id\":\"" + id + "\",\"name\":\"" + name + "\"}"));
    }
    hit.sortValues(new Object[] {docId}, new DocValueFormat[] {DocValueFormat.RAW});
    return hit;
  }
}
