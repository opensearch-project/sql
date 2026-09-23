/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.opensearch.storage.scan.OpenSearchIndexScanTest.mockResponse;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.exception.NoCursorException;
import org.opensearch.sql.executor.pagination.Cursor;
import org.opensearch.sql.executor.pagination.PlanSerializer;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ExpressionNodeVisitor;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.expression.env.Environment;
import org.opensearch.sql.monitor.ResourceMonitor;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory;
import org.opensearch.sql.opensearch.executor.pagination.PlanFlattener;
import org.opensearch.sql.opensearch.executor.protector.ResourceMonitorPlan;
import org.opensearch.sql.opensearch.request.OpenSearchQueryRequest;
import org.opensearch.sql.opensearch.request.OpenSearchRequest;
import org.opensearch.sql.opensearch.request.OpenSearchRequestBuilder;
import org.opensearch.sql.opensearch.storage.OpenSearchIndex;
import org.opensearch.sql.opensearch.storage.OpenSearchStorageEngine;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.planner.physical.ProjectOperator;
import org.opensearch.sql.utils.DeserializationFilterUtil;

@ExtendWith(MockitoExtension.class)
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
public class OpenSearchIndexScanPaginationTest {

  public static final OpenSearchRequest.IndexName INDEX_NAME =
      new OpenSearchRequest.IndexName("test");
  public static final int MAX_RESULT_WINDOW = 3;
  public static final TimeValue SCROLL_TIMEOUT = TimeValue.timeValueMinutes(4);
  @Mock private Settings settings;

  @BeforeEach
  void setup() {
    lenient().when(settings.getSettingValue(Settings.Key.QUERY_SIZE_LIMIT)).thenReturn(200);
    lenient().when(settings.getSettingValue(Settings.Key.QUERY_BUCKET_SIZE)).thenReturn(1000);
    lenient()
        .when(settings.getSettingValue(Settings.Key.SQL_CURSOR_KEEP_ALIVE))
        .thenReturn(TimeValue.timeValueMinutes(1));
    lenient().when(settings.getSettingValue(Settings.Key.FIELD_TYPE_TOLERANCE)).thenReturn(true);
    lenient()
        .when(settings.getSettingValue(Settings.Key.DESERIALIZATION_MAX_DEPTH))
        .thenReturn(DeserializationFilterUtil.DEFAULT_MAX_DEPTH);
    lenient()
        .when(settings.getSettingValue(Settings.Key.DESERIALIZATION_MAX_REFS))
        .thenReturn(DeserializationFilterUtil.DEFAULT_MAX_REFS);
    lenient()
        .when(settings.getSettingValue(Settings.Key.DESERIALIZATION_MAX_BYTES))
        .thenReturn(DeserializationFilterUtil.DEFAULT_MAX_BYTES);
  }

  @Mock private OpenSearchClient client;

  private final OpenSearchExprValueFactory exprValueFactory =
      new OpenSearchExprValueFactory(
          Map.of(
              "name", OpenSearchDataType.of(STRING),
              "department", OpenSearchDataType.of(STRING)),
          true);

  @Test
  void query_empty_result() {
    mockResponse(client);
    var builder = new OpenSearchRequestBuilder(exprValueFactory, MAX_RESULT_WINDOW, settings);
    try (var indexScan =
        new OpenSearchIndexScan(client, builder.build(INDEX_NAME, SCROLL_TIMEOUT, client))) {
      indexScan.open();
      assertFalse(indexScan.hasNext());
    }
    verify(client).forceCleanup(any());
  }

  @Test
  void explain_not_implemented() {
    assertThrows(
        Throwable.class,
        () ->
            mock(OpenSearchIndexScan.class, withSettings().defaultAnswer(CALLS_REAL_METHODS))
                .explain());
  }

  @Test
  @SneakyThrows
  void dont_serialize_if_no_cursor() {
    OpenSearchRequest request = mock(OpenSearchRequest.class);
    when(request.hasAnotherBatch()).thenReturn(false);
    try (var indexScan = new OpenSearchIndexScan(client, request)) {
      PlanFlattener flattener = new PlanFlattener();
      assertThrows(NoCursorException.class, () -> flattener.flatten(indexScan, null));
    }
  }

  @Test
  @SneakyThrows
  void round_trip_cursor_with_project_wrapping_index_scan() {
    // Set up a storage engine mock that can reconstruct the plan
    OpenSearchStorageEngine storageEngine = mock(OpenSearchStorageEngine.class);
    when(storageEngine.getClient()).thenReturn(client);
    when(storageEngine.getSettings()).thenReturn(settings);
    OpenSearchIndex index = mock(OpenSearchIndex.class);
    when(index.getFieldOpenSearchTypes())
        .thenReturn(
            Map.of(
                "name", OpenSearchDataType.of(STRING),
                "department", OpenSearchDataType.of(STRING)));
    when(index.isFieldTypeTolerance()).thenReturn(true);
    when(storageEngine.getTable(any(), any())).thenReturn(index);

    // Build a PIT-mode request with a real SearchSourceBuilder
    var searchSourceBuilder = new SearchSourceBuilder().size(4);
    var includes =
        Stream.iterate(1, i -> i + 1).limit(5).map(i -> "col" + i).collect(Collectors.toList());

    var osRequest =
        OpenSearchQueryRequest.pitOf(
            INDEX_NAME, searchSourceBuilder, exprValueFactory, includes, SCROLL_TIMEOUT, "pit123");

    // Simulate search response so hasAnotherBatch() returns true
    var searchResponse = mock(SearchResponse.class);
    when(searchResponse.getAggregations()).thenReturn(null);
    var hits = mock(SearchHits.class);
    when(searchResponse.getHits()).thenReturn(hits);
    SearchHit hit = mock(SearchHit.class);
    when(hit.getSortValues()).thenReturn(new String[] {"sort1"});
    when(hits.getHits()).thenReturn(new SearchHit[] {hit});
    osRequest.search(req -> searchResponse, null);

    // Now hasAnotherBatch() should be true
    assertTrue(osRequest.hasAnotherBatch());

    try (var indexScan = new OpenSearchIndexScan(client, MAX_RESULT_WINDOW, osRequest)) {
      // Create a ProjectOperator wrapping the index scan
      List<NamedExpression> projectList =
          List.of(
              new NamedExpression("name", new ReferenceExpression("name", STRING)),
              new NamedExpression("department", new ReferenceExpression("department", STRING)));
      ProjectOperator projectOp = new ProjectOperator(indexScan, projectList, List.of());

      // Serialize via PlanSerializer
      PlanSerializer serializer = new PlanSerializer(storageEngine);
      Cursor cursor = serializer.convertToCursor(projectOp);

      // Must produce a valid v2 cursor
      assertTrue(cursor.toString().startsWith("n:v2:"));

      // Deserialize
      PhysicalPlan rebuilt = serializer.convertToPlan(cursor.toString());

      // Check structure
      assertTrue(rebuilt instanceof ProjectOperator);
      ProjectOperator rebuiltProject = (ProjectOperator) rebuilt;
      assertEquals(2, rebuiltProject.getProjectList().size());
      assertTrue(rebuiltProject.getChild().get(0) instanceof OpenSearchIndexScan);

      // Continuation state must survive the round-trip, else pagination can't advance/terminate.
      OpenSearchIndexScan rebuiltScan = (OpenSearchIndexScan) rebuiltProject.getChild().get(0);
      assertEquals(MAX_RESULT_WINDOW, rebuiltScan.getMaxResponseSize(), "maxResponseSize dropped");
      OpenSearchQueryRequest rebuiltReq = (OpenSearchQueryRequest) rebuiltScan.getRequest();
      assertEquals("pit123", rebuiltReq.getPitId(), "pitId dropped");
      org.junit.jupiter.api.Assertions.assertArrayEquals(
          new Object[] {"sort1"}, rebuiltReq.getSearchAfter(), "searchAfter dropped/corrupted");
    }
    verify(client).cleanup(osRequest);
    verify(client, org.mockito.Mockito.never()).forceCleanup(osRequest);
  }

  @Test
  @SneakyThrows
  void round_trip_cursor_with_resource_monitor_wrapping_index_scan() {
    // Reproduces the production plan shape: ProjectOperator -> ResourceMonitorPlan -> IndexScan.
    // ResourceMonitorPlan is transparent for getChild() (it returns the delegate's children), so
    // flatten must unwrap via getDelegate(). Otherwise convertToCursor throws
    // IndexOutOfBoundsException, which is swallowed into Cursor.None, and pagination collapses to a
    // single page (the last full page never emits a trailing cursor).
    OpenSearchStorageEngine storageEngine = mock(OpenSearchStorageEngine.class);
    when(storageEngine.getClient()).thenReturn(client);
    when(storageEngine.getSettings()).thenReturn(settings);
    OpenSearchIndex index = mock(OpenSearchIndex.class);
    when(index.getFieldOpenSearchTypes())
        .thenReturn(
            Map.of(
                "name", OpenSearchDataType.of(STRING),
                "department", OpenSearchDataType.of(STRING)));
    when(index.isFieldTypeTolerance()).thenReturn(true);
    when(storageEngine.getTable(any(), any())).thenReturn(index);

    var searchSourceBuilder = new SearchSourceBuilder().size(4);
    var includes =
        Stream.iterate(1, i -> i + 1).limit(5).map(i -> "col" + i).collect(Collectors.toList());
    var osRequest =
        OpenSearchQueryRequest.pitOf(
            INDEX_NAME, searchSourceBuilder, exprValueFactory, includes, SCROLL_TIMEOUT, "pit123");

    var searchResponse = mock(SearchResponse.class);
    when(searchResponse.getAggregations()).thenReturn(null);
    var hits = mock(SearchHits.class);
    when(searchResponse.getHits()).thenReturn(hits);
    SearchHit hit = mock(SearchHit.class);
    when(hit.getSortValues()).thenReturn(new String[] {"sort1"});
    when(hits.getHits()).thenReturn(new SearchHit[] {hit});
    osRequest.search(req -> searchResponse, null);
    assertTrue(osRequest.hasAnotherBatch());

    try (var indexScan = new OpenSearchIndexScan(client, MAX_RESULT_WINDOW, osRequest)) {
      // Wrap the scan in a ResourceMonitorPlan, mirroring the execution protector.
      ResourceMonitor monitor = mock(ResourceMonitor.class);
      ResourceMonitorPlan monitored = new ResourceMonitorPlan(indexScan, monitor);
      List<NamedExpression> projectList =
          List.of(
              new NamedExpression("name", new ReferenceExpression("name", STRING)),
              new NamedExpression("department", new ReferenceExpression("department", STRING)));
      ProjectOperator projectOp = new ProjectOperator(monitored, projectList, List.of());

      PlanSerializer serializer = new PlanSerializer(storageEngine);
      Cursor cursor = serializer.convertToCursor(projectOp);

      // Before the fix this was Cursor.None because the monitor wrapper couldn't be unwrapped.
      assertTrue(
          cursor.toString().startsWith("n:v2:"),
          "ResourceMonitorPlan-wrapped scan must still produce a cursor");

      PhysicalPlan rebuilt = serializer.convertToPlan(cursor.toString());
      assertTrue(rebuilt instanceof ProjectOperator);
      assertTrue(((ProjectOperator) rebuilt).getChild().get(0) instanceof OpenSearchIndexScan);
    }
    verify(client).cleanup(osRequest);
    verify(client, org.mockito.Mockito.never()).forceCleanup(osRequest);
  }

  @Test
  void expression_validation_failure_does_not_transfer_pit_ownership() {
    OpenSearchRequest request = mock(OpenSearchRequest.class);
    when(request.hasAnotherBatch()).thenReturn(true);
    OpenSearchIndexScan indexScan = new OpenSearchIndexScan(client, request);
    ProjectOperator project =
        new ProjectOperator(
            indexScan,
            List.of(new NamedExpression("blocked", new DisallowedExpression())),
            List.of());
    PlanSerializer serializer = new PlanSerializer(mock(OpenSearchStorageEngine.class), settings);

    try {
      assertThrows(IllegalStateException.class, () -> serializer.convertToCursor(project));
    } finally {
      indexScan.close();
    }

    verify(client).forceCleanup(request);
    verify(client, org.mockito.Mockito.never()).cleanup(request);
  }

  private static final class DisallowedExpression implements Expression {
    private static final long serialVersionUID = 1L;

    @Override
    public ExprValue valueOf(Environment<Expression, ExprValue> valueEnv) {
      return null;
    }

    @Override
    public ExprType type() {
      return null;
    }

    @Override
    public <T, C> T accept(ExpressionNodeVisitor<T, C> visitor, C context) {
      return null;
    }
  }
}
