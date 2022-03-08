/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.opensearch.client.OpenSearchClient.META_CLUSTER_NAME;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.io.Resources;
import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.lucene.search.TotalHits;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.action.admin.indices.get.GetIndexResponse;
import org.opensearch.action.search.ClearScrollRequestBuilder;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.cluster.metadata.IndexAbstraction;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.collect.ImmutableOpenMap;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.xcontent.DeprecationHandler;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.sql.data.model.ExprIntegerValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory;
import org.opensearch.sql.opensearch.mapping.IndexMapping;
import org.opensearch.sql.opensearch.request.OpenSearchScrollRequest;
import org.opensearch.sql.opensearch.response.OpenSearchResponse;
import org.opensearch.threadpool.ThreadPool;

@ExtendWith(MockitoExtension.class)
class OpenSearchNodeClientTest {

  private static final String TEST_MAPPING_FILE = "mappings/accounts.json";

  @Mock(answer = RETURNS_DEEP_STUBS)
  private NodeClient nodeClient;

  @Mock
  private OpenSearchExprValueFactory factory;

  @Mock
  private SearchHit searchHit;

  @Mock
  private ThreadContext threadContext;

  @Mock
  private GetIndexResponse indexResponse;

  private ExprTupleValue exprTupleValue = ExprTupleValue.fromExprValueMap(ImmutableMap.of("id",
      new ExprIntegerValue(1)));

  @Test
  public void getIndexMappings() throws IOException {
    URL url = Resources.getResource(TEST_MAPPING_FILE);
    String mappings = Resources.toString(url, Charsets.UTF_8);
    String indexName = "test";
    OpenSearchNodeClient client = mockClient(indexName, mappings);

    Map<String, IndexMapping> indexMappings = client.getIndexMappings(indexName);
    assertEquals(1, indexMappings.size());

    IndexMapping indexMapping = indexMappings.values().iterator().next();
    assertEquals(18, indexMapping.size());
    assertEquals("text", indexMapping.getFieldType("address"));
    assertEquals("integer", indexMapping.getFieldType("age"));
    assertEquals("double", indexMapping.getFieldType("balance"));
    assertEquals("keyword", indexMapping.getFieldType("city"));
    assertEquals("date", indexMapping.getFieldType("birthday"));
    assertEquals("geo_point", indexMapping.getFieldType("location"));
    assertEquals("some_new_es_type_outside_type_system", indexMapping.getFieldType("new_field"));
    assertEquals("text", indexMapping.getFieldType("field with spaces"));
    assertEquals("text_keyword", indexMapping.getFieldType("employer"));
    assertEquals("nested", indexMapping.getFieldType("projects"));
    assertEquals("boolean", indexMapping.getFieldType("projects.active"));
    assertEquals("date", indexMapping.getFieldType("projects.release"));
    assertEquals("nested", indexMapping.getFieldType("projects.members"));
    assertEquals("text", indexMapping.getFieldType("projects.members.name"));
    assertEquals("object", indexMapping.getFieldType("manager"));
    assertEquals("text_keyword", indexMapping.getFieldType("manager.name"));
    assertEquals("keyword", indexMapping.getFieldType("manager.address"));
    assertEquals("long", indexMapping.getFieldType("manager.salary"));
  }

  @Test
  public void getIndexMappingsWithEmptyMapping() {
    String indexName = "test";
    OpenSearchNodeClient client = mockClient(indexName, "");
    Map<String, IndexMapping> indexMappings = client.getIndexMappings(indexName);
    assertEquals(1, indexMappings.size());

    IndexMapping indexMapping = indexMappings.values().iterator().next();
    assertEquals(0, indexMapping.size());
  }

  @Test
  public void getIndexMappingsWithIOException() {
    String indexName = "test";
    ClusterService clusterService = mockClusterService(indexName, new IOException());
    OpenSearchNodeClient client = new OpenSearchNodeClient(clusterService, nodeClient);

    assertThrows(IllegalStateException.class, () -> client.getIndexMappings(indexName));
  }

  @Test
  public void getIndexMappingsWithNonExistIndex() {
    OpenSearchNodeClient client =
        new OpenSearchNodeClient(mockClusterService("test"), nodeClient);

    assertThrows(IndexNotFoundException.class, () -> client.getIndexMappings("non_exist_index"));
  }

  /** Jacoco enforce this constant lambda be tested. */
  @Test
  public void testAllFieldsPredicate() {
    assertTrue(OpenSearchNodeClient.ALL_FIELDS.apply("any_index").test("any_field"));
  }

  @Test
  public void search() {
    OpenSearchNodeClient client =
        new OpenSearchNodeClient(mock(ClusterService.class), nodeClient);

    // Mock first scroll request
    SearchResponse searchResponse = mock(SearchResponse.class);
    when(nodeClient.search(any()).actionGet()).thenReturn(searchResponse);
    when(searchResponse.getScrollId()).thenReturn("scroll123");
    when(searchResponse.getHits())
        .thenReturn(
            new SearchHits(
                new SearchHit[] {searchHit},
                new TotalHits(1L, TotalHits.Relation.EQUAL_TO),
                1.0F));
    when(searchHit.getSourceAsString()).thenReturn("{\"id\", 1}");
    when(factory.construct(any())).thenReturn(exprTupleValue);

    // Mock second scroll request followed
    SearchResponse scrollResponse = mock(SearchResponse.class);
    when(nodeClient.searchScroll(any()).actionGet()).thenReturn(scrollResponse);
    when(scrollResponse.getScrollId()).thenReturn("scroll456");
    when(scrollResponse.getHits()).thenReturn(SearchHits.empty());

    // Verify response for first scroll request
    OpenSearchScrollRequest request = new OpenSearchScrollRequest("test", factory);
    OpenSearchResponse response1 = client.search(request);
    assertFalse(response1.isEmpty());

    Iterator<ExprValue> hits = response1.iterator();
    assertTrue(hits.hasNext());
    assertEquals(exprTupleValue, hits.next());
    assertFalse(hits.hasNext());

    // Verify response for second scroll request
    OpenSearchResponse response2 = client.search(request);
    assertTrue(response2.isEmpty());
  }

  @Test
  void schedule() {
    ThreadPool threadPool = mock(ThreadPool.class);
    when(nodeClient.threadPool()).thenReturn(threadPool);
    when(threadPool.getThreadContext()).thenReturn(threadContext);

    doAnswer(
        invocation -> {
          Runnable task = invocation.getArgument(0);
          task.run();
          return null;
        })
        .when(threadPool)
        .schedule(any(), any(), any());

    OpenSearchNodeClient client =
        new OpenSearchNodeClient(mock(ClusterService.class), nodeClient);
    AtomicBoolean isRun = new AtomicBoolean(false);
    client.schedule(() -> isRun.set(true));
    assertTrue(isRun.get());
  }

  @Test
  void cleanup() {
    ClearScrollRequestBuilder requestBuilder = mock(ClearScrollRequestBuilder.class);
    when(nodeClient.prepareClearScroll()).thenReturn(requestBuilder);
    when(requestBuilder.addScrollId(any())).thenReturn(requestBuilder);
    when(requestBuilder.get()).thenReturn(null);

    OpenSearchNodeClient client =
        new OpenSearchNodeClient(mock(ClusterService.class), nodeClient);
    OpenSearchScrollRequest request = new OpenSearchScrollRequest("test", factory);
    request.setScrollId("scroll123");
    client.cleanup(request);
    assertFalse(request.isScrollStarted());

    InOrder inOrder = Mockito.inOrder(nodeClient, requestBuilder);
    inOrder.verify(nodeClient).prepareClearScroll();
    inOrder.verify(requestBuilder).addScrollId("scroll123");
    inOrder.verify(requestBuilder).get();
  }

  @Test
  void cleanupWithoutScrollId() {
    OpenSearchNodeClient client =
        new OpenSearchNodeClient(mock(ClusterService.class), nodeClient);

    OpenSearchScrollRequest request = new OpenSearchScrollRequest("test", factory);
    client.cleanup(request);
    verify(nodeClient, never()).prepareClearScroll();
  }

  @Test
  void getIndices() {
    AliasMetadata aliasMetadata = mock(AliasMetadata.class);
    ImmutableOpenMap.Builder<String, List<AliasMetadata>> builder = ImmutableOpenMap.builder();
    builder.fPut("index",Arrays.asList(aliasMetadata));
    final ImmutableOpenMap<String, List<AliasMetadata>> openMap = builder.build();
    when(aliasMetadata.alias()).thenReturn("index_alias");
    when(nodeClient.admin().indices()
        .prepareGetIndex()
        .setLocal(true)
        .get()).thenReturn(indexResponse);
    when(indexResponse.getIndices()).thenReturn(new String[] {"index"});
    when(indexResponse.aliases()).thenReturn(openMap);

    OpenSearchNodeClient client =
        new OpenSearchNodeClient(mock(ClusterService.class), nodeClient);
    final List<String> indices = client.indices();
    assertEquals(2, indices.size());
  }

  @Test
  void meta() {
    ClusterName clusterName = mock(ClusterName.class);
    ClusterService mockService = mock(ClusterService.class);
    when(clusterName.value()).thenReturn("cluster-name");
    when(mockService.getClusterName()).thenReturn(clusterName);

    OpenSearchNodeClient client =
        new OpenSearchNodeClient(mockService, nodeClient);
    final Map<String, String> meta = client.meta();
    assertEquals("cluster-name", meta.get(META_CLUSTER_NAME));
  }

  private OpenSearchNodeClient mockClient(String indexName, String mappings) {
    ClusterService clusterService = mockClusterService(indexName, mappings);
    return new OpenSearchNodeClient(clusterService, nodeClient);
  }

  /** Mock getAliasAndIndexLookup() only for index name resolve test. */
  public ClusterService mockClusterService(String indexName) {
    ClusterService mockService = mock(ClusterService.class);
    ClusterState mockState = mock(ClusterState.class);
    Metadata mockMetaData = mock(Metadata.class);

    when(mockService.state()).thenReturn(mockState);
    when(mockState.metadata()).thenReturn(mockMetaData);
    when(mockMetaData.getIndicesLookup())
            .thenReturn(ImmutableSortedMap.of(indexName, mock(IndexAbstraction.class)));
    return mockService;
  }

  public ClusterService mockClusterService(String indexName, String mappings) {
    ClusterService mockService = mock(ClusterService.class);
    ClusterState mockState = mock(ClusterState.class);
    Metadata mockMetaData = mock(Metadata.class);

    when(mockService.state()).thenReturn(mockState);
    when(mockState.metadata()).thenReturn(mockMetaData);
    try {
      ImmutableOpenMap.Builder<String, ImmutableOpenMap<String, MappingMetadata>> builder =
          ImmutableOpenMap.builder();
      ImmutableOpenMap<String, MappingMetadata> metadata;
      if (mappings.isEmpty()) {
        metadata = ImmutableOpenMap.of();
      } else {
        metadata = IndexMetadata.fromXContent(createParser(mappings)).getMappings();
      }
      builder.put(indexName, metadata);
      when(mockMetaData.findMappings(any(), any(), any())).thenReturn(builder.build());

      // IndexNameExpressionResolver use this method to check if index exists. If not,
      // IndexNotFoundException is thrown.
      when(mockMetaData.getIndicesLookup())
              .thenReturn(ImmutableSortedMap.of(indexName, mock(IndexAbstraction.class)));
    } catch (IOException e) {
      throw new IllegalStateException("Failed to mock cluster service", e);
    }
    return mockService;
  }

  public ClusterService mockClusterService(String indexName, Throwable t) {
    ClusterService mockService = mock(ClusterService.class);
    ClusterState mockState = mock(ClusterState.class);
    Metadata mockMetaData = mock(Metadata.class);

    when(mockService.state()).thenReturn(mockState);
    when(mockState.metadata()).thenReturn(mockMetaData);
    try {
      when(mockMetaData.findMappings(any(), any(), any())).thenThrow(t);
      when(mockMetaData.getIndicesLookup())
              .thenReturn(ImmutableSortedMap.of(indexName, mock(IndexAbstraction.class)));
    } catch (IOException e) {
      throw new IllegalStateException("Failed to mock cluster service", e);
    }
    return mockService;
  }

  private XContentParser createParser(String mappings) throws IOException {
    return XContentType.JSON
        .xContent()
        .createParser(
            NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, mappings);
  }
}
