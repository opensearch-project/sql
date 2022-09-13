/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.opensearch.client.OpenSearchClient.META_CLUSTER_NAME;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
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
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.opensearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.opensearch.action.admin.indices.get.GetIndexResponse;
import org.opensearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.opensearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.opensearch.action.search.ClearScrollRequestBuilder;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.common.collect.ImmutableOpenMap;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.xcontent.DeprecationHandler;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.sql.data.model.ExprIntegerValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory;
import org.opensearch.sql.opensearch.mapping.IndexMapping;
import org.opensearch.sql.opensearch.request.OpenSearchScrollRequest;
import org.opensearch.sql.opensearch.response.OpenSearchResponse;

@ExtendWith(MockitoExtension.class)
class OpenSearchNodeClientTest {

  private static final String TEST_MAPPING_FILE = "mappings/accounts.json";
  private static final String TEST_MAPPING_SETTINGS_FILE = "mappings/accounts2.json";

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
  public void createIndex() {
    String indexName = "test";
    Map<String, Object> mappings = ImmutableMap.of(
        "properties",
        ImmutableMap.of("name", "text"));
    when(nodeClient.admin().indices()
        .exists(new IndicesExistsRequest(indexName)).actionGet())
        .thenReturn(new IndicesExistsResponse(false));
    when(nodeClient.admin().indices()
        .create(new CreateIndexRequest(indexName).mapping(mappings)).actionGet())
        .thenReturn(new CreateIndexResponse(true, true, indexName));
    OpenSearchNodeClient client = new OpenSearchNodeClient(nodeClient);

    assertTrue(client.createIndex(indexName, mappings));
  }

  @Test
  public void createIndexWithException() {
    when(nodeClient.admin().indices()).thenThrow(RuntimeException.class);
    OpenSearchNodeClient client = new OpenSearchNodeClient(nodeClient);
    String indexName = "test";

    assertThrows(IllegalStateException.class,
        () -> client.createIndex(indexName, ImmutableMap.of()));
  }

  @Test
  public void createIndexIfAlreadyExist() {
    when(nodeClient.admin().indices().exists(any()).actionGet())
        .thenReturn(new IndicesExistsResponse(true));
    OpenSearchNodeClient client = new OpenSearchNodeClient(nodeClient);
    String indexName = "test";

    assertFalse(client.createIndex(indexName, ImmutableMap.of()));
  }

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
    when(nodeClient.admin().indices()).thenThrow(RuntimeException.class);
    OpenSearchNodeClient client = new OpenSearchNodeClient(nodeClient);

    assertThrows(IllegalStateException.class, () -> client.getIndexMappings(indexName));
  }

  @Test
  public void getIndexMappingsWithNonExistIndex() {
    OpenSearchNodeClient client =
        new OpenSearchNodeClient(mockNodeClient("test"));
    assertTrue(client.getIndexMappings("non_exist_index").isEmpty());
  }

  @Test
  public void getIndexMaxResultWindows() throws IOException {
    URL url = Resources.getResource(TEST_MAPPING_SETTINGS_FILE);
    String indexMetadata = Resources.toString(url, Charsets.UTF_8);
    String indexName = "accounts";
    OpenSearchNodeClient client =
        new OpenSearchNodeClient(mockNodeClientSettings(indexName, indexMetadata));

    Map<String, Integer> indexMaxResultWindows = client.getIndexMaxResultWindows(indexName);
    assertEquals(1, indexMaxResultWindows.size());

    Integer indexMaxResultWindow = indexMaxResultWindows.values().iterator().next();
    assertEquals(100, indexMaxResultWindow);
  }

  @Test
  public void getIndexMaxResultWindowsWithDefaultSettings() throws IOException {
    URL url = Resources.getResource(TEST_MAPPING_FILE);
    String indexMetadata = Resources.toString(url, Charsets.UTF_8);
    String indexName = "accounts";
    OpenSearchNodeClient client =
        new OpenSearchNodeClient(mockNodeClientSettings(indexName, indexMetadata));

    Map<String, Integer> indexMaxResultWindows = client.getIndexMaxResultWindows(indexName);
    assertEquals(1, indexMaxResultWindows.size());

    Integer indexMaxResultWindow = indexMaxResultWindows.values().iterator().next();
    assertEquals(10000, indexMaxResultWindow);
  }

  @Test
  public void getIndexMaxResultWindowsWithIOException() {
    String indexName = "test";
    when(nodeClient.admin().indices()).thenThrow(RuntimeException.class);
    OpenSearchNodeClient client = new OpenSearchNodeClient(nodeClient);

    assertThrows(IllegalStateException.class, () -> client.getIndexMaxResultWindows(indexName));
  }

  /** Jacoco enforce this constant lambda be tested. */
  @Test
  public void testAllFieldsPredicate() {
    assertTrue(OpenSearchNodeClient.ALL_FIELDS.apply("any_index").test("any_field"));
  }

  @Test
  public void search() {
    OpenSearchNodeClient client =
        new OpenSearchNodeClient(nodeClient);

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
    OpenSearchNodeClient client = new OpenSearchNodeClient(nodeClient);
    AtomicBoolean isRun = new AtomicBoolean(false);
    client.schedule(
        () -> {
          isRun.set(true);
        });
    assertTrue(isRun.get());
  }

  @Test
  void cleanup() {
    ClearScrollRequestBuilder requestBuilder = mock(ClearScrollRequestBuilder.class);
    when(nodeClient.prepareClearScroll()).thenReturn(requestBuilder);
    when(requestBuilder.addScrollId(any())).thenReturn(requestBuilder);
    when(requestBuilder.get()).thenReturn(null);

    OpenSearchNodeClient client = new OpenSearchNodeClient(nodeClient);
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
    OpenSearchNodeClient client = new OpenSearchNodeClient(nodeClient);

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

    OpenSearchNodeClient client = new OpenSearchNodeClient(nodeClient);
    final List<String> indices = client.indices();
    assertEquals(2, indices.size());
  }

  @Test
  void meta() {
    Settings settings = mock(Settings.class);
    when(nodeClient.settings()).thenReturn(settings);
    when(settings.get(anyString(), anyString())).thenReturn("cluster-name");

    OpenSearchNodeClient client = new OpenSearchNodeClient(nodeClient);
    final Map<String, String> meta = client.meta();
    assertEquals("cluster-name", meta.get(META_CLUSTER_NAME));
  }

  @Test
  void ml() {
    OpenSearchNodeClient client = new OpenSearchNodeClient(nodeClient);
    assertNotNull(client.getNodeClient());
  }

  private OpenSearchNodeClient mockClient(String indexName, String mappings) {
    mockNodeClientIndicesMappings(indexName, mappings);
    return new OpenSearchNodeClient(nodeClient);
  }

  public void mockNodeClientIndicesMappings(String indexName, String mappings) {
    GetMappingsResponse mockResponse = mock(GetMappingsResponse.class);
    MappingMetadata emptyMapping = mock(MappingMetadata.class);
    when(nodeClient.admin().indices()
        .prepareGetMappings(any())
        .setLocal(anyBoolean())
        .get()).thenReturn(mockResponse);
    try {
      ImmutableOpenMap<String, MappingMetadata> metadata;
      if (mappings.isEmpty()) {
        when(emptyMapping.getSourceAsMap()).thenReturn(ImmutableMap.of());
        metadata =
            new ImmutableOpenMap.Builder<String, MappingMetadata>()
                .fPut(indexName, emptyMapping)
                .build();
      } else {
        metadata = new ImmutableOpenMap.Builder<String, MappingMetadata>().fPut(indexName,
            IndexMetadata.fromXContent(createParser(mappings)).mapping()).build();
      }
      when(mockResponse.mappings()).thenReturn(metadata);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to mock node client", e);
    }
  }

  public NodeClient mockNodeClient(String indexName) {
    GetMappingsResponse mockResponse = mock(GetMappingsResponse.class);
    when(nodeClient.admin().indices()
        .prepareGetMappings(any())
        .setLocal(anyBoolean())
        .get()).thenReturn(mockResponse);
    when(mockResponse.mappings()).thenReturn(ImmutableOpenMap.of());
    return nodeClient;
  }

  private NodeClient mockNodeClientSettings(String indexName, String indexMetadata)
      throws IOException {
    GetSettingsResponse mockResponse = mock(GetSettingsResponse.class);
    when(nodeClient.admin().indices().prepareGetSettings(any()).setLocal(anyBoolean()).get())
        .thenReturn(mockResponse);
    ImmutableOpenMap<String, Settings> metadata =
        new ImmutableOpenMap.Builder<String, Settings>()
            .fPut(indexName, IndexMetadata.fromXContent(createParser(indexMetadata)).getSettings())
            .build();

    when(mockResponse.getIndexToSettings()).thenReturn(metadata);
    return nodeClient;
  }

  private XContentParser createParser(String mappings) throws IOException {
    return XContentType.JSON
        .xContent()
        .createParser(
            NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, mappings);
  }
}
