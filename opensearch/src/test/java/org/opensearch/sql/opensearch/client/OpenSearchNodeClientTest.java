/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.client;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
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
import static org.opensearch.sql.opensearch.data.type.OpenSearchDataType.MappingType;

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
import org.junit.jupiter.api.BeforeEach;
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
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.sql.data.model.ExprIntegerValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.data.type.OpenSearchTextType;
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

  private OpenSearchClient client;

  @BeforeEach
  void setUp() {
    this.client = new OpenSearchNodeClient(nodeClient);
  }

  @Test
  void isIndexExist() {
    when(nodeClient.admin().indices()
        .exists(any(IndicesExistsRequest.class)).actionGet())
        .thenReturn(new IndicesExistsResponse(true));

    assertTrue(client.exists("test"));
  }

  @Test
  void isIndexNotExist() {
    String indexName = "test";
    when(nodeClient.admin().indices()
        .exists(any(IndicesExistsRequest.class)).actionGet())
        .thenReturn(new IndicesExistsResponse(false));

    assertFalse(client.exists(indexName));
  }

  @Test
  void isIndexExistWithException() {
    when(nodeClient.admin().indices().exists(any())).thenThrow(RuntimeException.class);

    assertThrows(IllegalStateException.class, () -> client.exists("test"));
  }

  @Test
  void createIndex() {
    String indexName = "test";
    Map<String, Object> mappings = ImmutableMap.of(
        "properties",
        ImmutableMap.of("name", "text"));
    when(nodeClient.admin().indices()
        .create(any(CreateIndexRequest.class)).actionGet())
        .thenReturn(new CreateIndexResponse(true, true, indexName));

    client.createIndex(indexName, mappings);
  }

  @Test
  void createIndexWithException() {
    when(nodeClient.admin().indices().create(any())).thenThrow(RuntimeException.class);

    assertThrows(IllegalStateException.class,
        () -> client.createIndex("test", ImmutableMap.of()));
  }

  @Test
  void getIndexMappings() throws IOException {
    URL url = Resources.getResource(TEST_MAPPING_FILE);
    String mappings = Resources.toString(url, Charsets.UTF_8);
    String indexName = "test";
    mockNodeClientIndicesMappings(indexName, mappings);

    Map<String, IndexMapping> indexMappings = client.getIndexMappings(indexName);

    var mapping = indexMappings.values().iterator().next().getFieldMappings();
    var parsedTypes = OpenSearchDataType.traverseAndFlatten(mapping);
    assertAll(
        () -> assertEquals(1, indexMappings.size()),
        // 10 types extended to 17 after flattening
        () -> assertEquals(10, mapping.size()),
        () -> assertEquals(17, parsedTypes.size()),
        () -> assertEquals("TEXT", mapping.get("address").legacyTypeName()),
        () -> assertEquals(OpenSearchTextType.of(MappingType.Text),
            parsedTypes.get("address")),
        () -> assertEquals("INTEGER", mapping.get("age").legacyTypeName()),
        () -> assertEquals(OpenSearchTextType.of(MappingType.Integer),
            parsedTypes.get("age")),
        () -> assertEquals("DOUBLE", mapping.get("balance").legacyTypeName()),
        () -> assertEquals(OpenSearchTextType.of(MappingType.Double),
            parsedTypes.get("balance")),
        () -> assertEquals("KEYWORD", mapping.get("city").legacyTypeName()),
        () -> assertEquals(OpenSearchTextType.of(MappingType.Keyword),
            parsedTypes.get("city")),
        () -> assertEquals("DATE", mapping.get("birthday").legacyTypeName()),
        () -> assertEquals(OpenSearchTextType.of(MappingType.Date),
            parsedTypes.get("birthday")),
        () -> assertEquals("GEO_POINT", mapping.get("location").legacyTypeName()),
        () -> assertEquals(OpenSearchTextType.of(MappingType.GeoPoint),
            parsedTypes.get("location")),
        // unknown type isn't parsed and ignored
        () -> assertFalse(mapping.containsKey("new_field")),
        () -> assertNull(parsedTypes.get("new_field")),
        () -> assertEquals("TEXT", mapping.get("field with spaces").legacyTypeName()),
        () -> assertEquals(OpenSearchTextType.of(MappingType.Text),
            parsedTypes.get("field with spaces")),
        () -> assertEquals("TEXT", mapping.get("employer").legacyTypeName()),
        () -> assertEquals(OpenSearchTextType.of(MappingType.Text),
            parsedTypes.get("employer")),
        // `employer` is a `text` with `fields`
        () -> assertTrue(((OpenSearchTextType)parsedTypes.get("employer")).getFields().size() > 0),
        () -> assertEquals("NESTED", mapping.get("projects").legacyTypeName()),
        () -> assertEquals(OpenSearchTextType.of(MappingType.Nested),
            parsedTypes.get("projects")),
        () -> assertEquals(OpenSearchTextType.of(MappingType.Boolean),
            parsedTypes.get("projects.active")),
        () -> assertEquals(OpenSearchTextType.of(MappingType.Date),
            parsedTypes.get("projects.release")),
        () -> assertEquals(OpenSearchTextType.of(MappingType.Nested),
            parsedTypes.get("projects.members")),
        () -> assertEquals(OpenSearchTextType.of(MappingType.Text),
            parsedTypes.get("projects.members.name")),
        () -> assertEquals("OBJECT", mapping.get("manager").legacyTypeName()),
        () -> assertEquals(OpenSearchTextType.of(MappingType.Object),
                parsedTypes.get("manager")),
        () -> assertEquals(OpenSearchTextType.of(MappingType.Text),
            parsedTypes.get("manager.name")),
        // `manager.name` is a `text` with `fields`
        () -> assertTrue(((OpenSearchTextType)parsedTypes.get("manager.name"))
                .getFields().size() > 0),
        () -> assertEquals(OpenSearchTextType.of(MappingType.Keyword),
            parsedTypes.get("manager.address")),
        () -> assertEquals(OpenSearchTextType.of(MappingType.Long),
            parsedTypes.get("manager.salary"))
    );
  }

  @Test
  void getIndexMappingsWithEmptyMapping() {
    String indexName = "test";
    mockNodeClientIndicesMappings(indexName, "");
    Map<String, IndexMapping> indexMappings = client.getIndexMappings(indexName);
    assertEquals(1, indexMappings.size());

    IndexMapping indexMapping = indexMappings.values().iterator().next();
    assertEquals(0, indexMapping.size());
  }

  @Test
  void getIndexMappingsWithIOException() {
    String indexName = "test";
    when(nodeClient.admin().indices()).thenThrow(RuntimeException.class);

    assertThrows(IllegalStateException.class, () -> client.getIndexMappings(indexName));
  }

  @Test
  void getIndexMappingsWithNonExistIndex() {
    when(nodeClient.admin().indices()
        .prepareGetMappings(any())
        .setLocal(anyBoolean())
        .get()
    ).thenThrow(IndexNotFoundException.class);

    assertThrows(IndexNotFoundException.class, () -> client.getIndexMappings("non_exist_index"));
  }

  @Test
  void getIndexMaxResultWindows() throws IOException {
    URL url = Resources.getResource(TEST_MAPPING_SETTINGS_FILE);
    String indexMetadata = Resources.toString(url, Charsets.UTF_8);
    String indexName = "accounts";
    mockNodeClientSettings(indexName, indexMetadata);

    Map<String, Integer> indexMaxResultWindows = client.getIndexMaxResultWindows(indexName);
    assertEquals(1, indexMaxResultWindows.size());

    Integer indexMaxResultWindow = indexMaxResultWindows.values().iterator().next();
    assertEquals(100, indexMaxResultWindow);
  }

  @Test
  void getIndexMaxResultWindowsWithDefaultSettings() throws IOException {
    URL url = Resources.getResource(TEST_MAPPING_FILE);
    String indexMetadata = Resources.toString(url, Charsets.UTF_8);
    String indexName = "accounts";
    mockNodeClientSettings(indexName, indexMetadata);

    Map<String, Integer> indexMaxResultWindows = client.getIndexMaxResultWindows(indexName);
    assertEquals(1, indexMaxResultWindows.size());

    Integer indexMaxResultWindow = indexMaxResultWindows.values().iterator().next();
    assertEquals(10000, indexMaxResultWindow);
  }

  @Test
  void getIndexMaxResultWindowsWithIOException() {
    String indexName = "test";
    when(nodeClient.admin().indices()).thenThrow(RuntimeException.class);

    assertThrows(IllegalStateException.class, () -> client.getIndexMaxResultWindows(indexName));
  }

  /** Jacoco enforce this constant lambda be tested. */
  @Test
  void testAllFieldsPredicate() {
    assertTrue(OpenSearchNodeClient.ALL_FIELDS.apply("any_index").test("any_field"));
  }

  @Test
  void search() {
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

    final List<String> indices = client.indices();
    assertEquals(2, indices.size());
  }

  @Test
  void meta() {
    Settings settings = mock(Settings.class);
    when(nodeClient.settings()).thenReturn(settings);
    when(settings.get(anyString(), anyString())).thenReturn("cluster-name");

    final Map<String, String> meta = client.meta();
    assertEquals("cluster-name", meta.get(META_CLUSTER_NAME));
  }

  @Test
  void ml() {
    assertNotNull(client.getNodeClient());
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

  private void mockNodeClientSettings(String indexName, String indexMetadata)
      throws IOException {
    GetSettingsResponse mockResponse = mock(GetSettingsResponse.class);
    when(nodeClient.admin().indices().prepareGetSettings(any()).setLocal(anyBoolean()).get())
        .thenReturn(mockResponse);
    ImmutableOpenMap<String, Settings> metadata =
        new ImmutableOpenMap.Builder<String, Settings>()
            .fPut(indexName, IndexMetadata.fromXContent(createParser(indexMetadata)).getSettings())
            .build();

    when(mockResponse.getIndexToSettings()).thenReturn(metadata);
  }

  private XContentParser createParser(String mappings) throws IOException {
    return XContentType.JSON
        .xContent()
        .createParser(
            NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, mappings);
  }
}
