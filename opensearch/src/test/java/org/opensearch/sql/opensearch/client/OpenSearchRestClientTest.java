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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.lucene.search.TotalHits;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.action.admin.cluster.settings.ClusterGetSettingsResponse;
import org.opensearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.opensearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexResponse;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.client.indices.GetIndexResponse;
import org.opensearch.client.indices.GetMappingsRequest;
import org.opensearch.client.indices.GetMappingsResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.common.collect.ImmutableOpenMap;
import org.opensearch.common.settings.Settings;
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
class OpenSearchRestClientTest {

  private static final String TEST_MAPPING_FILE = "mappings/accounts.json";

  @Mock(answer = RETURNS_DEEP_STUBS)
  private RestHighLevelClient restClient;

  private OpenSearchClient client;

  @Mock
  private OpenSearchExprValueFactory factory;

  @Mock
  private SearchHit searchHit;

  @Mock
  private GetIndexResponse getIndexResponse;

  private ExprTupleValue exprTupleValue = ExprTupleValue.fromExprValueMap(ImmutableMap.of("id",
      new ExprIntegerValue(1)));

  @BeforeEach
  void setUp() {
    client = new OpenSearchRestClient(restClient);
  }

  @Test
  void isIndexExist() throws IOException {
    when(restClient.indices()
        .exists(any(), any())) // use any() because missing equals() in GetIndexRequest
        .thenReturn(true);

    assertTrue(client.exists("test"));
  }

  @Test
  void isIndexNotExist() throws IOException {
    when(restClient.indices()
        .exists(any(), any())) // use any() because missing equals() in GetIndexRequest
        .thenReturn(false);

    assertFalse(client.exists("test"));
  }

  @Test
  void isIndexExistWithException() throws IOException {
    when(restClient.indices().exists(any(), any())).thenThrow(IOException.class);

    assertThrows(IllegalStateException.class, () -> client.exists("test"));
  }

  @Test
  void createIndex() throws IOException {
    String indexName = "test";
    Map<String, Object> mappings = ImmutableMap.of(
        "properties",
        ImmutableMap.of("name", "text"));
    when(restClient.indices()
        .create(any(), any()))
        .thenReturn(new CreateIndexResponse(true, true, indexName));

    client.createIndex(indexName, mappings);
  }

  @Test
  void createIndexWithIOException() throws IOException {
    when(restClient.indices().create(any(), any())).thenThrow(IOException.class);

    assertThrows(IllegalStateException.class,
        () -> client.createIndex("test", ImmutableMap.of()));
  }

  @Test
  void getIndexMappings() throws IOException {
    URL url = Resources.getResource(TEST_MAPPING_FILE);
    String mappings = Resources.toString(url, Charsets.UTF_8);
    String indexName = "test";

    GetMappingsResponse response = mock(GetMappingsResponse.class);
    when(response.mappings()).thenReturn(mockFieldMappings(indexName, mappings));
    when(restClient.indices().getMapping(any(GetMappingsRequest.class), any()))
        .thenReturn(response);

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
  void getIndexMappingsWithIOException() throws IOException {
    when(restClient.indices().getMapping(any(GetMappingsRequest.class), any()))
        .thenThrow(new IOException());
    assertThrows(IllegalStateException.class, () -> client.getIndexMappings("test"));
  }

  @Test
  void getIndexMaxResultWindowsSettings() throws IOException {
    String indexName = "test";
    Integer maxResultWindow = 1000;

    GetSettingsResponse response = mock(GetSettingsResponse.class);
    Settings maxResultWindowSettings = Settings.builder()
        .put("index.max_result_window", maxResultWindow)
        .build();
    Settings emptySettings = Settings.builder().build();
    ImmutableOpenMap<String, Settings> indexToSettings =
        mockSettings(indexName, maxResultWindowSettings);
    ImmutableOpenMap<String, Settings> indexToDefaultSettings =
        mockSettings(indexName, emptySettings);
    when(response.getIndexToSettings()).thenReturn(indexToSettings);
    when(response.getIndexToDefaultSettings()).thenReturn(indexToDefaultSettings);
    when(restClient.indices().getSettings(any(GetSettingsRequest.class), any()))
        .thenReturn(response);

    Map<String, Integer> indexMaxResultWindows = client.getIndexMaxResultWindows(indexName);
    assertEquals(1, indexMaxResultWindows.size());
    assertEquals(maxResultWindow, indexMaxResultWindows.values().iterator().next());
  }

  @Test
  void getIndexMaxResultWindowsDefaultSettings() throws IOException {
    String indexName = "test";
    Integer maxResultWindow = 10000;

    GetSettingsResponse response = mock(GetSettingsResponse.class);
    Settings maxResultWindowSettings = Settings.builder()
        .put("index.max_result_window", maxResultWindow)
        .build();
    Settings emptySettings = Settings.builder().build();
    ImmutableOpenMap<String, Settings> indexToSettings =
        mockSettings(indexName, emptySettings);
    ImmutableOpenMap<String, Settings> indexToDefaultSettings =
        mockSettings(indexName, maxResultWindowSettings);
    when(response.getIndexToSettings()).thenReturn(indexToSettings);
    when(response.getIndexToDefaultSettings()).thenReturn(indexToDefaultSettings);
    when(restClient.indices().getSettings(any(GetSettingsRequest.class), any()))
        .thenReturn(response);

    Map<String, Integer> indexMaxResultWindows = client.getIndexMaxResultWindows(indexName);
    assertEquals(1, indexMaxResultWindows.size());
    assertEquals(maxResultWindow, indexMaxResultWindows.values().iterator().next());
  }

  @Test
  void getIndexMaxResultWindowsWithIOException() throws IOException {
    when(restClient.indices().getSettings(any(GetSettingsRequest.class), any()))
        .thenThrow(new IOException());
    assertThrows(IllegalStateException.class, () -> client.getIndexMaxResultWindows("test"));
  }

  @Test
  void search() throws IOException {
    // Mock first scroll request
    SearchResponse searchResponse = mock(SearchResponse.class);
    when(restClient.search(any(), any())).thenReturn(searchResponse);
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
    when(restClient.scroll(any(), any())).thenReturn(scrollResponse);
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
  void searchWithIOException() throws IOException {
    when(restClient.search(any(), any())).thenThrow(new IOException());
    assertThrows(
        IllegalStateException.class,
        () -> client.search(new OpenSearchScrollRequest("test", factory)));
  }

  @Test
  void scrollWithIOException() throws IOException {
    // Mock first scroll request
    SearchResponse searchResponse = mock(SearchResponse.class);
    when(restClient.search(any(), any())).thenReturn(searchResponse);
    when(searchResponse.getScrollId()).thenReturn("scroll123");
    when(searchResponse.getHits())
        .thenReturn(
            new SearchHits(
                new SearchHit[] {new SearchHit(1)},
                new TotalHits(1L, TotalHits.Relation.EQUAL_TO),
                1.0F));

    // Mock second scroll request followed
    when(restClient.scroll(any(), any())).thenThrow(new IOException());

    // First request run successfully
    OpenSearchScrollRequest scrollRequest = new OpenSearchScrollRequest("test", factory);
    client.search(scrollRequest);
    assertThrows(
        IllegalStateException.class, () -> client.search(scrollRequest));
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
  void cleanup() throws IOException {
    OpenSearchScrollRequest request = new OpenSearchScrollRequest("test", factory);
    request.setScrollId("scroll123");
    client.cleanup(request);
    verify(restClient).clearScroll(any(), any());
    assertFalse(request.isScrollStarted());
  }

  @Test
  void cleanupWithoutScrollId() throws IOException {
    OpenSearchScrollRequest request = new OpenSearchScrollRequest("test", factory);
    client.cleanup(request);
    verify(restClient, never()).clearScroll(any(), any());
  }

  @Test
  void cleanupWithIOException() throws IOException {
    when(restClient.clearScroll(any(), any())).thenThrow(new IOException());

    OpenSearchScrollRequest request = new OpenSearchScrollRequest("test", factory);
    request.setScrollId("scroll123");
    assertThrows(IllegalStateException.class, () -> client.cleanup(request));
  }

  @Test
  void getIndices() throws IOException {
    when(restClient.indices().get(any(GetIndexRequest.class), any(RequestOptions.class)))
        .thenReturn(getIndexResponse);
    when(getIndexResponse.getIndices()).thenReturn(new String[] {"index"});

    final List<String> indices = client.indices();
    assertFalse(indices.isEmpty());
  }

  @Test
  void getIndicesWithIOException() throws IOException {
    when(restClient.indices().get(any(GetIndexRequest.class), any(RequestOptions.class)))
        .thenThrow(new IOException());
    assertThrows(IllegalStateException.class, () -> client.indices());
  }

  @Test
  void meta() throws IOException {
    Settings defaultSettings = Settings.builder().build();
    ClusterGetSettingsResponse settingsResponse = mock(ClusterGetSettingsResponse.class);
    when(restClient.cluster().getSettings(any(), any(RequestOptions.class)))
        .thenReturn(settingsResponse);
    when(settingsResponse.getDefaultSettings()).thenReturn(defaultSettings);

    final Map<String, String> meta = client.meta();
    assertEquals("opensearch", meta.get(META_CLUSTER_NAME));
  }

  @Test
  void metaWithIOException() throws IOException {
    when(restClient.cluster().getSettings(any(), any(RequestOptions.class)))
        .thenThrow(new IOException());

    assertThrows(IllegalStateException.class, () -> client.meta());
  }

  @Test
  void mlWithException() {
    assertThrows(UnsupportedOperationException.class, () -> client.getNodeClient());
  }

  private Map<String, MappingMetadata> mockFieldMappings(String indexName, String mappings)
      throws IOException {
    return ImmutableMap.of(indexName, IndexMetadata.fromXContent(createParser(mappings)).mapping());
  }

  private ImmutableOpenMap<String, Settings> mockSettings(String indexName, Settings settings) {
    ImmutableOpenMap.Builder<String, Settings> indexToSettingsBuilder = ImmutableOpenMap.builder();
    indexToSettingsBuilder.put(indexName, settings);
    return indexToSettingsBuilder.build();
  }

  private XContentParser createParser(String mappings) throws IOException {
    return XContentType.JSON
        .xContent()
        .createParser(
            NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, mappings);
  }
}
