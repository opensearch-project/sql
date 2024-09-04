/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.flint;

import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryRequestContext;
import org.opensearch.sql.spark.dispatcher.model.FlintIndexOptions;
import org.opensearch.sql.spark.dispatcher.model.FullyQualifiedTableName;
import org.opensearch.sql.spark.dispatcher.model.IndexQueryActionType;
import org.opensearch.sql.spark.dispatcher.model.IndexQueryDetails;

@ExtendWith(MockitoExtension.class)
public class FlintIndexMetadataServiceImplTest {
  @Mock(answer = RETURNS_DEEP_STUBS)
  private Client client;

  @Mock private AsyncQueryRequestContext asyncQueryRequestContext;

  @SneakyThrows
  @Test
  void testGetJobIdFromFlintSkippingIndexMetadata() {
    URL url =
        Resources.getResource(
            "flint-index-mappings/flint_mys3_default_http_logs_skipping_index.json");
    String mappings = Resources.toString(url, Charsets.UTF_8);
    String indexName = "flint_mys3_default_http_logs_skipping_index";
    mockNodeClientIndicesMappings(indexName, mappings);
    FlintIndexMetadataService flintIndexMetadataService = new FlintIndexMetadataServiceImpl(client);
    IndexQueryDetails indexQueryDetails =
        IndexQueryDetails.builder()
            .fullyQualifiedTableName(new FullyQualifiedTableName("mys3.default.http_logs"))
            .indexOptions(new FlintIndexOptions())
            .indexQueryActionType(IndexQueryActionType.DROP)
            .indexType(FlintIndexType.SKIPPING)
            .build();

    Map<String, FlintIndexMetadata> indexMetadataMap =
        flintIndexMetadataService.getFlintIndexMetadata(
            indexQueryDetails.openSearchIndexName(), asyncQueryRequestContext);

    Assertions.assertEquals(
        "00fhelvq7peuao0",
        indexMetadataMap.get(indexQueryDetails.openSearchIndexName()).getJobId());
  }

  @SneakyThrows
  @Test
  void testGetJobIdFromFlintSkippingIndexMetadataWithIndexState() {
    URL url =
        Resources.getResource(
            "flint-index-mappings/flint_mys3_default_http_logs_skipping_index.json");
    String mappings = Resources.toString(url, Charsets.UTF_8);
    String indexName = "flint_mys3_default_http_logs_skipping_index";
    mockNodeClientIndicesMappings(indexName, mappings);
    FlintIndexMetadataService flintIndexMetadataService = new FlintIndexMetadataServiceImpl(client);
    IndexQueryDetails indexQueryDetails =
        IndexQueryDetails.builder()
            .fullyQualifiedTableName(new FullyQualifiedTableName("mys3.default.http_logs"))
            .indexOptions(new FlintIndexOptions())
            .indexQueryActionType(IndexQueryActionType.DROP)
            .indexType(FlintIndexType.SKIPPING)
            .build();

    Map<String, FlintIndexMetadata> indexMetadataMap =
        flintIndexMetadataService.getFlintIndexMetadata(
            indexQueryDetails.openSearchIndexName(), asyncQueryRequestContext);

    FlintIndexMetadata metadata = indexMetadataMap.get(indexQueryDetails.openSearchIndexName());
    Assertions.assertEquals("00fhelvq7peuao0", metadata.getJobId());
  }

  @SneakyThrows
  @Test
  void testGetJobIdFromFlintCoveringIndexMetadata() {
    URL url =
        Resources.getResource("flint-index-mappings/flint_mys3_default_http_logs_cv1_index.json");
    String mappings = Resources.toString(url, Charsets.UTF_8);
    String indexName = "flint_mys3_default_http_logs_cv1_index";
    mockNodeClientIndicesMappings(indexName, mappings);
    IndexQueryDetails indexQueryDetails =
        IndexQueryDetails.builder()
            .indexName("cv1")
            .fullyQualifiedTableName(new FullyQualifiedTableName("mys3.default.http_logs"))
            .indexOptions(new FlintIndexOptions())
            .indexQueryActionType(IndexQueryActionType.DROP)
            .indexType(FlintIndexType.COVERING)
            .build();
    FlintIndexMetadataService flintIndexMetadataService = new FlintIndexMetadataServiceImpl(client);

    Map<String, FlintIndexMetadata> indexMetadataMap =
        flintIndexMetadataService.getFlintIndexMetadata(
            indexQueryDetails.openSearchIndexName(), asyncQueryRequestContext);

    Assertions.assertEquals(
        "00fdmvv9hp8u0o0q",
        indexMetadataMap.get(indexQueryDetails.openSearchIndexName()).getJobId());
  }

  @SneakyThrows
  @Test
  void testGetJobIDWithNPEException() {
    URL url = Resources.getResource("flint-index-mappings/npe_mapping.json");
    String mappings = Resources.toString(url, Charsets.UTF_8);
    String indexName = "flint_mys3_default_http_logs_cv1_index";
    mockNodeClientIndicesMappings(indexName, mappings);
    FlintIndexMetadataService flintIndexMetadataService = new FlintIndexMetadataServiceImpl(client);
    IndexQueryDetails indexQueryDetails =
        IndexQueryDetails.builder()
            .indexName("cv1")
            .fullyQualifiedTableName(new FullyQualifiedTableName("mys3.default.http_logs"))
            .indexOptions(new FlintIndexOptions())
            .indexQueryActionType(IndexQueryActionType.DROP)
            .indexType(FlintIndexType.COVERING)
            .build();

    Map<String, FlintIndexMetadata> flintIndexMetadataMap =
        flintIndexMetadataService.getFlintIndexMetadata(
            indexQueryDetails.openSearchIndexName(), asyncQueryRequestContext);

    Assertions.assertFalse(
        flintIndexMetadataMap.containsKey("flint_mys3_default_http_logs_cv1_index"));
  }

  @SneakyThrows
  @Test
  void testGetJobIDWithNPEExceptionForMultipleIndices() {
    HashMap<String, String> indexMappingsMap = new HashMap<>();
    URL url = Resources.getResource("flint-index-mappings/npe_mapping.json");
    String mappings = Resources.toString(url, Charsets.UTF_8);
    String indexName = "flint_mys3_default_http_logs_cv1_index";
    indexMappingsMap.put(indexName, mappings);
    url =
        Resources.getResource(
            "flint-index-mappings/flint_mys3_default_http_logs_skipping_index.json");
    mappings = Resources.toString(url, Charsets.UTF_8);
    indexName = "flint_mys3_default_http_logs_skipping_index";
    indexMappingsMap.put(indexName, mappings);
    mockNodeClientIndicesMappings("flint_mys3*", indexMappingsMap);
    FlintIndexMetadataService flintIndexMetadataService = new FlintIndexMetadataServiceImpl(client);

    Map<String, FlintIndexMetadata> flintIndexMetadataMap =
        flintIndexMetadataService.getFlintIndexMetadata("flint_mys3*", asyncQueryRequestContext);

    Assertions.assertFalse(
        flintIndexMetadataMap.containsKey("flint_mys3_default_http_logs_cv1_index"));
    Assertions.assertTrue(
        flintIndexMetadataMap.containsKey("flint_mys3_default_http_logs_skipping_index"));
  }

  @SneakyThrows
  public void mockNodeClientIndicesMappings(String indexName, String mappings) {
    GetMappingsResponse mockResponse = mock(GetMappingsResponse.class);
    when(client.admin().indices().prepareGetMappings().setIndices(indexName).get())
        .thenReturn(mockResponse);
    Map<String, MappingMetadata> metadata;
    metadata = Map.of(indexName, IndexMetadata.fromXContent(createParser(mappings)).mapping());
    when(mockResponse.getMappings()).thenReturn(metadata);
  }

  @SneakyThrows
  public void mockNodeClientIndicesMappings(
      String indexPattern, HashMap<String, String> indexMappingsMap) {
    GetMappingsResponse mockResponse = mock(GetMappingsResponse.class);
    when(client.admin().indices().prepareGetMappings().setIndices(indexPattern).get())
        .thenReturn(mockResponse);
    Map<String, MappingMetadata> metadataMap = new HashMap<>();
    for (String indexName : indexMappingsMap.keySet()) {
      metadataMap.put(
          indexName,
          IndexMetadata.fromXContent(createParser(indexMappingsMap.get(indexName))).mapping());
    }
    when(mockResponse.getMappings()).thenReturn(metadataMap);
  }

  private XContentParser createParser(String mappings) throws IOException {
    return XContentType.JSON
        .xContent()
        .createParser(
            NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, mappings);
  }
}
