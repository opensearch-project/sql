package org.opensearch.sql.spark.flint;

import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import java.io.IOException;
import java.net.URL;
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
import org.opensearch.sql.spark.dispatcher.model.FullyQualifiedTableName;
import org.opensearch.sql.spark.dispatcher.model.IndexDetails;

@ExtendWith(MockitoExtension.class)
public class FlintIndexMetadataReaderImplTest {
  @Mock(answer = RETURNS_DEEP_STUBS)
  private Client client;

  @SneakyThrows
  @Test
  void testGetJobIdFromFlintSkippingIndexMetadata() {
    URL url =
        Resources.getResource(
            "flint-index-mappings/flint_mys3_default_http_logs_skipping_index.json");
    String mappings = Resources.toString(url, Charsets.UTF_8);
    String indexName = "flint_mys3_default_http_logs_skipping_index";
    mockNodeClientIndicesMappings(indexName, mappings);
    FlintIndexMetadataReader flintIndexMetadataReader = new FlintIndexMetadataReaderImpl(client);
    FlintIndexMetadata indexMetadata =
        flintIndexMetadataReader.getFlintIndexMetadata(
            new IndexDetails(
                null,
                new FullyQualifiedTableName("mys3.default.http_logs"),
                false,
                true,
                FlintIndexType.SKIPPING));
    Assertions.assertEquals("00fdmvv9hp8u0o0q", indexMetadata.getJobId());
  }

  @SneakyThrows
  @Test
  void testGetJobIdFromFlintCoveringIndexMetadata() {
    URL url =
        Resources.getResource("flint-index-mappings/flint_mys3_default_http_logs_cv1_index.json");
    String mappings = Resources.toString(url, Charsets.UTF_8);
    String indexName = "flint_mys3_default_http_logs_cv1_index";
    mockNodeClientIndicesMappings(indexName, mappings);
    FlintIndexMetadataReader flintIndexMetadataReader = new FlintIndexMetadataReaderImpl(client);
    FlintIndexMetadata indexMetadata =
        flintIndexMetadataReader.getFlintIndexMetadata(
            new IndexDetails(
                "cv1",
                new FullyQualifiedTableName("mys3.default.http_logs"),
                false,
                true,
                FlintIndexType.COVERING));
    Assertions.assertEquals("00fdmvv9hp8u0o0q", indexMetadata.getJobId());
  }

  @SneakyThrows
  @Test
  void testGetJobIDWithNPEException() {
    URL url = Resources.getResource("flint-index-mappings/npe_mapping.json");
    String mappings = Resources.toString(url, Charsets.UTF_8);
    String indexName = "flint_mys3_default_http_logs_cv1_index";
    mockNodeClientIndicesMappings(indexName, mappings);
    FlintIndexMetadataReader flintIndexMetadataReader = new FlintIndexMetadataReaderImpl(client);
    IllegalArgumentException illegalArgumentException =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                flintIndexMetadataReader.getFlintIndexMetadata(
                    new IndexDetails(
                        "cv1",
                        new FullyQualifiedTableName("mys3.default.http_logs"),
                        false,
                        true,
                        FlintIndexType.COVERING)));
    Assertions.assertEquals("Provided Index doesn't exist", illegalArgumentException.getMessage());
  }

  @SneakyThrows
  @Test
  void testGetJobIdFromUnsupportedIndex() {
    FlintIndexMetadataReader flintIndexMetadataReader = new FlintIndexMetadataReaderImpl(client);
    UnsupportedOperationException unsupportedOperationException =
        Assertions.assertThrows(
            UnsupportedOperationException.class,
            () ->
                flintIndexMetadataReader.getFlintIndexMetadata(
                    new IndexDetails(
                        "cv1",
                        new FullyQualifiedTableName("mys3.default.http_logs"),
                        false,
                        true,
                        FlintIndexType.MATERIALIZED_VIEW)));
    Assertions.assertEquals(
        "Unsupported Index Type : MATERIALIZED_VIEW", unsupportedOperationException.getMessage());
  }

  @SneakyThrows
  public void mockNodeClientIndicesMappings(String indexName, String mappings) {
    GetMappingsResponse mockResponse = mock(GetMappingsResponse.class);
    when(client.admin().indices().prepareGetMappings(any()).get()).thenReturn(mockResponse);
    Map<String, MappingMetadata> metadata;
    metadata = Map.of(indexName, IndexMetadata.fromXContent(createParser(mappings)).mapping());
    when(mockResponse.mappings()).thenReturn(metadata);
  }

  private XContentParser createParser(String mappings) throws IOException {
    return XContentType.JSON
        .xContent()
        .createParser(
            NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, mappings);
  }
}
