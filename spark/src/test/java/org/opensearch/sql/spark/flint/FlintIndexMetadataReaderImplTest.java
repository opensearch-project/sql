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

  // TODO FIX this
  @SneakyThrows
  // @Test
  void testGetJobIdFromFlintIndexMetadata() {
    URL url =
        Resources.getResource(
            "flint-index-mappings/flint_my_glue_default_http_logs_size_year_covering_index.json");
    String mappings = Resources.toString(url, Charsets.UTF_8);
    String indexName = "flint_my_glue_default_http_logs_size_year_covering_index";
    mockNodeClientIndicesMappings(indexName, mappings);
    FlintIndexMetadataReader flintIndexMetadataReader = new FlintIndexMetadataReaderImpl(client);
    FlintIndexMetadata jobId =
        flintIndexMetadataReader.getJobIdFromFlintIndexMetadata(
            new IndexDetails(
                "size_year",
                new FullyQualifiedTableName("my_glue.default.http_logs"),
                false,
                true,
                FlintIndexType.COVERING));
    Assertions.assertEquals("00fdlum58g9g1g0q", jobId);
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
