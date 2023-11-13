package org.opensearch.sql.spark.flint;

import java.util.Map;
import lombok.AllArgsConstructor;
import org.opensearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.sql.spark.dispatcher.model.IndexDetails;

/** Implementation of {@link FlintIndexMetadataReader} */
@AllArgsConstructor
public class FlintIndexMetadataReaderImpl implements FlintIndexMetadataReader {

  private final Client client;

  @Override
  public FlintIndexMetadata getFlintIndexMetadata(IndexDetails indexDetails) {
    String indexName = indexDetails.openSearchIndexName();
    GetMappingsResponse mappingsResponse =
        client.admin().indices().prepareGetMappings(indexName).get();
    try {
      MappingMetadata mappingMetadata = mappingsResponse.mappings().get(indexName);
      Map<String, Object> mappingSourceMap = mappingMetadata.getSourceAsMap();
      return FlintIndexMetadata.fromMetatdata((Map<String, Object>) mappingSourceMap.get("_meta"));
    } catch (NullPointerException npe) {
      throw new IllegalArgumentException("Provided Index doesn't exist");
    }
  }
}
