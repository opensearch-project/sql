package org.opensearch.sql.spark.flint;

import java.util.Map;
import lombok.AllArgsConstructor;
import org.opensearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.sql.spark.dispatcher.model.FullyQualifiedTableName;
import org.opensearch.sql.spark.dispatcher.model.IndexDetails;

/** Implementation of {@link FlintIndexMetadataReader} */
@AllArgsConstructor
public class FlintIndexMetadataReaderImpl implements FlintIndexMetadataReader {

  private final Client client;

  @Override
  public String getJobIdFromFlintIndexMetadata(IndexDetails indexDetails) {
    String indexName = getIndexName(indexDetails).toLowerCase();
    GetMappingsResponse mappingsResponse =
        client.admin().indices().prepareGetMappings(indexName).get();
    try {
      MappingMetadata mappingMetadata = mappingsResponse.mappings().get(indexName);
      Map<String, Object> mappingSourceMap = mappingMetadata.getSourceAsMap();
      Map<String, Object> metaMap = (Map<String, Object>) mappingSourceMap.get("_meta");
      Map<String, Object> propertiesMap = (Map<String, Object>) metaMap.get("properties");
      Map<String, Object> envMap = (Map<String, Object>) propertiesMap.get("env");
      return (String) envMap.get("SERVERLESS_EMR_JOB_ID");
    } catch (NullPointerException npe) {
      throw new IllegalArgumentException("Index doesn't exist");
    }
  }

  private String getIndexName(IndexDetails indexDetails) {
    FullyQualifiedTableName fullyQualifiedTableName = indexDetails.getFullyQualifiedTableName();
    if (FlintIndexType.SKIPPING.equals(indexDetails.getIndexType())) {
      return "flint"
          + "_"
          + fullyQualifiedTableName.getDatasourceName()
          + "_"
          + fullyQualifiedTableName.getSchemaName()
          + "_"
          + fullyQualifiedTableName.getTableName()
          + "_"
          + indexDetails.getIndexType().getName();
    } else if (FlintIndexType.COVERING.equals(indexDetails.getIndexType())) {
      return "flint"
          + "_"
          + fullyQualifiedTableName.getDatasourceName()
          + "_"
          + fullyQualifiedTableName.getSchemaName()
          + "_"
          + fullyQualifiedTableName.getTableName()
          + "_"
          + indexDetails.getIndexName()
          + "_"
          + indexDetails.getIndexType().getName();
    } else {
      throw new UnsupportedOperationException(
          String.format("Unsupported Index Type : %s", indexDetails.getIndexType()));
    }
  }
}
