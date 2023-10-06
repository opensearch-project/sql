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

  protected static final String META_KEY = "_meta";
  protected static final String PROPERTIES_KEY = "properties";
  protected static final String ENV_KEY = "env";
  protected static final String JOB_ID_KEY = "SERVERLESS_EMR_JOB_ID";

  private final Client client;

  @Override
  public String getJobIdFromFlintIndexMetadata(IndexDetails indexDetails) {
    String indexName = getIndexName(indexDetails);
    GetMappingsResponse mappingsResponse =
        client.admin().indices().prepareGetMappings(indexName).get();
    try {
      MappingMetadata mappingMetadata = mappingsResponse.mappings().get(indexName);
      Map<String, Object> mappingSourceMap = mappingMetadata.getSourceAsMap();
      Map<String, Object> metaMap = (Map<String, Object>) mappingSourceMap.get(META_KEY);
      Map<String, Object> propertiesMap = (Map<String, Object>) metaMap.get(PROPERTIES_KEY);
      Map<String, Object> envMap = (Map<String, Object>) propertiesMap.get(ENV_KEY);
      return (String) envMap.get(JOB_ID_KEY);
    } catch (NullPointerException npe) {
      throw new IllegalArgumentException("Provided Index doesn't exist");
    }
  }

  private String getIndexName(IndexDetails indexDetails) {
    FullyQualifiedTableName fullyQualifiedTableName = indexDetails.getFullyQualifiedTableName();
    if (FlintIndexType.SKIPPING.equals(indexDetails.getIndexType())) {
      String indexName =
          "flint"
              + "_"
              + fullyQualifiedTableName.getDatasourceName()
              + "_"
              + fullyQualifiedTableName.getSchemaName()
              + "_"
              + fullyQualifiedTableName.getTableName()
              + "_"
              + indexDetails.getIndexType().getSuffix();
      return indexName.toLowerCase();
    } else if (FlintIndexType.COVERING.equals(indexDetails.getIndexType())) {
      String indexName =
          "flint"
              + "_"
              + fullyQualifiedTableName.getDatasourceName()
              + "_"
              + fullyQualifiedTableName.getSchemaName()
              + "_"
              + fullyQualifiedTableName.getTableName()
              + "_"
              + indexDetails.getIndexName()
              + "_"
              + indexDetails.getIndexType().getSuffix();
      return indexName.toLowerCase();
    } else {
      throw new UnsupportedOperationException(
          String.format("Unsupported Index Type : %s", indexDetails.getIndexType()));
    }
  }
}
