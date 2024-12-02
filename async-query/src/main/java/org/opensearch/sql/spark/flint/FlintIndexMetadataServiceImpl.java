/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.flint;

import static org.opensearch.sql.spark.flint.FlintIndexMetadata.APP_ID;
import static org.opensearch.sql.spark.flint.FlintIndexMetadata.ENV_KEY;
import static org.opensearch.sql.spark.flint.FlintIndexMetadata.KIND_KEY;
import static org.opensearch.sql.spark.flint.FlintIndexMetadata.LATEST_ID_KEY;
import static org.opensearch.sql.spark.flint.FlintIndexMetadata.META_KEY;
import static org.opensearch.sql.spark.flint.FlintIndexMetadata.NAME_KEY;
import static org.opensearch.sql.spark.flint.FlintIndexMetadata.OPTIONS_KEY;
import static org.opensearch.sql.spark.flint.FlintIndexMetadata.PROPERTIES_KEY;
import static org.opensearch.sql.spark.flint.FlintIndexMetadata.SERVERLESS_EMR_JOB_ID;
import static org.opensearch.sql.spark.flint.FlintIndexMetadata.SOURCE_KEY;

import java.util.HashMap;
import java.util.Map;
import lombok.AllArgsConstructor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.opensearch.client.Client;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryRequestContext;
import org.opensearch.sql.spark.dispatcher.model.FlintIndexOptions;

/** Implementation of {@link FlintIndexMetadataService} */
@AllArgsConstructor
public class FlintIndexMetadataServiceImpl implements FlintIndexMetadataService {

  private static final Logger LOGGER = LogManager.getLogger(FlintIndexMetadataServiceImpl.class);

  private final Client client;

  @Override
  public Map<String, FlintIndexMetadata> getFlintIndexMetadata(
      String indexPattern, AsyncQueryRequestContext asyncQueryRequestContext) {
    GetMappingsResponse mappingsResponse =
        client.admin().indices().prepareGetMappings().setIndices(indexPattern).get();
    Map<String, FlintIndexMetadata> indexMetadataMap = new HashMap<>();
    mappingsResponse
        .getMappings()
        .forEach(
            (indexName, mappingMetadata) -> {
              try {
                Map<String, Object> mappingSourceMap = mappingMetadata.getSourceAsMap();
                FlintIndexMetadata metadata =
                    fromMetadata(indexName, (Map<String, Object>) mappingSourceMap.get(META_KEY));
                indexMetadataMap.put(indexName, metadata);
              } catch (Exception exception) {
                LOGGER.error(
                    "Exception while building index details for index: {} due to: {}",
                    indexName,
                    exception.getMessage());
              }
            });
    return indexMetadataMap;
  }

  @Override
  public void updateIndexToManualRefresh(
      String indexName,
      FlintIndexOptions flintIndexOptions,
      AsyncQueryRequestContext asyncQueryRequestContext) {
    GetMappingsResponse mappingsResponse =
        client.admin().indices().prepareGetMappings().setIndices(indexName).get();
    Map<String, Object> flintMetadataMap =
        mappingsResponse.getMappings().get(indexName).getSourceAsMap();
    Map<String, Object> meta = (Map<String, Object>) flintMetadataMap.get("_meta");
    String kind = (String) meta.get("kind");
    Map<String, Object> options = (Map<String, Object>) meta.get("options");
    Map<String, String> newOptions = flintIndexOptions.getProvidedOptions();
    FlintIndexMetadataValidator.validateFlintIndexOptions(kind, options, newOptions);
    options.putAll(newOptions);
    client.admin().indices().preparePutMapping(indexName).setSource(flintMetadataMap).get();
  }

  private FlintIndexMetadata fromMetadata(String indexName, Map<String, Object> metaMap) {
    FlintIndexMetadata.FlintIndexMetadataBuilder flintIndexMetadataBuilder =
        FlintIndexMetadata.builder();
    Map<String, Object> propertiesMap = (Map<String, Object>) metaMap.get(PROPERTIES_KEY);
    Map<String, Object> envMap = (Map<String, Object>) propertiesMap.get(ENV_KEY);
    Map<String, Object> options = (Map<String, Object>) metaMap.get(OPTIONS_KEY);
    FlintIndexOptions flintIndexOptions = new FlintIndexOptions();
    for (String key : options.keySet()) {
      flintIndexOptions.setOption(key, (String) options.get(key));
    }
    String jobId = (String) envMap.get(SERVERLESS_EMR_JOB_ID);
    String appId = (String) envMap.getOrDefault(APP_ID, null);
    String latestId = (String) metaMap.getOrDefault(LATEST_ID_KEY, null);
    String kind = (String) metaMap.getOrDefault(KIND_KEY, null);
    String name = (String) metaMap.getOrDefault(NAME_KEY, null);
    String source = (String) metaMap.getOrDefault(SOURCE_KEY, null);
    flintIndexMetadataBuilder.jobId(jobId);
    flintIndexMetadataBuilder.appId(appId);
    flintIndexMetadataBuilder.latestId(latestId);
    flintIndexMetadataBuilder.name(name);
    flintIndexMetadataBuilder.kind(kind);
    flintIndexMetadataBuilder.source(source);
    flintIndexMetadataBuilder.opensearchIndexName(indexName);
    flintIndexMetadataBuilder.flintIndexOptions(flintIndexOptions);
    return flintIndexMetadataBuilder.build();
  }
}
