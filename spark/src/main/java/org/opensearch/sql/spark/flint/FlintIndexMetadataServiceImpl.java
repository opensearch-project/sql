/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.flint;

import static org.opensearch.sql.spark.dispatcher.model.FlintIndexOptions.AUTO_REFRESH;
import static org.opensearch.sql.spark.dispatcher.model.FlintIndexOptions.CHECKPOINT_LOCATION;
import static org.opensearch.sql.spark.dispatcher.model.FlintIndexOptions.INCREMENTAL_REFRESH;
import static org.opensearch.sql.spark.dispatcher.model.FlintIndexOptions.WATERMARK_DELAY;
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.AllArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.client.Client;
import org.opensearch.sql.spark.dispatcher.model.FlintIndexOptions;

/** Implementation of {@link FlintIndexMetadataService} */
@AllArgsConstructor
public class FlintIndexMetadataServiceImpl implements FlintIndexMetadataService {

  private static final Logger LOGGER = LogManager.getLogger(FlintIndexMetadataServiceImpl.class);

  private final Client client;
  public static final Set<String> ALTER_TO_FULL_REFRESH_ALLOWED_OPTIONS =
      new LinkedHashSet<>(Arrays.asList(AUTO_REFRESH, INCREMENTAL_REFRESH));
  public static final Set<String> ALTER_TO_INCREMENTAL_REFRESH_ALLOWED_OPTIONS =
      new LinkedHashSet<>(
          Arrays.asList(AUTO_REFRESH, INCREMENTAL_REFRESH, WATERMARK_DELAY, CHECKPOINT_LOCATION));

  @Override
  public Map<String, FlintIndexMetadata> getFlintIndexMetadata(String indexPattern) {
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
  public void updateIndexToManualRefresh(String indexName, FlintIndexOptions flintIndexOptions) {
    GetMappingsResponse mappingsResponse =
        client.admin().indices().prepareGetMappings().setIndices(indexName).get();
    Map<String, Object> flintMetadataMap =
        mappingsResponse.getMappings().get(indexName).getSourceAsMap();
    Map<String, Object> meta = (Map<String, Object>) flintMetadataMap.get("_meta");
    String kind = (String) meta.get("kind");
    Map<String, Object> options = (Map<String, Object>) meta.get("options");
    Map<String, String> newOptions = flintIndexOptions.getProvidedOptions();
    validateFlintIndexOptions(kind, options, newOptions);
    options.putAll(newOptions);
    client.admin().indices().preparePutMapping(indexName).setSource(flintMetadataMap).get();
  }

  @Override
  public void deleteFlintIndex(String indexName) {
    LOGGER.info("Vacuuming Flint index {}", indexName);
    DeleteIndexRequest request = new DeleteIndexRequest().indices(indexName);
    AcknowledgedResponse response = client.admin().indices().delete(request).actionGet();
    LOGGER.info("OpenSearch index delete result: {}", response.isAcknowledged());
  }

  private void validateFlintIndexOptions(
      String kind, Map<String, Object> existingOptions, Map<String, String> newOptions) {
    if ((newOptions.containsKey(INCREMENTAL_REFRESH)
            && Boolean.parseBoolean(newOptions.get(INCREMENTAL_REFRESH)))
        || ((!newOptions.containsKey(INCREMENTAL_REFRESH)
            && Boolean.parseBoolean((String) existingOptions.get(INCREMENTAL_REFRESH))))) {
      validateConversionToIncrementalRefresh(kind, existingOptions, newOptions);
    } else {
      validateConversionToFullRefresh(newOptions);
    }
  }

  private void validateConversionToFullRefresh(Map<String, String> newOptions) {
    if (!ALTER_TO_FULL_REFRESH_ALLOWED_OPTIONS.containsAll(newOptions.keySet())) {
      throw new IllegalArgumentException(
          String.format(
              "Altering to full refresh only allows: %s options",
              ALTER_TO_FULL_REFRESH_ALLOWED_OPTIONS));
    }
  }

  private void validateConversionToIncrementalRefresh(
      String kind, Map<String, Object> existingOptions, Map<String, String> newOptions) {
    if (!ALTER_TO_INCREMENTAL_REFRESH_ALLOWED_OPTIONS.containsAll(newOptions.keySet())) {
      throw new IllegalArgumentException(
          String.format(
              "Altering to incremental refresh only allows: %s options",
              ALTER_TO_INCREMENTAL_REFRESH_ALLOWED_OPTIONS));
    }
    HashMap<String, Object> mergedOptions = new HashMap<>();
    mergedOptions.putAll(existingOptions);
    mergedOptions.putAll(newOptions);
    List<String> missingAttributes = new ArrayList<>();
    if (!mergedOptions.containsKey(CHECKPOINT_LOCATION)
        || StringUtils.isEmpty((String) mergedOptions.get(CHECKPOINT_LOCATION))) {
      missingAttributes.add(CHECKPOINT_LOCATION);
    }
    if (kind.equals("mv")
        && (!mergedOptions.containsKey(WATERMARK_DELAY)
            || StringUtils.isEmpty((String) mergedOptions.get(WATERMARK_DELAY)))) {
      missingAttributes.add(WATERMARK_DELAY);
    }
    if (missingAttributes.size() > 0) {
      String errorMessage =
          "Conversion to incremental refresh index cannot proceed due to missing attributes: "
              + String.join(", ", missingAttributes)
              + ".";
      LOGGER.error(errorMessage);
      throw new IllegalArgumentException(errorMessage);
    }
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
