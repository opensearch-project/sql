/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.mapping;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.EnumUtils;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;

/**
 * OpenSearch index mapping. Because there is no specific behavior for different field types,
 * string is used to represent field types.
 */
@ToString
public class IndexMapping {

  /** Field mappings from field name to field type in OpenSearch date type system. */
  @Getter
  private final Map<String, OpenSearchDataType> fieldMappings;

  @SuppressWarnings("unchecked")
  public IndexMapping(MappingMetadata metaData) {
    this.fieldMappings = parseMapping((Map<String, Object>) metaData.getSourceAsMap()
        .getOrDefault("properties", null));
  }

  /**
   * How many fields in the index (after flatten).
   *
   * @return field size
   */
  public int size() {
    return fieldMappings.size();
  }

  @SuppressWarnings("unchecked")
  private Map<String, OpenSearchDataType> parseMapping(Map<String, Object> indexMapping) {
    Map<String, OpenSearchDataType> result = new LinkedHashMap<>();
    if (indexMapping != null) {
      indexMapping.forEach((k, v) -> {
        var innerMap = (Map<String, Object>)v;
        // TODO: confirm that only `object` mappings can omit `type` field.
        var type = ((String) innerMap.getOrDefault("type", "object")).replace("_", "");
        if (!EnumUtils.isValidEnumIgnoreCase(OpenSearchDataType.MappingType.class, type)) {
          // unknown type, e.g. `alias`
          // TODO resolve alias reference
          return;
        }
        // TODO read formats for date type
        result.put(k, OpenSearchDataType.of(
            EnumUtils.getEnumIgnoreCase(OpenSearchDataType.MappingType.class, type),
            parseMapping((Map<String, Object>) innerMap.getOrDefault("properties", null)),
            parseMapping((Map<String, Object>) innerMap.getOrDefault("fields", null))
            ));
      });
    }
    return result;
  }
}
