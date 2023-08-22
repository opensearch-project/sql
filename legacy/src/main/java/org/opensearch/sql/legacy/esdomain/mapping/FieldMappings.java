/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.esdomain.mapping;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiConsumer;
import org.json.JSONObject;
import org.opensearch.cluster.metadata.MappingMetadata;

/**
 *
 *
 * <pre>
 * Field mappings in a specific type.
 * <p>
 * Sample:
 * fieldMappings: {
 * 'properties': {
 * 'balance': {
 * 'type': long
 * },
 * 'age': {
 * 'type': integer
 * },
 * 'state': {
 * 'type': text
 * }
 * 'name': {
 * 'type': text
 * 'fields': {
 * 'keyword': {
 * 'type': keyword,
 * 'ignore_above': 256
 * }}}}}
 * </pre>
 */
@SuppressWarnings("unchecked")
public class FieldMappings implements Mappings<Map<String, Object>> {

  private static final String PROPERTIES = "properties";

  /** Mapping from field name to its type */
  private final Map<String, Object> fieldMappings;

  public FieldMappings(MappingMetadata mappings) {
    fieldMappings = mappings.sourceAsMap();
  }

  public FieldMappings(Map<String, Map<String, Object>> mapping) {
    Map<String, Object> finalMapping = new HashMap<>();
    finalMapping.put(PROPERTIES, mapping);
    fieldMappings = finalMapping;
  }

  @Override
  public boolean has(String path) {
    return mapping(path) != null;
  }

  /** Different from default implementation that search mapping for path is required */
  @Override
  public Map<String, Object> mapping(String path) {
    Map<String, Object> mapping = fieldMappings;
    for (String name : path.split("\\.")) {
      if (mapping == null || !mapping.containsKey(PROPERTIES)) {
        return null;
      }

      mapping = (Map<String, Object>) ((Map<String, Object>) mapping.get(PROPERTIES)).get(name);
    }
    return mapping;
  }

  @Override
  public Map<String, Map<String, Object>> data() {
    // Is this assumption true? Is it possible mapping of field is NOT a Map<String,Object>?
    return (Map<String, Map<String, Object>>) fieldMappings.get(PROPERTIES);
  }

  public void flat(BiConsumer<String, String> func) {
    flatMappings(data(), Optional.empty(), func);
  }

  @SuppressWarnings("unchecked")
  private void flatMappings(
      Map<String, Map<String, Object>> mappings,
      Optional<String> path,
      BiConsumer<String, String> func) {
    mappings.forEach(
        (fieldName, mapping) -> {
          String fullFieldName = path.map(s -> s + "." + fieldName).orElse(fieldName);
          String type = (String) mapping.getOrDefault("type", "object");
          func.accept(fullFieldName, type);

          if (mapping.containsKey("fields")) {
            ((Map<String, Map<String, Object>>) mapping.get("fields"))
                .forEach(
                    (innerFieldName, innerMapping) ->
                        func.accept(
                            fullFieldName + "." + innerFieldName,
                            (String) innerMapping.getOrDefault("type", "object")));
          }

          if (mapping.containsKey("properties")) {
            flatMappings(
                (Map<String, Map<String, Object>>) mapping.get("properties"),
                Optional.of(fullFieldName),
                func);
          }
        });
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    FieldMappings that = (FieldMappings) o;
    return Objects.equals(fieldMappings, that.fieldMappings);
  }

  @Override
  public int hashCode() {
    return Objects.hash(fieldMappings);
  }

  @Override
  public String toString() {
    return "FieldMappings" + new JSONObject(fieldMappings).toString(2);
  }
}
