/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.rewriter.matchtoterm;

import java.util.*;
import org.json.JSONObject;
import org.opensearch.sql.legacy.esdomain.mapping.FieldMappings;
import org.opensearch.sql.legacy.esdomain.mapping.IndexMappings;

/** Index Mapping information in current query being visited. */
public class TermFieldScope {

  // mapper => index, type, field_name, FieldMappingMetaData
  private IndexMappings mapper;
  private FieldMappings finalMapping;
  private Map<String, String> aliases;

  public TermFieldScope() {
    this.mapper = IndexMappings.EMPTY;
    this.aliases = new HashMap<>();
  }

  public Map<String, String> getAliases() {
    return aliases;
  }

  public void setAliases(Map<String, String> aliases) {
    this.aliases = aliases;
  }

  public IndexMappings getMapper() {
    return this.mapper;
  }

  public void setMapper(IndexMappings mapper) {
    this.mapper = mapper;
  }

  public Optional<Map<String, Object>> resolveFieldMapping(String fieldName) {
    Set<FieldMappings> indexMappings = new HashSet<>(mapper.allMappings());
    Optional<Map<String, Object>> resolvedMapping =
        indexMappings.stream()
            .filter(mapping -> mapping.has(fieldName))
            .map(mapping -> mapping.mapping(fieldName))
            .reduce(
                (map1, map2) -> {
                  if (!map1.equals(map2)) {
                    // TODO: Merge mappings if they are compatible, for text and text/keyword to
                    // text/keyword.
                    String exceptionReason =
                        String.format(
                            Locale.ROOT,
                            "Different mappings are not allowed "
                                + "for the same field[%s]: found [%s] and [%s] ",
                            fieldName,
                            pretty(map1),
                            pretty(map2));
                    throw new VerificationException(exceptionReason);
                  }
                  return map1;
                });
    return resolvedMapping;
  }

  private static String pretty(Map<String, Object> mapping) {
    return new JSONObject(mapping).toString().replaceAll("\"", "");
  }
}
