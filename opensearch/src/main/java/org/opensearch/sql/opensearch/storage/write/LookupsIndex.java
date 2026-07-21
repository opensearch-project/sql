/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.write;

import java.util.List;
import java.util.Map;
import org.opensearch.ResourceAlreadyExistsException;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.common.settings.Settings;
import org.opensearch.transport.client.node.NodeClient;

/**
 * Creates a lookup backing index: an ordinary non-hidden index, per-lookup by default so it owns
 * its mapping and write boundary. Its dynamic template types every string as {@code text} plus
 * {@code keyword} with detection off, so lookups sharing an index type deterministically.
 */
public final class LookupsIndex {

  private LookupsIndex() {}

  /** Per-row lookup slice discriminant. */
  public static final String LOOKUP_FIELD = "__lookup";

  /** Suffix of the dedicated per-lookup backing index derived from the lookup name. */
  public static final String BACKING_SUFFIX = "__lookup";

  private static Map<String, Object> mappings() {
    Map<String, Object> stringAsTextAndKeyword =
        Map.of(
            "match_mapping_type",
            "string",
            "mapping",
            Map.of(
                "type",
                "text",
                "fields",
                Map.of("keyword", Map.of("type", "keyword", "ignore_above", 256))));
    return Map.of(
        "dynamic_templates",
        List.of(Map.of("strings_as_text_and_keyword", stringAsTextAndKeyword)),
        "date_detection",
        false,
        "numeric_detection",
        false,
        "properties",
        Map.of(LOOKUP_FIELD, Map.of("type", "keyword")));
  }

  private static Settings settings() {
    return Settings.builder().put("index.mapping.total_fields.limit", 2000).build();
  }

  public static CreateIndexRequest createRequest(String index) {
    return new CreateIndexRequest(index).mapping(mappings()).settings(settings());
  }

  /** Create {@code index} with its template if absent; a concurrent create is a no-op. */
  public static void ensureExists(NodeClient client, String index) {
    try {
      client.admin().indices().create(createRequest(index)).actionGet();
    } catch (ResourceAlreadyExistsException alreadyExists) {
      // Created concurrently, or already present from a prior write.
    }
  }
}
