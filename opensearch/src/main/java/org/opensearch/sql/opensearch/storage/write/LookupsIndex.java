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
 * The single shared index that co-locates every lookup as rows tagged {@code __lookup=<uuid>}
 * behind a filtered alias, the same artifact the Dashboards data importer produces.
 *
 * <p>It is a normal hidden index, not a registered system index, so a lookup stays readable by a
 * user through its alias (a system index would block that read for non-system callers). Its uniform
 * dynamic template maps every string field to {@code text} plus {@code keyword} and disables
 * content-based detection, so lookups sharing the index never collide on field type and typing is
 * deterministic.
 */
public final class LookupsIndex {

  private LookupsIndex() {}

  /** Fixed, single, not-configurable shared lookup store. */
  public static final String INDEX_NAME = ".lookups";

  /** Per-row discriminant; matches the Dashboards importer constant. */
  public static final String LOOKUP_FIELD = "__lookup";

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
    return Settings.builder()
        .put("index.hidden", true)
        .put("index.mapping.total_fields.limit", 2000)
        .build();
  }

  public static CreateIndexRequest createRequest() {
    return new CreateIndexRequest(INDEX_NAME).mapping(mappings()).settings(settings());
  }

  /** Create {@code .lookups} with its template if absent; a concurrent create is a no-op. */
  public static void ensureExists(NodeClient client) {
    try {
      client.admin().indices().create(createRequest()).actionGet();
    } catch (ResourceAlreadyExistsException alreadyExists) {
      // Created concurrently.
    }
  }
}
