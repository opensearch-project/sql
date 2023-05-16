/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.metadata;

import java.util.Map;

/**
 * Flint metadata follows Flint index specification and defines metadata
 * for a Flint index regardless of query engine integration and storage.
 */
public class FlintMetadata {

  /** Field name and type for each field in a Flint index. */
  private final Map<String, String> schema;

  /** Meta info for the Flint index. */
  private final Map<String, Object> meta;

  public FlintMetadata(Map<String, String> schema,
                       Map<String, Object> meta) {
    this.schema = schema;
    this.meta = meta;
  }

  public Map<String, String> getSchema() {
    return schema;
  }

  public Map<String, Object> getMeta() {
    return meta;
  }
}
