/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.metadata;

/**
 * Flint metadata follows Flint index specification and defines metadata
 * for a Flint index regardless of query engine integration and storage.
 */
public class FlintMetadata {

  // TODO: define metadata format and create strong-typed class
  private final String content;

  public FlintMetadata(String content) {
    this.content = content;
  }

  public String getContent() {
    return content;
  }
}
