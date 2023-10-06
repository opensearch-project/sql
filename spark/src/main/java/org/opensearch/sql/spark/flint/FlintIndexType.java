/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.flint;

/** Enum for FlintIndex Type. */
public enum FlintIndexType {
  SKIPPING("skipping_index"),
  COVERING("index"),
  MATERIALIZED_VIEW("materialized_view");

  private final String suffix;

  FlintIndexType(String suffix) {
    this.suffix = suffix;
  }

  public String getSuffix() {
    return this.suffix;
  }
}
