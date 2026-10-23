/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.system;

import java.util.Map;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.opensearch.request.system.OpenSearchSystemRequest;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlan;

/**
 * Strategy supplying the schema and row source for an {@link OpenSearchCatalogTable} backed by a
 * read-only OpenSearch admin API. Each endpoint family plugs in its own {@code CatalogSource}
 * rather than defining a separate table and Calcite scan hierarchy.
 */
public interface CatalogSource {

  /** Fixed schema mapping each column name to its type. */
  Map<String, ExprType> getFieldTypes();

  /**
   * Builds the read-only request whose {@link OpenSearchSystemRequest#search()} yields the rows.
   */
  OpenSearchSystemRequest createRequest();

  /**
   * Whether the enumerable scan should carry the {@link org.opensearch.sql.calcite.plan.Scannable}
   * marker for the {@code collect} short-circuit. Defaults to {@code false}.
   */
  default boolean isScannable() {
    return false;
  }

  /** The V2 physical plan path, or throws when the source is Calcite only. */
  PhysicalPlan implementV2(LogicalPlan plan);
}
