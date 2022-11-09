/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.planner.physical.catalog;

import com.google.common.annotations.VisibleForTesting;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.catalog.CatalogService;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.planner.DefaultImplementor;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.logical.LogicalRelation;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.storage.Table;


/**
 * Table implementation to handle show datasources command.
 * Since catalog information is not tied to any storage engine, this info
 * is handled via Catalog Table.
 *
 */
@RequiredArgsConstructor
@EqualsAndHashCode
public class CatalogTable implements Table {

  private final CatalogService catalogService;

  @Override
  public Map<String, ExprType> getFieldTypes() {
    return CatalogTableSchema.CATALOG_TABLE_SCHEMA.getMapping();
  }

  @Override
  public PhysicalPlan implement(LogicalPlan plan) {
    return plan.accept(new CatalogTableDefaultImplementor(catalogService), null);
  }

  @VisibleForTesting
  @RequiredArgsConstructor
  public static class CatalogTableDefaultImplementor
      extends DefaultImplementor<Object> {

    private final CatalogService catalogService;

    @Override
    public PhysicalPlan visitRelation(LogicalRelation node, Object context) {
      return new CatalogTableScan(catalogService);
    }
  }

}
