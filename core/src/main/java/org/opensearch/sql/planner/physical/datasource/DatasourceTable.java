/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.planner.physical.datasource;

import com.google.common.annotations.VisibleForTesting;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.datasource.DatasourceService;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.planner.DefaultImplementor;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.logical.LogicalRelation;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.storage.Table;


/**
 * Table implementation to handle show datasources command.
 * Since datasource information is not tied to any storage engine, this info
 * is handled via Datasource Table.
 *
 */
@RequiredArgsConstructor
@EqualsAndHashCode
public class DatasourceTable implements Table {

  private final DatasourceService datasourceService;

  @Override
  public Map<String, ExprType> getFieldTypes() {
    return DatasourcesTableSchema.DATASOURCES_TABLE_SCHEMA.getMapping();
  }

  @Override
  public PhysicalPlan implement(LogicalPlan plan) {
    return plan.accept(new DatasourceTableDefaultImplementor(datasourceService), null);
  }

  @VisibleForTesting
  @RequiredArgsConstructor
  public static class DatasourceTableDefaultImplementor
      extends DefaultImplementor<Object> {

    private final DatasourceService datasourceService;

    @Override
    public PhysicalPlan visitRelation(LogicalRelation node, Object context) {
      return new DatasourcesTableScan(datasourceService);
    }
  }

}
