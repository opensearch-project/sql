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
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.planner.DefaultImplementor;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.logical.LogicalRelation;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.storage.Table;

/**
 * Table implementation to handle show datasources command. Since datasource information is not tied
 * to any storage engine, this info is handled via DataSource Table.
 */
@RequiredArgsConstructor
@EqualsAndHashCode
public class DataSourceTable implements Table {

  private final DataSourceService dataSourceService;

  @Override
  public Map<String, ExprType> getFieldTypes() {
    return DataSourceTableSchema.DATASOURCE_TABLE_SCHEMA.getMapping();
  }

  @Override
  public PhysicalPlan implement(LogicalPlan plan) {
    return plan.accept(new DataSourceTableDefaultImplementor(dataSourceService), null);
  }

  @VisibleForTesting
  @RequiredArgsConstructor
  public static class DataSourceTableDefaultImplementor extends DefaultImplementor<Object> {

    private final DataSourceService dataSourceService;

    @Override
    public PhysicalPlan visitRelation(LogicalRelation node, Object context) {
      return new DataSourceTableScan(dataSourceService);
    }
  }
}
