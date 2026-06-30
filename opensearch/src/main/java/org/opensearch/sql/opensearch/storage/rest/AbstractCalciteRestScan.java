/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.rest;

import static java.util.Objects.requireNonNull;

import java.util.List;
import lombok.Getter;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;

/** An abstract relational operator representing a scan of a {@link RestSourceTable}. */
@Getter
public abstract class AbstractCalciteRestScan extends TableScan {
  protected final RestSourceTable restTable;
  protected final RelDataType schema;

  protected AbstractCalciteRestScan(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      List<RelHint> hints,
      RelOptTable table,
      RestSourceTable restTable,
      RelDataType schema) {
    super(cluster, traitSet, hints, table);
    this.restTable = requireNonNull(restTable, "rest source table");
    this.schema = schema;
  }

  @Override
  public RelDataType deriveRowType() {
    return this.schema;
  }
}
