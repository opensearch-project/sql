/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.system;

import static java.util.Objects.requireNonNull;

import java.util.List;
import lombok.Getter;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;

@Getter
public abstract class CalciteSystemIndexScan extends TableScan {
  public final OpenSearchSystemIndex sysIndex;
  protected final RelDataType schema;

  protected CalciteSystemIndexScan(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      List<RelHint> hints,
      RelOptTable table,
      OpenSearchSystemIndex sysIndex,
      RelDataType schema) {
    super(cluster, traitSet, hints, table);
    this.sysIndex = requireNonNull(sysIndex, "OpenSearch system index");
    this.schema = schema;
  }

  @Override
  public RelDataType deriveRowType() {
    return this.schema;
  }
}
