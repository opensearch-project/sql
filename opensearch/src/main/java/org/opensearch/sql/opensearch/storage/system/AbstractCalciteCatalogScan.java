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

/** An abstract relational operator representing a scan of an {@link OpenSearchCatalogTable}. */
@Getter
public abstract class AbstractCalciteCatalogScan extends TableScan {
  public final OpenSearchCatalogTable catalogTable;
  protected final RelDataType schema;

  protected AbstractCalciteCatalogScan(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      List<RelHint> hints,
      RelOptTable table,
      OpenSearchCatalogTable catalogTable,
      RelDataType schema) {
    super(cluster, traitSet, hints, table);
    this.catalogTable = requireNonNull(catalogTable, "OpenSearch catalog table");
    this.schema = schema;
  }

  @Override
  public RelDataType deriveRowType() {
    return this.schema;
  }
}
