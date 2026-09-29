/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.system;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.opensearch.sql.calcite.plan.Scannable;

/**
 * A {@link CalciteEnumerableCatalogScan} that additionally carries the {@link Scannable} marker,
 * enabling the {@code collect} short-circuit. Produced when the {@link CatalogSource} opts in via
 * {@link CatalogSource#isScannable()}.
 */
public class CalciteScannableCatalogScan extends CalciteEnumerableCatalogScan implements Scannable {
  public CalciteScannableCatalogScan(
      RelOptCluster cluster,
      List<RelHint> hints,
      RelOptTable table,
      OpenSearchCatalogTable catalogTable,
      RelDataType schema) {
    super(cluster, hints, table, catalogTable, schema);
  }
}
