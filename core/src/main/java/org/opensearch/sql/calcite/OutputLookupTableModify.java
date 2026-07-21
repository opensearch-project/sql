/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import java.util.List;
import lombok.Getter;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.Prepare.CatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Terminal write node for outputlookup, a Calcite {@link TableModify} INSERT so the optimizer
 * treats it as a mandatory side effect (never dropped or reordered). The referenced table is the
 * destination backing index, used only so the lowering rule can reach the in-cluster client; the
 * lookup is created and written at execution time. Subclasses {@link TableModify} rather than
 * LogicalTableModify so the built-in EnumerableTableModifyRule (which needs a ModifiableTable) does
 * not fire.
 */
@Getter
public class OutputLookupTableModify extends TableModify {

  private final String indexName;
  private final boolean append;
  private final boolean overrideIfEmpty;
  private final java.util.List<String> keyFields;
  private final @Nullable Integer max;

  public OutputLookupTableModify(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelOptTable table,
      CatalogReader catalogReader,
      RelNode input,
      String indexName,
      boolean append,
      boolean overrideIfEmpty,
      java.util.List<String> keyFields,
      @Nullable Integer max) {
    super(cluster, traits, table, catalogReader, input, Operation.INSERT, null, null, false);
    this.indexName = indexName;
    this.append = append;
    this.overrideIfEmpty = overrideIfEmpty;
    this.keyFields = keyFields;
    this.max = max;
  }

  public static OutputLookupTableModify create(
      RelNode input,
      RelOptTable table,
      CatalogReader catalogReader,
      String indexName,
      boolean append,
      boolean overrideIfEmpty,
      java.util.List<String> keyFields,
      @Nullable Integer max) {
    RelOptCluster cluster = input.getCluster();
    RelTraitSet traits = cluster.traitSetOf(Convention.NONE);
    return new OutputLookupTableModify(
        cluster,
        traits,
        table,
        catalogReader,
        input,
        indexName,
        append,
        overrideIfEmpty,
        keyFields,
        max);
  }

  @Override
  public RelDataType deriveRowType() {
    RelDataTypeFactory typeFactory = getCluster().getTypeFactory();
    return typeFactory
        .builder()
        .add("rows_written", typeFactory.createSqlType(SqlTypeName.BIGINT))
        .build();
  }

  /** Registers the write-lowering rule when no OpenSearch scan in the pipeline does. */
  @Override
  public void register(org.apache.calcite.plan.RelOptPlanner planner) {
    org.opensearch.sql.calcite.plan.AbstractOpenSearchTable osTable =
        table.unwrap(org.opensearch.sql.calcite.plan.AbstractOpenSearchTable.class);
    if (osTable != null) {
      osTable.getWriteConversionRules().forEach(planner::addRule);
    }
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new OutputLookupTableModify(
        getCluster(),
        traitSet,
        getTable(),
        getCatalogReader(),
        inputs.get(0),
        indexName,
        append,
        overrideIfEmpty,
        keyFields,
        max);
  }
}
