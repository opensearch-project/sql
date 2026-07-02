/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner.rules;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.junit.jupiter.api.Test;

/**
 * TDD unit tests for the {@code collect} full-table-scope detector (gap 3). The detector decides
 * whether a {@code LogicalTableModify} input is a plain index copy (scan, optionally under
 * row-preserving filters) — the only shape the server-side reindex fast lane can express. Anything
 * that changes row cardinality or shape (aggregate, project/eval, join, sort, ...) is NOT full
 * scope and must take the batched-bulk path.
 *
 * <p>Nodes are built with a real {@link RelBuilder} (Calcite's {@code getInputs()} is final and
 * cannot be mocked). The predicate keys on Calcite's {@code TableScan}, which OpenSearch's {@code
 * AbstractCalciteIndexScan} extends, so the same structural check holds at runtime.
 */
public class CollectWriteStrategyTest {

  private RelBuilder builder() {
    SchemaPlus root = Frameworks.createRootSchema(true);
    root.add("T", table());
    return RelBuilder.create(Frameworks.newConfigBuilder().defaultSchema(root).build());
  }

  private static AbstractTable table() {
    return new AbstractTable() {
      @Override
      public RelDataType getRowType(RelDataTypeFactory f) {
        RelProtoDataType proto =
            t ->
                t.builder().add("ID", SqlTypeName.INTEGER).add("NAME", SqlTypeName.VARCHAR).build();
        return proto.apply(f);
      }
    };
  }

  @Test
  public void bareScanIsFullTableScope() {
    RelNode scan = builder().scan("T").build();
    assertTrue(CollectWriteStrategy.isFullTableScope(scan));
  }

  @Test
  public void filterOverScanIsFullTableScope() {
    RelBuilder b = builder();
    RelNode rel = b.scan("T").filter(b.equals(b.field("ID"), b.literal(1))).build();
    assertTrue(CollectWriteStrategy.isFullTableScope(rel));
  }

  @Test
  public void chainedFiltersOverScanIsFullTableScope() {
    RelBuilder b = builder();
    RelNode rel =
        b.scan("T")
            .filter(b.equals(b.field("ID"), b.literal(1)))
            .filter(
                b.call(org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_NOT_NULL, b.field("NAME")))
            .build();
    assertTrue(CollectWriteStrategy.isFullTableScope(rel));
  }

  @Test
  public void projectOverScanIsNotFullTableScope() {
    RelBuilder b = builder();
    RelNode rel = b.scan("T").project(b.field("ID")).build();
    assertFalse(CollectWriteStrategy.isFullTableScope(rel));
  }

  @Test
  public void aggregateOverScanIsNotFullTableScope() {
    RelBuilder b = builder();
    RelNode rel = b.scan("T").aggregate(b.groupKey("ID"), b.count(false, "c")).build();
    assertFalse(CollectWriteStrategy.isFullTableScope(rel));
  }

  @Test
  public void sortOverScanIsNotFullTableScope() {
    RelBuilder b = builder();
    RelNode rel = b.scan("T").sort(b.field("ID")).build();
    assertFalse(CollectWriteStrategy.isFullTableScope(rel));
  }

  @Test
  public void joinIsNotFullTableScope() {
    RelBuilder b = builder();
    RelNode rel =
        b.scan("T")
            .scan("T")
            .join(
                org.apache.calcite.rel.core.JoinRelType.INNER,
                b.equals(b.field(2, 0, "ID"), b.field(2, 1, "ID")))
            .build();
    assertFalse(CollectWriteStrategy.isFullTableScope(rel));
  }
}
