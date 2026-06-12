/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.plan.rel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.calcite.type.ExprDateType;
import org.opensearch.sql.calcite.type.ExprTimeStampType;
import org.opensearch.sql.calcite.type.ExprTimeType;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;

public class TemporalUdtRewriteShuttleTest {

  private final OpenSearchTypeFactory tf = OpenSearchTypeFactory.TYPE_FACTORY;

  @Test
  public void rewrites_standardTimestamp_to_udt_in_rowType() {
    RelDataType row =
        tf.createStructType(
            List.of(
                tf.createSqlType(SqlTypeName.TIMESTAMP, true),
                tf.createSqlType(SqlTypeName.DATE, true),
                tf.createSqlType(SqlTypeName.TIME, true),
                tf.createSqlType(SqlTypeName.VARCHAR, true)),
            List.of("ts", "d", "t", "name"),
            true);

    RelDataType rewritten = TemporalUdtRewriteShuttle.rewriteRowType(tf, row);

    assertTrue(rewritten.getFieldList().get(0).getType() instanceof ExprTimeStampType);
    assertTrue(rewritten.getFieldList().get(1).getType() instanceof ExprDateType);
    assertTrue(rewritten.getFieldList().get(2).getType() instanceof ExprTimeType);
    assertEquals(SqlTypeName.VARCHAR, rewritten.getFieldList().get(3).getType().getSqlTypeName());
  }

  @Test
  public void rewrites_rexLiteral_timestamp_to_udt() {
    org.apache.calcite.rex.RexBuilder rb = new org.apache.calcite.rex.RexBuilder(tf);
    org.apache.calcite.rex.RexLiteral lit =
        (org.apache.calcite.rex.RexLiteral)
            rb.makeTimestampLiteral(
                new org.apache.calcite.util.TimestampString("2024-01-02 03:04:05"), 9);
    org.apache.calcite.rex.RexNode rewritten = TemporalUdtRewriteShuttle.rewriteRex(tf, lit);
    assertTrue(rewritten.getType() instanceof ExprTimeStampType);
  }

  @Test
  public void rewrites_rexInputRef_timestamp_to_udt() {
    org.apache.calcite.rex.RexBuilder rb = new org.apache.calcite.rex.RexBuilder(tf);
    org.apache.calcite.rex.RexInputRef ref =
        rb.makeInputRef(tf.createSqlType(SqlTypeName.TIMESTAMP, true), 0);
    org.apache.calcite.rex.RexNode rewritten = TemporalUdtRewriteShuttle.rewriteRex(tf, ref);
    assertTrue(rewritten.getType() instanceof ExprTimeStampType);
    assertEquals(0, ((org.apache.calcite.rex.RexInputRef) rewritten).getIndex());
  }

  @Test
  public void rewrites_rexInputRef_date_to_udt() {
    org.apache.calcite.rex.RexBuilder rb = new org.apache.calcite.rex.RexBuilder(tf);
    org.apache.calcite.rex.RexInputRef ref =
        rb.makeInputRef(tf.createSqlType(SqlTypeName.DATE, true), 2);
    org.apache.calcite.rex.RexNode rewritten = TemporalUdtRewriteShuttle.rewriteRex(tf, ref);
    assertTrue(rewritten.getType() instanceof ExprDateType);
    assertEquals(2, ((org.apache.calcite.rex.RexInputRef) rewritten).getIndex());
  }

  @Test
  public void rewrites_rexInputRef_time_to_udt() {
    org.apache.calcite.rex.RexBuilder rb = new org.apache.calcite.rex.RexBuilder(tf);
    org.apache.calcite.rex.RexInputRef ref =
        rb.makeInputRef(tf.createSqlType(SqlTypeName.TIME, true), 1);
    org.apache.calcite.rex.RexNode rewritten = TemporalUdtRewriteShuttle.rewriteRex(tf, ref);
    assertTrue(rewritten.getType() instanceof ExprTimeType);
  }

  @Test
  public void rewrites_rexInputRef_passes_varchar_through() {
    org.apache.calcite.rex.RexBuilder rb = new org.apache.calcite.rex.RexBuilder(tf);
    org.apache.calcite.rex.RexInputRef ref =
        rb.makeInputRef(tf.createSqlType(SqlTypeName.VARCHAR, true), 0);
    org.apache.calcite.rex.RexNode rewritten = TemporalUdtRewriteShuttle.rewriteRex(tf, ref);
    assertEquals(SqlTypeName.VARCHAR, rewritten.getType().getSqlTypeName());
  }

  @Test
  public void visit_logicalValues_rewrites_rowType_to_udt() {
    org.apache.calcite.plan.RelOptCluster cluster = newCluster();
    RelDataType rowType =
        tf.createStructType(
            List.of(
                tf.createSqlType(SqlTypeName.TIMESTAMP, true),
                tf.createSqlType(SqlTypeName.VARCHAR, true)),
            List.of("ts", "name"),
            true);
    org.apache.calcite.rel.logical.LogicalValues values =
        org.apache.calcite.rel.logical.LogicalValues.createEmpty(cluster, rowType);

    org.apache.calcite.rel.RelNode rewritten = values.accept(new TemporalUdtRewriteShuttle(tf));
    assertTrue(rewritten.getRowType().getFieldList().get(0).getType() instanceof ExprTimeStampType);
    assertEquals(
        SqlTypeName.VARCHAR,
        rewritten.getRowType().getFieldList().get(1).getType().getSqlTypeName());
  }

  @Test
  public void rewrites_rexCall_timestamp_return_type_to_udt() {
    org.apache.calcite.rex.RexBuilder rb = new org.apache.calcite.rex.RexBuilder(tf);
    // Build a CAST(VARCHAR -> TIMESTAMP) call via vanilla RexBuilder.
    org.apache.calcite.rex.RexNode varcharRef =
        rb.makeInputRef(tf.createSqlType(SqlTypeName.VARCHAR, true), 0);
    RelDataType targetTs = tf.createSqlType(SqlTypeName.TIMESTAMP, true);
    org.apache.calcite.rex.RexNode cast = rb.makeCast(targetTs, varcharRef);
    org.apache.calcite.rex.RexNode rewritten = TemporalUdtRewriteShuttle.rewriteRex(tf, cast);
    assertTrue(rewritten.getType() instanceof ExprTimeStampType);
  }

  private static org.apache.calcite.plan.RelOptCluster newCluster() {
    org.apache.calcite.rex.RexBuilder rb =
        new org.apache.calcite.rex.RexBuilder(OpenSearchTypeFactory.TYPE_FACTORY);
    return org.apache.calcite.plan.RelOptCluster.create(
        new org.apache.calcite.plan.volcano.VolcanoPlanner(), rb);
  }
}
