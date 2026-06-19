/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.test.CalciteAssert;
import org.junit.Test;
import org.opensearch.sql.calcite.type.AbstractExprRelDataType;
import org.opensearch.sql.calcite.type.ExprDateType;
import org.opensearch.sql.calcite.type.ExprTimeStampType;
import org.opensearch.sql.calcite.type.ExprTimeType;

/**
 * Verifies that the PPL analysis stage produces logical RelNode/RexNode trees whose date columns
 * use standard Calcite {@code DATE}/{@code TIME(9)}/{@code TIMESTAMP(9)} types — never the
 * OpenSearch temporal UDTs ({@link ExprDateType}/{@link ExprTimeType}/{@link ExprTimeStampType}).
 * Temporal UDTs only appear after the prepare-statement boundary, when {@code
 * TemporalUdtRewriteShuttle} lowers the standard types to UDT for runtime execution. IP and BINARY
 * UDTs are intentionally out of scope and remain UDT throughout.
 */
public class CalcitePPLLogicalPlanStandardTemporalTest extends CalcitePPLAbstractTest {

  public CalcitePPLLogicalPlanStandardTemporalTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Test
  public void tableScanProducesStandardDate() {
    RelNode root = getRelNode("source=EMP");
    RelDataType hiredate = root.getRowType().getField("HIREDATE", false, false).getType();
    assertSame(SqlTypeName.DATE, hiredate);
    assertNotTemporalUdt(hiredate);
  }

  @Test
  public void evalCastToTimestampUsesStandardTimestamp() {
    RelNode root = getRelNode("source=EMP | eval ts = cast(HIREDATE as timestamp)");
    RelDataType ts = root.getRowType().getField("ts", false, false).getType();
    assertSame(SqlTypeName.TIMESTAMP, ts);
    assertNotTemporalUdt(ts);
    assertNoTemporalUdtAnywhere(root);
  }

  @Test
  public void evalCastToDateUsesStandardDate() {
    RelNode root = getRelNode("source=EMP | eval d = cast(HIREDATE as date)");
    RelDataType d = root.getRowType().getField("d", false, false).getType();
    assertSame(SqlTypeName.DATE, d);
    assertNotTemporalUdt(d);
    assertNoTemporalUdtAnywhere(root);
  }

  @Test
  public void evalCastToTimeUsesStandardTime() {
    RelNode root = getRelNode("source=EMP | eval t = cast('12:34:56' as time)");
    RelDataType t = root.getRowType().getField("t", false, false).getType();
    assertSame(SqlTypeName.TIME, t);
    assertNotTemporalUdt(t);
    assertNoTemporalUdtAnywhere(root);
  }

  @Test
  public void filterOnTemporalColumnKeepsStandardTypes() {
    RelNode root =
        getRelNode("source=EMP | where HIREDATE = cast('1981-06-09' as date) | fields EMPNO");
    assertNoTemporalUdtAnywhere(root);
  }

  @Test
  public void aggregateGroupByDateKeepsStandardTypes() {
    RelNode root = getRelNode("source=EMP | stats count() by HIREDATE");
    assertNoTemporalUdtAnywhere(root);
  }

  @Test
  public void timestampUdfReturnTypeIsStandardTimestamp() {
    RelNode root = getRelNode("source=EMP | eval ts = timestamp('2024-01-02 03:04:05')");
    RelDataType ts = root.getRowType().getField("ts", false, false).getType();
    assertSame(SqlTypeName.TIMESTAMP, ts);
    assertNotTemporalUdt(ts);
    assertNoTemporalUdtAnywhere(root);
  }

  // ---------- helpers ----------

  /** Asserts the type's SqlTypeName matches and the type is not a temporal UDT subclass. */
  private static void assertSame(SqlTypeName expected, RelDataType actual) {
    assertTrue(
        "expected SqlTypeName " + expected + " but got " + actual,
        actual.getSqlTypeName() == expected);
  }

  private static void assertNotTemporalUdt(RelDataType type) {
    assertFalse(
        "logical plan produced a temporal UDT (" + type.getClass().getSimpleName() + ")",
        type instanceof ExprDateType
            || type instanceof ExprTimeType
            || type instanceof ExprTimeStampType);
  }

  /**
   * Walks the entire RelNode tree (and every RexNode it stores) and asserts no temporal UDT
   * appears. IP/BINARY UDTs are still permitted.
   */
  private static void assertNoTemporalUdtAnywhere(RelNode root) {
    root.accept(
        new RelShuttleImpl() {
          @Override
          public RelNode visit(RelNode other) {
            checkRowType(other.getRowType());
            other.accept(
                new RexShuttle() {
                  @Override
                  public RexNode visitInputRef(RexInputRef inputRef) {
                    assertNotTemporalUdt(inputRef.getType());
                    return inputRef;
                  }

                  @Override
                  public RexNode visitLiteral(RexLiteral literal) {
                    assertNotTemporalUdt(literal.getType());
                    return literal;
                  }

                  @Override
                  public RexNode visitCall(RexCall call) {
                    assertNotTemporalUdt(call.getType());
                    return super.visitCall(call);
                  }
                });
            return super.visit(other);
          }
        });
  }

  private static void checkRowType(RelDataType row) {
    if (row.isStruct()) {
      for (RelDataTypeField f : row.getFieldList()) {
        assertNotTemporalUdt(f.getType());
        // Defensively reject any AbstractExprRelDataType whose ExprType is temporal.
        if (f.getType() instanceof AbstractExprRelDataType<?> udt) {
          assertFalse(
              "logical plan field " + f.getName() + " has temporal UDT " + udt,
              udt instanceof ExprDateType
                  || udt instanceof ExprTimeType
                  || udt instanceof ExprTimeStampType);
        }
      }
    } else {
      assertNotTemporalUdt(row);
    }
  }
}
