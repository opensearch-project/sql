/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.test.CalciteAssert;
import org.junit.Test;

// TODO: Extend RelToSqlConverter to support customized operator in SparkSQL dialect
public class CalcitePPLAdTest extends CalcitePPLAbstractTest {

  public CalcitePPLAdTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Test
  public void testAd() {
    String ppl = "source=EMP | where SAL < 10 | fields SAL | AD";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalAD(arguments=[{}])\n"
            + "  LogicalProject(SAL=[$5])\n"
            + "    LogicalFilter(condition=[<($5, 10)])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
  }

  @Test
  public void testAdWithCategory() {
    String ppl = "source=EMP | fields JOB, SAL | AD category_field='JOB'";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalAD(arguments=[{category_field=JOB}])\n"
            + "  LogicalProject(JOB=[$2], SAL=[$5])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
  }

  @Test
  public void testAdWithOtherParameters() {
    String ppl =
        "source=EMP | where SAL < 10 | fields JOB, SAL, DEPTNO | AD category_field='JOB'"
            + " time_field='DEPTNO' sample_size=10";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalAD(arguments=[{time_field=DEPTNO, sample_size=10, category_field=JOB}])\n"
            + "  LogicalProject(JOB=[$2], SAL=[$5], DEPTNO=[$7])\n"
            + "    LogicalFilter(condition=[<($5, 10)])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
  }
}
