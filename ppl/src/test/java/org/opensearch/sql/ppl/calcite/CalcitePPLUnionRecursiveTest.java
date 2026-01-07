/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.test.CalciteAssert;
import org.junit.Assert;
import org.junit.Test;
import org.opensearch.sql.exception.SemanticCheckException;

public class CalcitePPLUnionRecursiveTest extends CalcitePPLAbstractTest {

  public CalcitePPLUnionRecursiveTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Test
  public void testUnionRecursivePlan() {
    String ppl =
        "source=EMP | fields EMPNO, MGR | union recursive name=emp_hierarchy max_depth=1"
            + " [ source=emp_hierarchy | fields EMPNO, MGR ] | fields EMPNO, MGR";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalRepeatUnion(iterationLimit=[1], all=[true])\n"
            + "  LogicalTableSpool(readType=[LAZY], writeType=[LAZY], table=[[emp_hierarchy]])\n"
            + "    LogicalProject(EMPNO=[$0], MGR=[$3])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n"
            + "  LogicalTableSpool(readType=[LAZY], writeType=[LAZY], table=[[emp_hierarchy]])\n"
            + "    LogicalTableScan(table=[[emp_hierarchy]])\n";
    verifyLogical(root, expectedLogical);
  }

  @Test
  public void testUnionRecursiveSchemaMismatch() {
    String ppl =
        "source=EMP | fields EMPNO, MGR | union recursive name=emp_hierarchy"
            + " [ source=emp_hierarchy | fields EMPNO ]";
    Throwable t = Assert.assertThrows(SemanticCheckException.class, () -> getRelNode(ppl));
    Assert.assertTrue(t.getMessage().contains("UNION RECURSIVE schema mismatch"));
  }
}
