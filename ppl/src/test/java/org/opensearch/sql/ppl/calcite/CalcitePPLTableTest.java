/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.test.CalciteAssert;
import org.junit.Test;

/**
 * Test suite for PPL table command functionality with Calcite engine.
 * Verifies table command works as alias for fields command.
 */
public class CalcitePPLTableTest extends CalcitePPLAbstractTest {

  public CalcitePPLTableTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  /**
   * Tests basic table command functionality.
   * Verifies logical plan generation and Spark SQL translation.
   */
  @Test
  public void testTableBasic() {
    String ppl = "source=EMP | table EMPNO, ENAME";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql = "SELECT `EMPNO`, `ENAME`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  /**
   * Verifies table command produces identical logical plans to fields command.
   * Both commands should generate the same Calcite RelNode structure.
   */
  @Test
  public void testTableEquivalentToFields() {
    String pplFields = "source=EMP | fields EMPNO, ENAME, JOB";
    String pplTable = "source=EMP | table EMPNO, ENAME, JOB";
    
    RelNode rootFields = getRelNode(pplFields);
    RelNode rootTable = getRelNode(pplTable);
    
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    
    // Both should produce identical logical plans
    verifyLogical(rootFields, expectedLogical);
    verifyLogical(rootTable, expectedLogical);

    String expectedSparkSql = "SELECT `EMPNO`, `ENAME`, `JOB`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(rootFields, expectedSparkSql);
    verifyPPLToSparkSQL(rootTable, expectedSparkSql);
  }

  /**
   * Tests table command with explicit include operator (+).
   * Should behave identically to basic table command.
   */
  @Test
  public void testTableWithPlusOperator() {
    String ppl = "source=EMP | table + EMPNO, ENAME";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql = "SELECT `EMPNO`, `ENAME`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }
}