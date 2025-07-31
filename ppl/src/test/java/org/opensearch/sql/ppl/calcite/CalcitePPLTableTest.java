/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.test.CalciteAssert;
import org.junit.Test;

/** Test suite for PPL table command wildcard functionality with Calcite engine. */
public class CalcitePPLTableTest extends CalcitePPLAbstractTest {

  public CalcitePPLTableTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  /** Tests prefix wildcard pattern matching with table command. */
  @Test
  public void testTableWithPrefixWildcard() {
    String ppl = "source=EMP | table EMP*";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0])\n" + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql = "SELECT `EMPNO`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  /** Tests suffix wildcard pattern matching with table command. */
  @Test
  public void testTableWithSuffixWildcard() {
    String ppl = "source=EMP | table *NO";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], DEPTNO=[$7])\n" + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql = "SELECT `EMPNO`, `DEPTNO`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  /** Tests mixed wildcard patterns with table command. */
  @Test
  public void testTableWithMixedWildcards() {
    String ppl = "source=EMP | table JOB, EMP*, *NO";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(JOB=[$2], EMPNO=[$0], DEPTNO=[$7])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql = "SELECT `JOB`, `EMPNO`, `DEPTNO`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  /** Tests space-delimited wildcards with table command. */
  @Test
  public void testTableWithSpaceDelimitedWildcards() {
    String ppl = "source=EMP | table JOB EMP* *NO";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(JOB=[$2], EMPNO=[$0], DEPTNO=[$7])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql = "SELECT `JOB`, `EMPNO`, `DEPTNO`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }
}
