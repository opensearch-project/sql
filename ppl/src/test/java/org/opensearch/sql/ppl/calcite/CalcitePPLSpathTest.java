/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.test.CalciteAssert;
import org.junit.Test;

public class CalcitePPLSpathTest extends CalcitePPLAbstractTest {

  public CalcitePPLSpathTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Test
  public void testSimpleEval() {
    String ppl = "source=EMP | spath src.path input=ENAME";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7], src.path=[JSON_EXTRACT($1, 'src.path':VARCHAR)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`,"
            + " JSON_EXTRACT(`ENAME`, 'src.path') `src.path`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testEvalWithOutput() {
    String ppl = "source=EMP | spath src.path input=ENAME output=custom | fields custom";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(custom=[JSON_EXTRACT($1, 'src.path':VARCHAR)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT JSON_EXTRACT(`ENAME`, 'src.path') `custom`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testExtractAllNoPath() {
    String ppl = "source=EMP | spath input=ENAME";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7], ENAME=[JSON_EXTRACT_ALL($1)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
  }

  @Test
  public void testExtractAllNoPathWithOutput() {
    String ppl = "source=EMP | spath input=ENAME output=result | fields result";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(result=[JSON_EXTRACT_ALL($1)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
  }
}
