/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import org.apache.calcite.test.CalciteAssert;
import org.junit.Test;

public class CalcitePPLSpathTest extends CalcitePPLAbstractTest {

  public CalcitePPLSpathTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Test
  public void testSpathPathMode() {
    withPPLQuery("source=EMP | spath src.path input=ENAME")
        .expectLogical(
            "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
                + " COMM=[$6], DEPTNO=[$7], src.path=[JSON_EXTRACT($1, 'src.path':VARCHAR)])\n"
                + "  LogicalTableScan(table=[[scott, EMP]])\n")
        .expectSparkSQL(
            "SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`,"
                + " JSON_EXTRACT(`ENAME`, 'src.path') `src.path`\n"
                + "FROM `scott`.`EMP`");
  }

  @Test
  public void testSpathPathModeWithOutput() {
    withPPLQuery("source=EMP | spath src.path input=ENAME output=custom | fields custom")
        .expectLogical(
            "LogicalProject(custom=[JSON_EXTRACT($1, 'src.path':VARCHAR)])\n"
                + "  LogicalTableScan(table=[[scott, EMP]])\n")
        .expectSparkSQL(
            "SELECT JSON_EXTRACT(`ENAME`, 'src.path') `custom`\n" + "FROM `scott`.`EMP`");
  }

  @Test
  public void testSpathAutoExtractMode() {
    withPPLQuery("source=EMP | spath input=ENAME")
        .expectLogical(
            "LogicalProject(EMPNO=[$0], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
                + " COMM=[$6], DEPTNO=[$7], ENAME=[JSON_EXTRACT_ALL($1)])\n"
                + "  LogicalTableScan(table=[[scott, EMP]])\n")
        .expectSparkSQL(
            "SELECT `EMPNO`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`,"
                + " JSON_EXTRACT_ALL(`ENAME`) `ENAME`\n"
                + "FROM `scott`.`EMP`");
  }

  @Test
  public void testSpathAutoExtractModeWithOutput() {
    withPPLQuery("source=EMP | spath input=ENAME output=result | fields result")
        .expectLogical(
            "LogicalProject(result=[JSON_EXTRACT_ALL($1)])\n"
                + "  LogicalTableScan(table=[[scott, EMP]])\n")
        .expectSparkSQL("SELECT JSON_EXTRACT_ALL(`ENAME`) `result`\n" + "FROM `scott`.`EMP`");
  }
}
