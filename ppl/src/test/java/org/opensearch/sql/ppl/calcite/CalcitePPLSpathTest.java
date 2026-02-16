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

  // spath + common command combinations using path navigation

  @Test
  public void testSpathWithEval() {
    // spath auto-extract + eval with path navigation + arithmetic
    withPPLQuery(
            "source=EMP | spath input=ENAME output=result"
                + " | eval age = result.user.age + 1 | fields age")
        .expectLogical(
            "LogicalProject(age=[+(SAFE_CAST(AS(ITEM(JSON_EXTRACT_ALL($1),"
                + " 'user.age'), 'result.user.age')), 1.0E0)])\n"
                + "  LogicalTableScan(table=[[scott, EMP]])\n")
        .expectSparkSQL(
            "SELECT TRY_CAST((JSON_EXTRACT_ALL(`ENAME`)['user.age']"
                + " 'result.user.age') AS DOUBLE) + 1.0E0 `age`\n"
                + "FROM `scott`.`EMP`");
  }

  @Test
  public void testSpathWithStats() {
    // spath auto-extract + stats using path navigation
    withPPLQuery(
            "source=EMP | spath input=ENAME output=result" + " | stats count() by result.user.name")
        .expectLogical(
            "LogicalProject(count()=[$1], result.user.name=[$0])\n"
                + "  LogicalAggregate(group=[{0}], count()=[COUNT()])\n"
                + "    LogicalProject(result.user.name=[ITEM(JSON_EXTRACT_ALL($1),"
                + " 'user.name')])\n"
                + "      LogicalTableScan(table=[[scott, EMP]])\n")
        .expectSparkSQL(
            "SELECT COUNT(*) `count()`,"
                + " JSON_EXTRACT_ALL(`ENAME`)['user.name'] `result.user.name`\n"
                + "FROM `scott`.`EMP`\n"
                + "GROUP BY JSON_EXTRACT_ALL(`ENAME`)['user.name']");
  }

  @Test
  public void testSpathWithWhere() {
    // spath auto-extract + where using path navigation
    withPPLQuery(
            "source=EMP | spath input=ENAME output=result"
                + " | where result.active = 'true' | fields result")
        .expectLogical(
            "LogicalProject(result=[$8])\n"
                + "  LogicalFilter(condition=[=(AS(ITEM($8, 'active'),"
                + " 'result.active'), 'true')])\n"
                + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3],"
                + " HIREDATE=[$4], SAL=[$5], COMM=[$6], DEPTNO=[$7],"
                + " result=[JSON_EXTRACT_ALL($1)])\n"
                + "      LogicalTableScan(table=[[scott, EMP]])\n")
        .expectSparkSQL(
            "SELECT `result`\n"
                + "FROM (SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`,"
                + " `SAL`, `COMM`, `DEPTNO`, JSON_EXTRACT_ALL(`ENAME`) `result`\n"
                + "FROM `scott`.`EMP`) `t`\n"
                + "WHERE (`result`['active'] 'result.active') = 'true'");
  }

  @Test
  public void testSpathWithFields() {
    // spath auto-extract + fields selecting via path navigation
    withPPLQuery(
            "source=EMP | spath input=ENAME output=result"
                + " | fields result.user.name, result.user.age")
        .expectLogical(
            "LogicalProject(result.user.name=[ITEM(JSON_EXTRACT_ALL($1), 'user.name')],"
                + " result.user.age=[ITEM(JSON_EXTRACT_ALL($1), 'user.age')])\n"
                + "  LogicalTableScan(table=[[scott, EMP]])\n")
        .expectSparkSQL(
            "SELECT JSON_EXTRACT_ALL(`ENAME`)['user.name'] `result.user.name`,"
                + " JSON_EXTRACT_ALL(`ENAME`)['user.age'] `result.user.age`\n"
                + "FROM `scott`.`EMP`");
  }

  @Test
  public void testSpathWithSort() {
    // spath auto-extract + sort by path navigation field
    withPPLQuery("source=EMP | spath input=ENAME" + " | sort ENAME | fields EMPNO, ENAME")
        .expectLogical(
            "LogicalProject(EMPNO=[$0], ENAME=[$7])\n"
                + "  LogicalSort(sort0=[$7], dir0=[ASC-nulls-first])\n"
                + "    LogicalProject(EMPNO=[$0], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
                + " SAL=[$5], COMM=[$6], DEPTNO=[$7], ENAME=[JSON_EXTRACT_ALL($1)])\n"
                + "      LogicalTableScan(table=[[scott, EMP]])\n")
        .expectSparkSQL(
            "SELECT `EMPNO`, `ENAME`\n"
                + "FROM (SELECT `EMPNO`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`,"
                + " JSON_EXTRACT_ALL(`ENAME`) `ENAME`\n"
                + "FROM `scott`.`EMP`\n"
                + "ORDER BY 8) `t0`");
  }
}
