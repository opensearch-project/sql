/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.test.CalciteAssert;
import org.junit.Test;

public class CalcitePPLAppendcolTest extends CalcitePPLAbstractTest {

  public CalcitePPLAppendcolTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Test
  public void testAppendcol() {
    String ppl = "source=EMP | appendcol [ where DEPTNO = 20 ]";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7])\n"
            + "  LogicalJoin(condition=[=($8, $9)], joinType=[full])\n"
            + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], _row_number_main_=[ROW_NUMBER() OVER ()])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n"
            + "    LogicalProject(_row_number_subsearch_=[ROW_NUMBER() OVER ()])\n"
            + "      LogicalFilter(condition=[=($7, 20)])\n"
            + "        LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    verifyResultCount(root, 14);

    String expectedSparkSql =
        "SELECT `t`.`EMPNO`, `t`.`ENAME`, `t`.`JOB`, `t`.`MGR`, `t`.`HIREDATE`, `t`.`SAL`,"
            + " `t`.`COMM`, `t`.`DEPTNO`\n"
            + "FROM (SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`,"
            + " ROW_NUMBER() OVER () `_row_number_main_`\n"
            + "FROM `scott`.`EMP`) `t`\n"
            + "FULL JOIN (SELECT ROW_NUMBER() OVER () `_row_number_subsearch_`\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE `DEPTNO` = 20) `t1` ON `t`.`_row_number_main_` ="
            + " `t1`.`_row_number_subsearch_`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testAppendcol2() {
    String ppl =
        "source=EMP | eval left_col = DEPTNO | appendcol [ eval right_col = DEPTNO | where DEPTNO ="
            + " 20 ]";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7], left_col=[$8], right_col=[$10])\n"
            + "  LogicalJoin(condition=[=($9, $11)], joinType=[full])\n"
            + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], left_col=[$7], _row_number_main_=[ROW_NUMBER()"
            + " OVER ()])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n"
            + "    LogicalProject(right_col=[$8], _row_number_subsearch_=[ROW_NUMBER() OVER ()])\n"
            + "      LogicalFilter(condition=[=($7, 20)])\n"
            + "        LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], right_col=[$7])\n"
            + "          LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    verifyResultCount(root, 14);

    String expectedSparkSql =
        "SELECT `t`.`EMPNO`, `t`.`ENAME`, `t`.`JOB`, `t`.`MGR`, `t`.`HIREDATE`, `t`.`SAL`,"
            + " `t`.`COMM`, `t`.`DEPTNO`, `t`.`left_col`, `t2`.`right_col`\n"
            + "FROM (SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`,"
            + " `DEPTNO` `left_col`, ROW_NUMBER() OVER () `_row_number_main_`\n"
            + "FROM `scott`.`EMP`) `t`\n"
            + "FULL JOIN (SELECT `right_col`, ROW_NUMBER() OVER () `_row_number_subsearch_`\n"
            + "FROM (SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`,"
            + " `DEPTNO` `right_col`\n"
            + "FROM `scott`.`EMP`) `t0`\n"
            + "WHERE `DEPTNO` = 20) `t2` ON `t`.`_row_number_main_` ="
            + " `t2`.`_row_number_subsearch_`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testAppendcolOverride() {
    String ppl = "source=EMP | appendcol override=true [ where DEPTNO = 20 ]";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[CASE(=($8, $17), $9, $0)], ENAME=[CASE(=($8, $17), $10, $1)],"
            + " JOB=[CASE(=($8, $17), $11, $2)], MGR=[CASE(=($8, $17), $12, $3)],"
            + " HIREDATE=[CASE(=($8, $17), $13, $4)], SAL=[CASE(=($8, $17), $14, $5)],"
            + " COMM=[CASE(=($8, $17), $15, $6)], DEPTNO=[CASE(=($8, $17), $16, $7)])\n"
            + "  LogicalJoin(condition=[=($8, $17)], joinType=[full])\n"
            + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], _row_number_main_=[ROW_NUMBER() OVER ()])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n"
            + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], _row_number_subsearch_=[ROW_NUMBER() OVER ()])\n"
            + "      LogicalFilter(condition=[=($7, 20)])\n"
            + "        LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    verifyResultCount(root, 14);

    String expectedSparkSql =
        "SELECT CASE WHEN `t`.`_row_number_main_` = `t1`.`_row_number_subsearch_` THEN `t1`.`EMPNO`"
            + " ELSE `t`.`EMPNO` END `EMPNO`, CASE WHEN `t`.`_row_number_main_` ="
            + " `t1`.`_row_number_subsearch_` THEN `t1`.`ENAME` ELSE `t`.`ENAME` END `ENAME`, CASE"
            + " WHEN `t`.`_row_number_main_` = `t1`.`_row_number_subsearch_` THEN `t1`.`JOB` ELSE"
            + " `t`.`JOB` END `JOB`, CASE WHEN `t`.`_row_number_main_` ="
            + " `t1`.`_row_number_subsearch_` THEN `t1`.`MGR` ELSE `t`.`MGR` END `MGR`, CASE WHEN"
            + " `t`.`_row_number_main_` = `t1`.`_row_number_subsearch_` THEN `t1`.`HIREDATE` ELSE"
            + " `t`.`HIREDATE` END `HIREDATE`, CASE WHEN `t`.`_row_number_main_` ="
            + " `t1`.`_row_number_subsearch_` THEN `t1`.`SAL` ELSE `t`.`SAL` END `SAL`, CASE WHEN"
            + " `t`.`_row_number_main_` = `t1`.`_row_number_subsearch_` THEN `t1`.`COMM` ELSE"
            + " `t`.`COMM` END `COMM`, CASE WHEN `t`.`_row_number_main_` ="
            + " `t1`.`_row_number_subsearch_` THEN `t1`.`DEPTNO` ELSE `t`.`DEPTNO` END `DEPTNO`\n"
            + "FROM (SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`,"
            + " ROW_NUMBER() OVER () `_row_number_main_`\n"
            + "FROM `scott`.`EMP`) `t`\n"
            + "FULL JOIN (SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`,"
            + " `DEPTNO`, ROW_NUMBER() OVER () `_row_number_subsearch_`\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE `DEPTNO` = 20) `t1` ON `t`.`_row_number_main_` ="
            + " `t1`.`_row_number_subsearch_`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testAppendcolStats() {
    String ppl = "source=EMP | stats count() by DEPTNO | appendcol [ stats avg(SAL) by DEPTNO ]";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(count()=[$0], DEPTNO=[$1], avg(SAL)=[$3])\n"
            + "  LogicalJoin(condition=[=($2, $4)], joinType=[full])\n"
            + "    LogicalProject(count()=[$1], DEPTNO=[$0], _row_number_main_=[ROW_NUMBER() OVER"
            + " ()])\n"
            + "      LogicalAggregate(group=[{0}], count()=[COUNT()])\n"
            + "        LogicalProject(DEPTNO=[$7])\n"
            + "          LogicalTableScan(table=[[scott, EMP]])\n"
            + "    LogicalProject(avg(SAL)=[$1], _row_number_subsearch_=[ROW_NUMBER() OVER ()])\n"
            + "      LogicalAggregate(group=[{0}], avg(SAL)=[AVG($1)])\n"
            + "        LogicalProject(DEPTNO=[$7], SAL=[$5])\n"
            + "          LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult =
        ""
            + "count()=5; DEPTNO=20; avg(SAL)=2175.\n"
            + "count()=3; DEPTNO=10; avg(SAL)=2916.666666\n"
            + "count()=6; DEPTNO=30; avg(SAL)=1566.666666\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT `t1`.`count()`, `t1`.`DEPTNO`, `t4`.`avg(SAL)`\n"
            + "FROM (SELECT COUNT(*) `count()`, `DEPTNO`, ROW_NUMBER() OVER ()"
            + " `_row_number_main_`\n"
            + "FROM `scott`.`EMP`\n"
            + "GROUP BY `DEPTNO`) `t1`\n"
            + "FULL JOIN (SELECT AVG(`SAL`) `avg(SAL)`, ROW_NUMBER() OVER ()"
            + " `_row_number_subsearch_`\n"
            + "FROM `scott`.`EMP`\n"
            + "GROUP BY `DEPTNO`) `t4` ON `t1`.`_row_number_main_` = `t4`.`_row_number_subsearch_`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testAppendcolStatsOverride() {
    String ppl =
        "source=EMP | stats count() by DEPTNO | appendcol override=true [ stats avg(SAL) by DEPTNO"
            + " ]";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(count()=[$0], DEPTNO=[CASE(=($2, $5), $4, $1)], avg(SAL)=[$3])\n"
            + "  LogicalJoin(condition=[=($2, $5)], joinType=[full])\n"
            + "    LogicalProject(count()=[$1], DEPTNO=[$0], _row_number_main_=[ROW_NUMBER() OVER"
            + " ()])\n"
            + "      LogicalAggregate(group=[{0}], count()=[COUNT()])\n"
            + "        LogicalProject(DEPTNO=[$7])\n"
            + "          LogicalTableScan(table=[[scott, EMP]])\n"
            + "    LogicalProject(avg(SAL)=[$1], DEPTNO=[$0], _row_number_subsearch_=[ROW_NUMBER()"
            + " OVER ()])\n"
            + "      LogicalAggregate(group=[{0}], avg(SAL)=[AVG($1)])\n"
            + "        LogicalProject(DEPTNO=[$7], SAL=[$5])\n"
            + "          LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult =
        ""
            + "count()=5; DEPTNO=20; avg(SAL)=2175.\n"
            + "count()=3; DEPTNO=10; avg(SAL)=2916.666666\n"
            + "count()=6; DEPTNO=30; avg(SAL)=1566.666666\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT `t1`.`count()`, CASE WHEN `t1`.`_row_number_main_` = `t4`.`_row_number_subsearch_`"
            + " THEN `t4`.`DEPTNO` ELSE `t1`.`DEPTNO` END `DEPTNO`, `t4`.`avg(SAL)`\n"
            + "FROM (SELECT COUNT(*) `count()`, `DEPTNO`, ROW_NUMBER() OVER ()"
            + " `_row_number_main_`\n"
            + "FROM `scott`.`EMP`\n"
            + "GROUP BY `DEPTNO`) `t1`\n"
            + "FULL JOIN (SELECT AVG(`SAL`) `avg(SAL)`, `DEPTNO`, ROW_NUMBER() OVER ()"
            + " `_row_number_subsearch_`\n"
            + "FROM `scott`.`EMP`\n"
            + "GROUP BY `DEPTNO`) `t4` ON `t1`.`_row_number_main_` = `t4`.`_row_number_subsearch_`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }
}
