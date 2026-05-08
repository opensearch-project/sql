/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.test.CalciteAssert;
import org.junit.Test;

public class CalcitePPLAppendPipeTest extends CalcitePPLAbstractTest {
  public CalcitePPLAppendPipeTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Test
  public void testAppendPipe() {
    String ppl = "source=EMP | appendpipe [ where DEPTNO = 20 ]";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalUnion(all=[true])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n"
            + "  LogicalFilter(condition=[=($7, 20)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    verifyResultCount(root, 19); // 14 original table rows + 5 filtered subquery rows

    String expectedSparkSql =
        "SELECT *\n"
            + "FROM `scott`.`EMP`\n"
            + "UNION ALL\n"
            + "SELECT *\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE `DEPTNO` = 20";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testAppendPipeWithMergedColumns() {
    String ppl =
        "source=EMP | fields DEPTNO | appendpipe [ fields DEPTNO | eval DEPTNO_PLUS ="
            + " DEPTNO + 10 ]";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalUnion(all=[true])\n"
            + "  LogicalProject(DEPTNO=[$7], DEPTNO_PLUS=[null:INTEGER])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n"
            + "  LogicalProject(DEPTNO=[$7], DEPTNO_PLUS=[+($7, 10)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    verifyResultCount(root, 28);

    String expectedSparkSql =
        "SELECT `DEPTNO`, CAST(NULL AS INTEGER) `DEPTNO_PLUS`\n"
            + "FROM `scott`.`EMP`\n"
            + "UNION ALL\n"
            + "SELECT `DEPTNO`, `DEPTNO` + 10 `DEPTNO_PLUS`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  /**
   * Regression test: double appendpipe with different aggregations. Result count (16 = 14 + 1 avg +
   * 1 max) is verified in integration tests only because RelRunners.run() creates a new planner
   * that conflicts with shared RelNode subtrees — a test framework limitation that does not affect
   * the production path.
   */
  @Test
  public void testDoubleAppendPipe() {
    String ppl =
        "source=EMP | appendpipe [stats avg(SAL) as avg_sal] | appendpipe [stats max(SAL) as"
            + " max_sal]";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalUnion(all=[true])\n"
            + "  LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], avg_sal=[$8], max_sal=[null:DECIMAL(7, 2)])\n"
            + "    LogicalUnion(all=[true])\n"
            + "      LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], avg_sal=[null:DECIMAL(11, 6)])\n"
            + "        LogicalTableScan(table=[[scott, EMP]])\n"
            + "      LogicalProject(EMPNO=[null:SMALLINT], ENAME=[null:VARCHAR(10)],"
            + " JOB=[null:VARCHAR(9)], MGR=[null:SMALLINT], HIREDATE=[null:DATE],"
            + " SAL=[null:DECIMAL(7, 2)], COMM=[null:DECIMAL(7, 2)], DEPTNO=[null:TINYINT],"
            + " avg_sal=[$0])\n"
            + "        LogicalAggregate(group=[{}], avg_sal=[AVG($0)])\n"
            + "          LogicalProject(SAL=[$5])\n"
            + "            LogicalTableScan(table=[[scott, EMP]])\n"
            + "  LogicalProject(EMPNO=[null:SMALLINT], ENAME=[null:VARCHAR(10)],"
            + " JOB=[null:VARCHAR(9)], MGR=[null:SMALLINT], HIREDATE=[null:DATE],"
            + " SAL=[null:DECIMAL(7, 2)], COMM=[null:DECIMAL(7, 2)], DEPTNO=[null:TINYINT],"
            + " avg_sal=[null:DECIMAL(11, 6)], max_sal=[$0])\n"
            + "    LogicalAggregate(group=[{}], max_sal=[MAX($0)])\n"
            + "      LogicalProject(SAL=[$5])\n"
            + "        LogicalUnion(all=[true])\n"
            + "          LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3],"
            + " HIREDATE=[$4], SAL=[$5], COMM=[$6], DEPTNO=[$7],"
            + " avg_sal=[null:DECIMAL(11, 6)])\n"
            + "            LogicalTableScan(table=[[scott, EMP]])\n"
            + "          LogicalProject(EMPNO=[null:SMALLINT], ENAME=[null:VARCHAR(10)],"
            + " JOB=[null:VARCHAR(9)], MGR=[null:SMALLINT], HIREDATE=[null:DATE],"
            + " SAL=[null:DECIMAL(7, 2)], COMM=[null:DECIMAL(7, 2)], DEPTNO=[null:TINYINT],"
            + " avg_sal=[$0])\n"
            + "            LogicalAggregate(group=[{}], avg_sal=[AVG($0)])\n"
            + "              LogicalProject(SAL=[$5])\n"
            + "                LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
  }

  /**
   * Regression test: triple appendpipe with different aggregations. Result count (17 = 14 + 1 avg +
   * 1 max + 1 min) is verified in integration tests only — see testDoubleAppendPipe for rationale.
   */
  @Test
  public void testTripleAppendPipe() {
    String ppl =
        "source=EMP | appendpipe [stats avg(SAL) as avg_sal] | appendpipe [stats max(SAL) as"
            + " max_sal] | appendpipe [stats min(SAL) as min_sal]";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalUnion(all=[true])\n"
            + "  LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], avg_sal=[$8], max_sal=[$9],"
            + " min_sal=[null:DECIMAL(7, 2)])\n"
            + "    LogicalUnion(all=[true])\n"
            + "      LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], avg_sal=[$8],"
            + " max_sal=[null:DECIMAL(7, 2)])\n"
            + "        LogicalUnion(all=[true])\n"
            + "          LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3],"
            + " HIREDATE=[$4], SAL=[$5], COMM=[$6], DEPTNO=[$7],"
            + " avg_sal=[null:DECIMAL(11, 6)])\n"
            + "            LogicalTableScan(table=[[scott, EMP]])\n"
            + "          LogicalProject(EMPNO=[null:SMALLINT], ENAME=[null:VARCHAR(10)],"
            + " JOB=[null:VARCHAR(9)], MGR=[null:SMALLINT], HIREDATE=[null:DATE],"
            + " SAL=[null:DECIMAL(7, 2)], COMM=[null:DECIMAL(7, 2)], DEPTNO=[null:TINYINT],"
            + " avg_sal=[$0])\n"
            + "            LogicalAggregate(group=[{}], avg_sal=[AVG($0)])\n"
            + "              LogicalProject(SAL=[$5])\n"
            + "                LogicalTableScan(table=[[scott, EMP]])\n"
            + "      LogicalProject(EMPNO=[null:SMALLINT], ENAME=[null:VARCHAR(10)],"
            + " JOB=[null:VARCHAR(9)], MGR=[null:SMALLINT], HIREDATE=[null:DATE],"
            + " SAL=[null:DECIMAL(7, 2)], COMM=[null:DECIMAL(7, 2)], DEPTNO=[null:TINYINT],"
            + " avg_sal=[null:DECIMAL(11, 6)], max_sal=[$0])\n"
            + "        LogicalAggregate(group=[{}], max_sal=[MAX($0)])\n"
            + "          LogicalProject(SAL=[$5])\n"
            + "            LogicalUnion(all=[true])\n"
            + "              LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3],"
            + " HIREDATE=[$4], SAL=[$5], COMM=[$6], DEPTNO=[$7],"
            + " avg_sal=[null:DECIMAL(11, 6)])\n"
            + "                LogicalTableScan(table=[[scott, EMP]])\n"
            + "              LogicalProject(EMPNO=[null:SMALLINT], ENAME=[null:VARCHAR(10)],"
            + " JOB=[null:VARCHAR(9)], MGR=[null:SMALLINT], HIREDATE=[null:DATE],"
            + " SAL=[null:DECIMAL(7, 2)], COMM=[null:DECIMAL(7, 2)], DEPTNO=[null:TINYINT],"
            + " avg_sal=[$0])\n"
            + "                LogicalAggregate(group=[{}], avg_sal=[AVG($0)])\n"
            + "                  LogicalProject(SAL=[$5])\n"
            + "                    LogicalTableScan(table=[[scott, EMP]])\n"
            + "  LogicalProject(EMPNO=[null:SMALLINT], ENAME=[null:VARCHAR(10)],"
            + " JOB=[null:VARCHAR(9)], MGR=[null:SMALLINT], HIREDATE=[null:DATE],"
            + " SAL=[null:DECIMAL(7, 2)], COMM=[null:DECIMAL(7, 2)], DEPTNO=[null:TINYINT],"
            + " avg_sal=[null:DECIMAL(11, 6)], max_sal=[null:DECIMAL(7, 2)], min_sal=[$0])\n"
            + "    LogicalAggregate(group=[{}], min_sal=[MIN($0)])\n"
            + "      LogicalProject(SAL=[$5])\n"
            + "        LogicalUnion(all=[true])\n"
            + "          LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3],"
            + " HIREDATE=[$4], SAL=[$5], COMM=[$6], DEPTNO=[$7], avg_sal=[$8],"
            + " max_sal=[null:DECIMAL(7, 2)])\n"
            + "            LogicalUnion(all=[true])\n"
            + "              LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3],"
            + " HIREDATE=[$4], SAL=[$5], COMM=[$6], DEPTNO=[$7],"
            + " avg_sal=[null:DECIMAL(11, 6)])\n"
            + "                LogicalTableScan(table=[[scott, EMP]])\n"
            + "              LogicalProject(EMPNO=[null:SMALLINT], ENAME=[null:VARCHAR(10)],"
            + " JOB=[null:VARCHAR(9)], MGR=[null:SMALLINT], HIREDATE=[null:DATE],"
            + " SAL=[null:DECIMAL(7, 2)], COMM=[null:DECIMAL(7, 2)], DEPTNO=[null:TINYINT],"
            + " avg_sal=[$0])\n"
            + "                LogicalAggregate(group=[{}], avg_sal=[AVG($0)])\n"
            + "                  LogicalProject(SAL=[$5])\n"
            + "                    LogicalTableScan(table=[[scott, EMP]])\n"
            + "          LogicalProject(EMPNO=[null:SMALLINT], ENAME=[null:VARCHAR(10)],"
            + " JOB=[null:VARCHAR(9)], MGR=[null:SMALLINT], HIREDATE=[null:DATE],"
            + " SAL=[null:DECIMAL(7, 2)], COMM=[null:DECIMAL(7, 2)], DEPTNO=[null:TINYINT],"
            + " avg_sal=[null:DECIMAL(11, 6)], max_sal=[$0])\n"
            + "            LogicalAggregate(group=[{}], max_sal=[MAX($0)])\n"
            + "              LogicalProject(SAL=[$5])\n"
            + "                LogicalUnion(all=[true])\n"
            + "                  LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3],"
            + " HIREDATE=[$4], SAL=[$5], COMM=[$6], DEPTNO=[$7],"
            + " avg_sal=[null:DECIMAL(11, 6)])\n"
            + "                    LogicalTableScan(table=[[scott, EMP]])\n"
            + "                  LogicalProject(EMPNO=[null:SMALLINT], ENAME=[null:VARCHAR(10)],"
            + " JOB=[null:VARCHAR(9)], MGR=[null:SMALLINT], HIREDATE=[null:DATE],"
            + " SAL=[null:DECIMAL(7, 2)], COMM=[null:DECIMAL(7, 2)], DEPTNO=[null:TINYINT],"
            + " avg_sal=[$0])\n"
            + "                    LogicalAggregate(group=[{}], avg_sal=[AVG($0)])\n"
            + "                      LogicalProject(SAL=[$5])\n"
            + "                        LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
  }

  /** Regression test: double appendpipe with non-aggregation (filter) subpipeline. */
  @Test
  public void testDoubleAppendPipeWithFilter() {
    String ppl = "source=EMP | appendpipe [where DEPTNO = 20] | appendpipe [where DEPTNO = 30]";
    RelNode root = getRelNode(ppl);
    verifyResultCount(root, 25); // 14 original + 5 (dept 20) + 6 (dept 30)
  }
}
