/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.test.CalciteAssert;
import org.junit.Test;

public class CalcitePPLStreamstatsTest extends CalcitePPLAbstractTest {

  public CalcitePPLStreamstatsTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Test
  public void testStreamstatsBy() {
    String ppl = "source=EMP | streamstats max(SAL) by DEPTNO";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7], max(SAL)=[$9])\n"
            + "  LogicalSort(sort0=[$8], dir0=[ASC])\n"
            + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], __stream_seq__=[$8], max(SAL)=[MAX($5) OVER"
            + " (PARTITION BY $7 ROWS UNBOUNDED PRECEDING)])\n"
            + "      LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], __stream_seq__=[ROW_NUMBER() OVER ()])\n"
            + "        LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`, MAX(`SAL`)"
            + " OVER (PARTITION BY `DEPTNO` ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)"
            + " `max(SAL)`\n"
            + "FROM (SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`,"
            + " ROW_NUMBER() OVER () `__stream_seq__`\n"
            + "FROM `scott`.`EMP`) `t`\n"
            + "ORDER BY `__stream_seq__` NULLS LAST";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testStreamstatsByNullBucket() {
    String ppl = "source=EMP | streamstats bucket_nullable=false max(SAL) by DEPTNO";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7], max(SAL)=[$9])\n"
            + "  LogicalSort(sort0=[$8], dir0=[ASC])\n"
            + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], __stream_seq__=[$8], max(SAL)=[CASE(IS NOT"
            + " NULL($7), MAX($5) OVER (PARTITION BY $7 ROWS UNBOUNDED PRECEDING), null:DECIMAL(7,"
            + " 2))])\n"
            + "      LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], __stream_seq__=[ROW_NUMBER() OVER ()])\n"
            + "        LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`, CASE WHEN"
            + " `DEPTNO` IS NOT NULL THEN MAX(`SAL`) OVER (PARTITION BY `DEPTNO` ROWS BETWEEN"
            + " UNBOUNDED PRECEDING AND CURRENT ROW) ELSE NULL END `max(SAL)`\n"
            + "FROM (SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`,"
            + " ROW_NUMBER() OVER () `__stream_seq__`\n"
            + "FROM `scott`.`EMP`) `t`\n"
            + "ORDER BY `__stream_seq__` NULLS LAST";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testStreamstatsCurrent() {
    String ppl = "source=EMP | streamstats current = false max(SAL)";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7], max(SAL)=[MAX($5) OVER (ROWS BETWEEN UNBOUNDED PRECEDING"
            + " AND 1 PRECEDING)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`, MAX(`SAL`)"
            + " OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) `max(SAL)`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testStreamstatsWindow() {
    String ppl = "source=EMP | streamstats window = 5 max(SAL) by DEPTNO";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7], max(SAL)=[$9])\n"
            + "  LogicalSort(sort0=[$8], dir0=[ASC])\n"
            + "    LogicalCorrelate(correlation=[$cor0], joinType=[left], requiredColumns=[{7,"
            + " 8}])\n"
            + "      LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], __stream_seq__=[ROW_NUMBER() OVER ()])\n"
            + "        LogicalTableScan(table=[[scott, EMP]])\n"
            + "      LogicalAggregate(group=[{}], max(SAL)=[MAX($0)])\n"
            + "        LogicalProject(SAL=[$5])\n"
            + "          LogicalFilter(condition=[AND(>=($8, -($cor0.__stream_seq__, 4)), <=($8,"
            + " $cor0.__stream_seq__), OR(=($7, $cor0.DEPTNO), AND(IS NULL($7), IS"
            + " NULL($cor0.DEPTNO))))])\n"
            + "            LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3],"
            + " HIREDATE=[$4], SAL=[$5], COMM=[$6], DEPTNO=[$7], __stream_seq__=[ROW_NUMBER() OVER"
            + " ()])\n"
            + "              LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `$cor0`.`EMPNO`, `$cor0`.`ENAME`, `$cor0`.`JOB`, `$cor0`.`MGR`, `$cor0`.`HIREDATE`,"
            + " `$cor0`.`SAL`, `$cor0`.`COMM`, `$cor0`.`DEPTNO`, `t3`.`max(SAL)`\n"
            + "FROM (SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`,"
            + " ROW_NUMBER() OVER () `__stream_seq__`\n"
            + "FROM `scott`.`EMP`) `$cor0`,\n"
            + "LATERAL (SELECT MAX(`SAL`) `max(SAL)`\n"
            + "FROM (SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`,"
            + " ROW_NUMBER() OVER () `__stream_seq__`\n"
            + "FROM `scott`.`EMP`) `t0`\n"
            + "WHERE `__stream_seq__` >= `$cor0`.`__stream_seq__` - 4 AND `__stream_seq__` <="
            + " `$cor0`.`__stream_seq__` AND (`DEPTNO` = `$cor0`.`DEPTNO` OR `DEPTNO` IS NULL AND"
            + " `$cor0`.`DEPTNO` IS NULL)) `t3`\n"
            + "ORDER BY `$cor0`.`__stream_seq__` NULLS LAST";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testStreamstatsGlobal() {
    String ppl = "source=EMP | streamstats window = 5 global= false max(SAL) by DEPTNO";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7], max(SAL)=[$9])\n"
            + "  LogicalSort(sort0=[$8], dir0=[ASC])\n"
            + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], __stream_seq__=[$8], max(SAL)=[MAX($5) OVER"
            + " (PARTITION BY $7 ROWS 4 PRECEDING)])\n"
            + "      LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], __stream_seq__=[ROW_NUMBER() OVER ()])\n"
            + "        LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`, MAX(`SAL`)"
            + " OVER (PARTITION BY `DEPTNO` ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) `max(SAL)`\n"
            + "FROM (SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`,"
            + " ROW_NUMBER() OVER () `__stream_seq__`\n"
            + "FROM `scott`.`EMP`) `t`\n"
            + "ORDER BY `__stream_seq__` NULLS LAST";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testStreamstatsReset() {
    String ppl =
        "source=EMP | streamstats reset_before=SAL>100 reset_after=SAL<50 avg(SAL) by DEPTNO";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7], avg(SAL)=[$12])\n"
            + "  LogicalSort(sort0=[$8], dir0=[ASC])\n"
            + "    LogicalCorrelate(correlation=[$cor0], joinType=[left], requiredColumns=[{7, 8,"
            + " 11}])\n"
            + "      LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], __stream_seq__=[$8], __reset_before_flag__=[$9],"
            + " __reset_after_flag__=[$10], __seg_id__=[+(SUM($9) OVER (ROWS UNBOUNDED PRECEDING),"
            + " COALESCE(SUM($10) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0))])\n"
            + "        LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], __stream_seq__=[ROW_NUMBER() OVER ()],"
            + " __reset_before_flag__=[CASE(>($5, 100), 1, 0)], __reset_after_flag__=[CASE(<($5,"
            + " 50), 1, 0)])\n"
            + "          LogicalTableScan(table=[[scott, EMP]])\n"
            + "      LogicalAggregate(group=[{}], avg(SAL)=[AVG($0)])\n"
            + "        LogicalProject(SAL=[$5])\n"
            + "          LogicalFilter(condition=[AND(<=($8, $cor0.__stream_seq__), =($11,"
            + " $cor0.__seg_id__), OR(=($7, $cor0.DEPTNO), AND(IS NULL($7), IS"
            + " NULL($cor0.DEPTNO))))])\n"
            + "            LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3],"
            + " HIREDATE=[$4], SAL=[$5], COMM=[$6], DEPTNO=[$7], __stream_seq__=[$8],"
            + " __reset_before_flag__=[$9], __reset_after_flag__=[$10], __seg_id__=[+(SUM($9) OVER"
            + " (ROWS UNBOUNDED PRECEDING), COALESCE(SUM($10) OVER (ROWS BETWEEN UNBOUNDED"
            + " PRECEDING AND 1 PRECEDING), 0))])\n"
            + "              LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3],"
            + " HIREDATE=[$4], SAL=[$5], COMM=[$6], DEPTNO=[$7], __stream_seq__=[ROW_NUMBER() OVER"
            + " ()], __reset_before_flag__=[CASE(>($5, 100), 1, 0)],"
            + " __reset_after_flag__=[CASE(<($5, 50), 1, 0)])\n"
            + "                LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `$cor0`.`EMPNO`, `$cor0`.`ENAME`, `$cor0`.`JOB`, `$cor0`.`MGR`, `$cor0`.`HIREDATE`,"
            + " `$cor0`.`SAL`, `$cor0`.`COMM`, `$cor0`.`DEPTNO`, `t5`.`avg(SAL)`\n"
            + "FROM (SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`,"
            + " `__stream_seq__`, `__reset_before_flag__`, `__reset_after_flag__`,"
            + " (SUM(`__reset_before_flag__`) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT"
            + " ROW)) + COALESCE(SUM(`__reset_after_flag__`) OVER (ROWS BETWEEN UNBOUNDED PRECEDING"
            + " AND 1 PRECEDING), 0) `__seg_id__`\n"
            + "FROM (SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`,"
            + " ROW_NUMBER() OVER () `__stream_seq__`, CASE WHEN `SAL` > 100 THEN 1 ELSE 0 END"
            + " `__reset_before_flag__`, CASE WHEN `SAL` < 50 THEN 1 ELSE 0 END"
            + " `__reset_after_flag__`\n"
            + "FROM `scott`.`EMP`) `t`) `$cor0`,\n"
            + "LATERAL (SELECT AVG(`SAL`) `avg(SAL)`\n"
            + "FROM (SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`,"
            + " `__stream_seq__`, `__reset_before_flag__`, `__reset_after_flag__`,"
            + " (SUM(`__reset_before_flag__`) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT"
            + " ROW)) + COALESCE(SUM(`__reset_after_flag__`) OVER (ROWS BETWEEN UNBOUNDED PRECEDING"
            + " AND 1 PRECEDING), 0) `__seg_id__`\n"
            + "FROM (SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`,"
            + " ROW_NUMBER() OVER () `__stream_seq__`, CASE WHEN `SAL` > 100 THEN 1 ELSE 0 END"
            + " `__reset_before_flag__`, CASE WHEN `SAL` < 50 THEN 1 ELSE 0 END"
            + " `__reset_after_flag__`\n"
            + "FROM `scott`.`EMP`) `t1`) `t2`\n"
            + "WHERE `__stream_seq__` <= `$cor0`.`__stream_seq__` AND `__seg_id__` ="
            + " `$cor0`.`__seg_id__` AND (`DEPTNO` = `$cor0`.`DEPTNO` OR `DEPTNO` IS NULL AND"
            + " `$cor0`.`DEPTNO` IS NULL)) `t5`\n"
            + "ORDER BY `$cor0`.`__stream_seq__` NULLS LAST";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testStreamstatsWithReverse() {
    String ppl = "source=EMP | streamstats max(SAL) by DEPTNO | reverse";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7], max(SAL)=[$9])\n"
            + "  LogicalSort(sort0=[$8], dir0=[DESC])\n"
            + "    LogicalSort(sort0=[$8], dir0=[ASC])\n"
            + "      LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], __stream_seq__=[$8], max(SAL)=[MAX($5) OVER"
            + " (PARTITION BY $7 ROWS UNBOUNDED PRECEDING)])\n"
            + "        LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], __stream_seq__=[ROW_NUMBER() OVER ()])\n"
            + "          LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`, `max(SAL)`\n"
            + "FROM (SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`,"
            + " `__stream_seq__`, MAX(`SAL`) OVER (PARTITION BY `DEPTNO` ROWS BETWEEN UNBOUNDED"
            + " PRECEDING AND CURRENT ROW) `max(SAL)`\n"
            + "FROM (SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`,"
            + " ROW_NUMBER() OVER () `__stream_seq__`\n"
            + "FROM `scott`.`EMP`) `t`\n"
            + "ORDER BY `__stream_seq__` NULLS LAST) `t1`\n"
            + "ORDER BY `__stream_seq__` DESC NULLS FIRST";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }
}
