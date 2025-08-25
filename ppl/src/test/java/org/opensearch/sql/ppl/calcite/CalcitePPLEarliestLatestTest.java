/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import org.apache.calcite.rel.RelNode;
import org.junit.Test;

public class CalcitePPLEarliestLatestTest extends CalcitePPLAggregationTestBase {

  @Test
  public void testEarliestFunction() {
    testAggregationWithAlias(
        "earliest(ENAME, HIREDATE)",
        "earliest_name",
        createSimpleAggLogicalPattern(
            "earliest_name=[ARG_MIN($0, $1)]", "LogicalProject(ENAME=[$1], HIREDATE=[$4])"),
        "earliest_name=SMITH\n",
        createSimpleAggSparkSql("ARG_MIN(`ENAME`, `HIREDATE`) `earliest_name`"));
  }

  @Test
  public void testLatestFunction() {
    testAggregationWithAlias(
        "latest(ENAME, HIREDATE)",
        "latest_name",
        createSimpleAggLogicalPattern(
            "latest_name=[ARG_MAX($0, $1)]", "LogicalProject(ENAME=[$1], HIREDATE=[$4])"),
        "latest_name=ADAMS\n",
        createSimpleAggSparkSql("ARG_MAX(`ENAME`, `HIREDATE`) `latest_name`"));
  }

  @Test
  public void testEarliestWithCustomTimeField() {
    String ppl = "source=EMP | stats earliest(ENAME, HIREDATE) as earliest_name";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalAggregate(group=[{}], earliest_name=[ARG_MIN($0, $1)])\n"
            + "  LogicalProject(ENAME=[$1], HIREDATE=[$4])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedResult = "earliest_name=SMITH\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT ARG_MIN(`ENAME`, `HIREDATE`) `earliest_name`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testLatestWithCustomTimeField() {
    String ppl = "source=EMP | stats latest(ENAME, HIREDATE) as latest_name";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalAggregate(group=[{}], latest_name=[ARG_MAX($0, $1)])\n"
            + "  LogicalProject(ENAME=[$1], HIREDATE=[$4])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedResult = "latest_name=ADAMS\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT ARG_MAX(`ENAME`, `HIREDATE`) `latest_name`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testEarliestByDepartment() {
    String ppl = "source=EMP | stats earliest(ENAME, HIREDATE) as earliest_name by DEPTNO";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(earliest_name=[$1], DEPTNO=[$0])\n"
            + "  LogicalAggregate(group=[{0}], earliest_name=[ARG_MIN($1, $2)])\n"
            + "    LogicalProject(DEPTNO=[$7], ENAME=[$1], HIREDATE=[$4])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    // Results should show earliest hired employee per department
    String expectedResult =
        "earliest_name=SMITH; DEPTNO=20\n"
            + "earliest_name=CLARK; DEPTNO=10\n"
            + "earliest_name=BLAKE; DEPTNO=30\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT ARG_MIN(`ENAME`, `HIREDATE`) `earliest_name`, `DEPTNO`\n"
            + "FROM `scott`.`EMP`\n"
            + "GROUP BY `DEPTNO`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testLatestByDepartment() {
    String ppl = "source=EMP | stats latest(ENAME, HIREDATE) as latest_name by DEPTNO";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(latest_name=[$1], DEPTNO=[$0])\n"
            + "  LogicalAggregate(group=[{0}], latest_name=[ARG_MAX($1, $2)])\n"
            + "    LogicalProject(DEPTNO=[$7], ENAME=[$1], HIREDATE=[$4])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedResult =
        "latest_name=ADAMS; DEPTNO=20\n"
            + "latest_name=MILLER; DEPTNO=10\n"
            + "latest_name=JAMES; DEPTNO=30\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT ARG_MAX(`ENAME`, `HIREDATE`) `latest_name`, `DEPTNO`\n"
            + "FROM `scott`.`EMP`\n"
            + "GROUP BY `DEPTNO`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testEarliestLatestCombined() {
    String ppl =
        "source=EMP | stats earliest(ENAME, HIREDATE) as earliest_name, latest(ENAME, HIREDATE) as"
            + " latest_name by DEPTNO";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(earliest_name=[$1], latest_name=[$2], DEPTNO=[$0])\n"
            + "  LogicalAggregate(group=[{0}], earliest_name=[ARG_MIN($1, $2)],"
            + " latest_name=[ARG_MAX($1, $2)])\n"
            + "    LogicalProject(DEPTNO=[$7], ENAME=[$1], HIREDATE=[$4])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedResult =
        "earliest_name=SMITH; latest_name=ADAMS; DEPTNO=20\n"
            + "earliest_name=CLARK; latest_name=MILLER; DEPTNO=10\n"
            + "earliest_name=BLAKE; latest_name=JAMES; DEPTNO=30\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT ARG_MIN(`ENAME`, `HIREDATE`) `earliest_name`, "
            + "ARG_MAX(`ENAME`, `HIREDATE`) `latest_name`, `DEPTNO`\n"
            + "FROM `scott`.`EMP`\n"
            + "GROUP BY `DEPTNO`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testEarliestWithOtherAggregates() {
    String ppl =
        "source=EMP | stats earliest(ENAME, HIREDATE) as earliest_name, count() as cnt, avg(SAL) as"
            + " avg_sal by DEPTNO";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(earliest_name=[$1], cnt=[$2], avg_sal=[$3], DEPTNO=[$0])\n"
            + "  LogicalAggregate(group=[{0}], earliest_name=[ARG_MIN($1, $2)], cnt=[COUNT()],"
            + " avg_sal=[AVG($3)])\n"
            + "    LogicalProject(DEPTNO=[$7], ENAME=[$1], HIREDATE=[$4], SAL=[$5])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedResult =
        "earliest_name=SMITH; cnt=5; avg_sal=2175.; DEPTNO=20\n"
            + "earliest_name=CLARK; cnt=3; avg_sal=2916.666666; DEPTNO=10\n"
            + "earliest_name=BLAKE; cnt=6; avg_sal=1566.666666; DEPTNO=30\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT ARG_MIN(`ENAME`, `HIREDATE`) `earliest_name`, "
            + "COUNT(*) `cnt`, AVG(`SAL`) `avg_sal`, `DEPTNO`\n"
            + "FROM `scott`.`EMP`\n"
            + "GROUP BY `DEPTNO`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testEarliestSalaryByJob() {
    String ppl = "source=EMP | stats earliest(SAL, HIREDATE) as earliest_salary by JOB";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(earliest_salary=[$1], JOB=[$0])\n"
            + "  LogicalAggregate(group=[{0}], earliest_salary=[ARG_MIN($1, $2)])\n"
            + "    LogicalProject(JOB=[$2], SAL=[$5], HIREDATE=[$4])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedResult =
        "earliest_salary=1600.00; JOB=SALESMAN\n"
            + "earliest_salary=3000.00; JOB=ANALYST\n"
            + "earliest_salary=800.00; JOB=CLERK\n"
            + "earliest_salary=5000.00; JOB=PRESIDENT\n"
            + "earliest_salary=2850.00; JOB=MANAGER\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT ARG_MIN(`SAL`, `HIREDATE`) `earliest_salary`, `JOB`\n"
            + "FROM `scott`.`EMP`\n"
            + "GROUP BY `JOB`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testLatestSalaryByJob() {
    String ppl = "source=EMP | stats latest(SAL, HIREDATE) as latest_salary by JOB";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(latest_salary=[$1], JOB=[$0])\n"
            + "  LogicalAggregate(group=[{0}], latest_salary=[ARG_MAX($1, $2)])\n"
            + "    LogicalProject(JOB=[$2], SAL=[$5], HIREDATE=[$4])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedResult =
        "latest_salary=1250.00; JOB=SALESMAN\n"
            + "latest_salary=3000.00; JOB=ANALYST\n"
            + "latest_salary=1100.00; JOB=CLERK\n"
            + "latest_salary=5000.00; JOB=PRESIDENT\n"
            + "latest_salary=2450.00; JOB=MANAGER\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT ARG_MAX(`SAL`, `HIREDATE`) `latest_salary`, `JOB`\n"
            + "FROM `scott`.`EMP`\n"
            + "GROUP BY `JOB`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testEarliestWithAlias() {
    String ppl = "source=EMP | stats earliest(ENAME, HIREDATE) as first_hired";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalAggregate(group=[{}], first_hired=[ARG_MIN($0, $1)])\n"
            + "  LogicalProject(ENAME=[$1], HIREDATE=[$4])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedResult = "first_hired=SMITH\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT ARG_MIN(`ENAME`, `HIREDATE`) `first_hired`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testLatestWithAlias() {
    String ppl = "source=EMP | stats latest(ENAME, HIREDATE) as last_hired";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalAggregate(group=[{}], last_hired=[ARG_MAX($0, $1)])\n"
            + "  LogicalProject(ENAME=[$1], HIREDATE=[$4])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedResult = "last_hired=ADAMS\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT ARG_MAX(`ENAME`, `HIREDATE`) `last_hired`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }
}
