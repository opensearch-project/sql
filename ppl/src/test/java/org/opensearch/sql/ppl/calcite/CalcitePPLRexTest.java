/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.test.CalciteAssert;
import org.junit.Test;

public class CalcitePPLRexTest extends CalcitePPLAbstractTest {
  public CalcitePPLRexTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Test
  public void testRexBasicFieldExtraction() {
    String ppl = "source=EMP | rex field=ENAME '(?<first>[A-Z]).*' | fields ENAME, first";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(ENAME=[$1], first=[REX_EXTRACT($1, '(?<first>[A-Z]).*', 1)])\n"
            + "  LogicalFilter(condition=[REGEXP_CONTAINS($1, '([A-Z]).*')])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `ENAME`, `REX_EXTRACT`(`ENAME`, '(?<first>[A-Z]).*', 1) `first`\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE REGEXP_CONTAINS(`ENAME`, '([A-Z]).*')";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testRexMultipleNamedGroups() {
    String ppl =
        "source=EMP | rex field=ENAME '(?<first>[A-Z])(?<rest>.*)' | fields ENAME, first, rest";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(ENAME=[$1], first=[REX_EXTRACT($1, '(?<first>[A-Z])(?<rest>.*)', 1)],"
            + " rest=[REX_EXTRACT($1, '(?<first>[A-Z])(?<rest>.*)', 2)])\n"
            + "  LogicalFilter(condition=[REGEXP_CONTAINS($1, '([A-Z])(.*)')])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `ENAME`, `REX_EXTRACT`(`ENAME`, '(?<first>[A-Z])(?<rest>.*)', 1) `first`,"
            + " `REX_EXTRACT`(`ENAME`, '(?<first>[A-Z])(?<rest>.*)', 2) `rest`\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE REGEXP_CONTAINS(`ENAME`, '([A-Z])(.*)')";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testRexWithMaxMatch() {
    String ppl =
        "source=EMP | rex field=ENAME '(?<letter>[A-Z])' max_match=3 | fields ENAME, letter";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(ENAME=[$1], letter=[REX_EXTRACT_MULTI($1, '(?<letter>[A-Z])', 1, 3)])\n"
            + "  LogicalFilter(condition=[REGEXP_CONTAINS($1, '([A-Z])')])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `ENAME`, `REX_EXTRACT_MULTI`(`ENAME`, '(?<letter>[A-Z])', 1, 3) `letter`\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE REGEXP_CONTAINS(`ENAME`, '([A-Z])')";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testRexSedMode() {
    String ppl = "source=EMP | rex field=ENAME mode=sed 's/A/X/' | fields ENAME";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(ENAME=[REX_SED($1, 's/A/X/')])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `REX_SED`(`ENAME`, 's/A/X/') `ENAME`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testRexWithOffsetField() {
    String ppl =
        "source=EMP | rex field=ENAME '(?<first>[A-Z])' offset_field=pos | fields ENAME, first,"
            + " pos";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(ENAME=[$1], first=[REX_EXTRACT($1, '(?<first>[A-Z])', 1)],"
            + " pos=[REX_OFFSET($1, '(?<first>[A-Z])')])\n"
            + "  LogicalFilter(condition=[REGEXP_CONTAINS($1, '([A-Z])')])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `ENAME`, `REX_EXTRACT`(`ENAME`, '(?<first>[A-Z])', 1) `first`,"
            + " `REX_OFFSET`(`ENAME`, '(?<first>[A-Z])') `pos`\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE REGEXP_CONTAINS(`ENAME`, '([A-Z])')";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testRexChainedCommands() {
    String ppl =
        "source=EMP | rex field=ENAME '(?<firstinitial>^.)' | rex field=JOB '(?<jobtype>\\w+)' |"
            + " fields ENAME, JOB, firstinitial, jobtype";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(ENAME=[$1], JOB=[$2], firstinitial=[$8], jobtype=[REX_EXTRACT($2,"
            + " '(?<jobtype>\\w+)', 1)])\n"
            + "  LogicalFilter(condition=[REGEXP_CONTAINS($2, '(\\w+)')])\n"
            + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], firstinitial=[REX_EXTRACT($1,"
            + " '(?<firstinitial>^.)', 1)])\n"
            + "      LogicalFilter(condition=[REGEXP_CONTAINS($1, '(^.)')])\n"
            + "        LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `ENAME`, `JOB`, `firstinitial`, `REX_EXTRACT`(`JOB`, '(?<jobtype>\\w+)', 1)"
            + " `jobtype`\n"
            + "FROM (SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`,"
            + " `REX_EXTRACT`(`ENAME`, '(?<firstinitial>^.)', 1) `firstinitial`\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE REGEXP_CONTAINS(`ENAME`, '(^.)')) `t0`\n"
            + "WHERE REGEXP_CONTAINS(`JOB`, '(\\w+)')";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testRexWithWhereClause() {
    String ppl =
        "source=EMP | where SAL > 1000 | rex field=ENAME '(?<first>[A-Z]).*' | fields ENAME, first,"
            + " SAL";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(ENAME=[$1], first=[REX_EXTRACT($1, '(?<first>[A-Z]).*', 1)], SAL=[$5])\n"
            + "  LogicalFilter(condition=[REGEXP_CONTAINS($1, '([A-Z]).*')])\n"
            + "    LogicalFilter(condition=[>($5, 1000)])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `ENAME`, `REX_EXTRACT`(`ENAME`, '(?<first>[A-Z]).*', 1) `first`, `SAL`\n"
            + "FROM (SELECT *\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE `SAL` > 1000) `t`\n"
            + "WHERE REGEXP_CONTAINS(`ENAME`, '([A-Z]).*')";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testRexWithAggregation() {
    String ppl = "source=EMP | rex field=JOB '(?<jobtype>\\w+)' | stats count() by jobtype";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(count()=[$1], jobtype=[$0])\n"
            + "  LogicalAggregate(group=[{0}], count()=[COUNT()])\n"
            + "    LogicalProject(jobtype=[REX_EXTRACT($2, '(?<jobtype>\\w+)', 1)])\n"
            + "      LogicalFilter(condition=[REGEXP_CONTAINS($2, '(\\w+)')])\n"
            + "        LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT COUNT(*) `count()`, `REX_EXTRACT`(`JOB`, '(?<jobtype>\\w+)', 1) `jobtype`\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE REGEXP_CONTAINS(`JOB`, '(\\w+)')\n"
            + "GROUP BY `REX_EXTRACT`(`JOB`, '(?<jobtype>\\w+)', 1)";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testRexComplexPattern() {
    String ppl =
        "source=EMP | rex field=ENAME '(?<prefix>[A-Z]{2})(?<suffix>[A-Z]+)' | fields ENAME,"
            + " prefix, suffix";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(ENAME=[$1], prefix=[REX_EXTRACT($1, '(?<prefix>[A-Z]{2})(?<suffix>[A-Z]+)',"
            + " 1)], suffix=[REX_EXTRACT($1, '(?<prefix>[A-Z]{2})(?<suffix>[A-Z]+)', 2)])\n"
            + "  LogicalFilter(condition=[REGEXP_CONTAINS($1, '([A-Z]{2})([A-Z]+)')])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `ENAME`, `REX_EXTRACT`(`ENAME`, '(?<prefix>[A-Z]{2})(?<suffix>[A-Z]+)', 1)"
            + " `prefix`, `REX_EXTRACT`(`ENAME`, '(?<prefix>[A-Z]{2})(?<suffix>[A-Z]+)', 2)"
            + " `suffix`\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE REGEXP_CONTAINS(`ENAME`, '([A-Z]{2})([A-Z]+)')";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testRexWithSort() {
    String ppl =
        "source=EMP | rex field=ENAME '(?<firstletter>^.)' | fields ENAME, firstletter | sort"
            + " firstletter | head 5";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalSort(sort0=[$1], dir0=[ASC-nulls-first], fetch=[5])\n"
            + "  LogicalProject(ENAME=[$1], firstletter=[REX_EXTRACT($1, '(?<firstletter>^.)',"
            + " 1)])\n"
            + "    LogicalFilter(condition=[REGEXP_CONTAINS($1, '(^.)')])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `ENAME`, `REX_EXTRACT`(`ENAME`, '(?<firstletter>^.)', 1) `firstletter`\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE REGEXP_CONTAINS(`ENAME`, '(^.)')\n"
            + "ORDER BY 2\n"
            + "LIMIT 5";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }
}
