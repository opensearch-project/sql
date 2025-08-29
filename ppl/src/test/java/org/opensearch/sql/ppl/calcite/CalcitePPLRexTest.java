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
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `ENAME`, `REX_EXTRACT`(`ENAME`, '(?<first>[A-Z]).*', 1) `first`\n"
            + "FROM `scott`.`EMP`";
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
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `ENAME`, `REX_EXTRACT`(`ENAME`, '(?<first>[A-Z])(?<rest>.*)', 1) `first`,"
            + " `REX_EXTRACT`(`ENAME`, '(?<first>[A-Z])(?<rest>.*)', 2) `rest`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testRexWithMaxMatch() {
    String ppl =
        "source=EMP | rex field=ENAME '(?<letter>[A-Z])' max_match=3 | fields ENAME, letter";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(ENAME=[$1], letter=[REX_EXTRACT_MULTI($1, '(?<letter>[A-Z])', 1, 3)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `ENAME`, `REX_EXTRACT_MULTI`(`ENAME`, '(?<letter>[A-Z])', 1, 3) `letter`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testRexSedMode() {
    String ppl = "source=EMP | rex field=ENAME mode=sed 's/A/X/' | fields ENAME";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(ENAME=[REGEXP_REPLACE($1, 'A', 'X')])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT REGEXP_REPLACE(`ENAME`, 'A', 'X') `ENAME`\n" + "FROM `scott`.`EMP`";
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
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `ENAME`, `REX_EXTRACT`(`ENAME`, '(?<first>[A-Z])', 1) `first`,"
            + " `REX_OFFSET`(`ENAME`, '(?<first>[A-Z])') `pos`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testRexChainedCommands() {
    String ppl =
        "source=EMP | rex field=ENAME '(?<firstinitial>^.)' | rex field=JOB '(?<jobtype>\\w+)' |"
            + " fields ENAME, JOB, firstinitial, jobtype";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(ENAME=[$1], JOB=[$2], firstinitial=[REX_EXTRACT($1,"
            + " '(?<firstinitial>^.)', 1)], jobtype=[REX_EXTRACT($2, '(?<jobtype>\\w+)', 1)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `ENAME`, `JOB`, `REX_EXTRACT`(`ENAME`, '(?<firstinitial>^.)', 1) `firstinitial`,"
            + " `REX_EXTRACT`(`JOB`, '(?<jobtype>\\w+)', 1) `jobtype`\n"
            + "FROM `scott`.`EMP`";
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
            + "  LogicalFilter(condition=[>($5, 1000)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `ENAME`, `REX_EXTRACT`(`ENAME`, '(?<first>[A-Z]).*', 1) `first`, `SAL`\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE `SAL` > 1000";
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
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT COUNT(*) `count()`, `REX_EXTRACT`(`JOB`, '(?<jobtype>\\w+)', 1) `jobtype`\n"
            + "FROM `scott`.`EMP`\n"
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
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `ENAME`, `REX_EXTRACT`(`ENAME`, '(?<prefix>[A-Z]{2})(?<suffix>[A-Z]+)', 1)"
            + " `prefix`, `REX_EXTRACT`(`ENAME`, '(?<prefix>[A-Z]{2})(?<suffix>[A-Z]+)', 2)"
            + " `suffix`\n"
            + "FROM `scott`.`EMP`";
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
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `ENAME`, `REX_EXTRACT`(`ENAME`, '(?<firstletter>^.)', 1) `firstletter`\n"
            + "FROM `scott`.`EMP`\n"
            + "ORDER BY 2\n"
            + "LIMIT 5";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testRexSedModeGlobalFlag() {
    String ppl = "source=EMP | rex field=ENAME mode=sed 's/A/X/g' | fields ENAME";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(ENAME=[REGEXP_REPLACE($1, 'A', 'X', 'g')])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT REGEXP_REPLACE(`ENAME`, 'A', 'X', 'g') `ENAME`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testRexSedModeCaseInsensitiveFlag() {
    String ppl = "source=EMP | rex field=ENAME mode=sed 's/a/X/i' | fields ENAME";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(ENAME=[REGEXP_REPLACE($1, 'a', 'X', 'i')])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT REGEXP_REPLACE(`ENAME`, 'a', 'X', 'i') `ENAME`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testRexSedModeGlobalCaseInsensitiveFlags() {
    String ppl = "source=EMP | rex field=ENAME mode=sed 's/a/X/gi' | fields ENAME";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(ENAME=[REGEXP_REPLACE($1, 'a', 'X', 'gi')])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT REGEXP_REPLACE(`ENAME`, 'a', 'X', 'gi') `ENAME`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testRexSedModeNthOccurrence() {
    String ppl = "source=EMP | rex field=ENAME mode=sed 's/A/X/3' | fields ENAME";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(ENAME=[REGEXP_REPLACE($1, 'A', 'X', 1, 3)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT REGEXP_REPLACE(`ENAME`, 'A', 'X', 1, 3) `ENAME`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testRexSedModeTransliteration() {
    String ppl = "source=EMP | rex field=ENAME mode=sed 'y/ABC/XYZ/' | fields ENAME";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(ENAME=[TRANSLATE3($1, 'ABC', 'XYZ')])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT TRANSLATE(`ENAME`, 'ABC', 'XYZ') `ENAME`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testRexSedModeComplexPattern() {
    String ppl =
        "source=EMP | rex field=ENAME mode=sed 's/([A-Z])([a-z]+)/\\\\2_\\\\1/g' | fields ENAME";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(ENAME=[REGEXP_REPLACE($1, '([A-Z])([a-z]+)', '$2_$1', 'g')])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT REGEXP_REPLACE(`ENAME`, '([A-Z])([a-z]+)', '$2_$1', 'g') `ENAME`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testRexModeExtractExplicit() {
    String ppl =
        "source=EMP | rex field=ENAME mode=extract '(?<first>[A-Z]).*' | fields ENAME, first";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(ENAME=[$1], first=[REX_EXTRACT($1, '(?<first>[A-Z]).*', 1)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `ENAME`, `REX_EXTRACT`(`ENAME`, '(?<first>[A-Z]).*', 1) `first`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testRexModeExtractMultipleGroups() {
    String ppl =
        "source=EMP | rex field=ENAME mode=extract '(?<first>[A-Z])(?<rest>.*)' | fields ENAME,"
            + " first, rest";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(ENAME=[$1], first=[REX_EXTRACT($1, '(?<first>[A-Z])(?<rest>.*)', 1)],"
            + " rest=[REX_EXTRACT($1, '(?<first>[A-Z])(?<rest>.*)', 2)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `ENAME`, `REX_EXTRACT`(`ENAME`, '(?<first>[A-Z])(?<rest>.*)', 1) `first`,"
            + " `REX_EXTRACT`(`ENAME`, '(?<first>[A-Z])(?<rest>.*)', 2) `rest`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testRexModeExtractWithMaxMatch() {
    String ppl =
        "source=EMP | rex field=ENAME mode=extract '(?<letter>[A-Z])' max_match=3 | fields ENAME,"
            + " letter";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(ENAME=[$1], letter=[REX_EXTRACT_MULTI($1, '(?<letter>[A-Z])', 1, 3)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `ENAME`, `REX_EXTRACT_MULTI`(`ENAME`, '(?<letter>[A-Z])', 1, 3) `letter`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }
}
