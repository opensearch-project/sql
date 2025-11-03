/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import static org.mockito.Mockito.doReturn;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.test.CalciteAssert;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.sql.common.setting.Settings;

public class CalcitePPLRexTest extends CalcitePPLAbstractTest {
  public CalcitePPLRexTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Before
  public void setUp() {
    doReturn(10).when(settings).getSettingValue(Settings.Key.PPL_REX_MAX_MATCH_LIMIT);
  }

  @Test
  public void testRexBasicFieldExtraction() {
    String ppl = "source=EMP | rex field=ENAME '(?<first>[A-Z]).*' | fields ENAME, first";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(ENAME=[$1], first=[REX_EXTRACT($1, '(?<first>[A-Z]).*', 'first')])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `ENAME`, `REX_EXTRACT`(`ENAME`, '(?<first>[A-Z]).*', 'first') `first`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testRexMultipleNamedGroups() {
    String ppl =
        "source=EMP | rex field=ENAME '(?<first>[A-Z])(?<rest>.*)' | fields ENAME, first, rest";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(ENAME=[$1], first=[REX_EXTRACT($1, '(?<first>[A-Z])(?<rest>.*)', 'first')],"
            + " rest=[REX_EXTRACT($1, '(?<first>[A-Z])(?<rest>.*)', 'rest')])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `ENAME`, `REX_EXTRACT`(`ENAME`, '(?<first>[A-Z])(?<rest>.*)', 'first') `first`,"
            + " `REX_EXTRACT`(`ENAME`, '(?<first>[A-Z])(?<rest>.*)', 'rest') `rest`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testRexWithMaxMatch() {
    String ppl =
        "source=EMP | rex field=ENAME '(?<letter>[A-Z])' max_match=3 | fields ENAME, letter";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(ENAME=[$1], letter=[REX_EXTRACT_MULTI($1, '(?<letter>[A-Z])', 'letter',"
            + " 3)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `ENAME`, `REX_EXTRACT_MULTI`(`ENAME`, '(?<letter>[A-Z])', 'letter', 3) `letter`\n"
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
        "LogicalProject(ENAME=[$1], JOB=[$2], firstinitial=[REX_EXTRACT($1, '(?<firstinitial>^.)',"
            + " 'firstinitial')], jobtype=[REX_EXTRACT($2, '(?<jobtype>\\w+)', 'jobtype')])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `ENAME`, `JOB`, `REX_EXTRACT`(`ENAME`, '(?<firstinitial>^.)', 'firstinitial')"
            + " `firstinitial`, `REX_EXTRACT`(`JOB`, '(?<jobtype>\\w+)', 'jobtype') `jobtype`\n"
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
        "LogicalProject(ENAME=[$1], first=[REX_EXTRACT($1, '(?<first>[A-Z]).*', 'first')],"
            + " SAL=[$5])\n"
            + "  LogicalFilter(condition=[>($5, 1000)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `ENAME`, `REX_EXTRACT`(`ENAME`, '(?<first>[A-Z]).*', 'first') `first`, `SAL`\n"
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
            + "    LogicalProject(jobtype=[REX_EXTRACT($2, '(?<jobtype>\\w+)', 'jobtype')])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT COUNT(*) `count()`, `REX_EXTRACT`(`JOB`, '(?<jobtype>\\w+)', 'jobtype') `jobtype`\n"
            + "FROM `scott`.`EMP`\n"
            + "GROUP BY `REX_EXTRACT`(`JOB`, '(?<jobtype>\\w+)', 'jobtype')";
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
            + " 'prefix')], suffix=[REX_EXTRACT($1, '(?<prefix>[A-Z]{2})(?<suffix>[A-Z]+)',"
            + " 'suffix')])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `ENAME`, `REX_EXTRACT`(`ENAME`, '(?<prefix>[A-Z]{2})(?<suffix>[A-Z]+)', 'prefix')"
            + " `prefix`, `REX_EXTRACT`(`ENAME`, '(?<prefix>[A-Z]{2})(?<suffix>[A-Z]+)', 'suffix')"
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
            + " 'firstletter')])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `ENAME`, `REX_EXTRACT`(`ENAME`, '(?<firstletter>^.)', 'firstletter')"
            + " `firstletter`\n"
            + "FROM `scott`.`EMP`\n"
            + "ORDER BY 2\n"
            + "LIMIT 5";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testRexWithMaxMatchZero() {
    // Test that max_match=0 (unlimited) is capped to the configured limit
    String ppl =
        "source=EMP | rex field=ENAME '(?<letter>[A-Z])' max_match=0 | fields ENAME, letter";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(ENAME=[$1], letter=[REX_EXTRACT_MULTI($1, '(?<letter>[A-Z])', 'letter',"
            + " 10)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `ENAME`, `REX_EXTRACT_MULTI`(`ENAME`, '(?<letter>[A-Z])', 'letter', 10) `letter`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRexWithMaxMatchExceedsLimit() {
    // Test that max_match exceeding the configured limit throws an exception
    String ppl =
        "source=EMP | rex field=ENAME '(?<letter>[A-Z])' max_match=100 | fields ENAME, letter";
    getRelNode(ppl);
  }

  @Test
  public void testRexWithMaxMatchWithinLimit() {
    String ppl =
        "source=EMP | rex field=ENAME '(?<letter>[A-Z])' max_match=5 | fields ENAME, letter";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(ENAME=[$1], letter=[REX_EXTRACT_MULTI($1, '(?<letter>[A-Z])', 'letter',"
            + " 5)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `ENAME`, `REX_EXTRACT_MULTI`(`ENAME`, '(?<letter>[A-Z])', 'letter', 5) `letter`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testRexWithMaxMatchAtLimit() {
    // Test that max_match exactly at the limit works
    String ppl =
        "source=EMP | rex field=ENAME '(?<letter>[A-Z])' max_match=10 | fields ENAME, letter";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(ENAME=[$1], letter=[REX_EXTRACT_MULTI($1, '(?<letter>[A-Z])', 'letter',"
            + " 10)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `ENAME`, `REX_EXTRACT_MULTI`(`ENAME`, '(?<letter>[A-Z])', 'letter', 10) `letter`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testRexSedMode() {
    String ppl = "source=EMP | rex field=ENAME mode=sed 's/([A-Z])([a-z]*)/\\1/' | fields ENAME";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(ENAME=[REGEXP_REPLACE($1, '([A-Z])([a-z]*)', '$1')])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT REGEXP_REPLACE(`ENAME`, '([A-Z])([a-z]*)', '$1') `ENAME`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testRexWithOffsetField() {
    String ppl =
        "source=EMP | rex field=ENAME '(?<first>[A-Z]).*' offset_field=offsets | fields ENAME,"
            + " first, offsets";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(ENAME=[$1], first=[REX_EXTRACT($1, '(?<first>[A-Z]).*', 'first')],"
            + " offsets=[REX_OFFSET($1, '(?<first>[A-Z]).*')])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `ENAME`, `REX_EXTRACT`(`ENAME`, '(?<first>[A-Z]).*', 'first') `first`,"
            + " `REX_OFFSET`(`ENAME`, '(?<first>[A-Z]).*') `offsets`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testRexWithMultipleNamedGroupsAndOffsetField() {
    String ppl =
        "source=EMP | rex field=ENAME '(?<first>[A-Z])(?<rest>.*)' offset_field=positions | fields"
            + " ENAME, first, rest, positions";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(ENAME=[$1], first=[REX_EXTRACT($1, '(?<first>[A-Z])(?<rest>.*)', 'first')],"
            + " rest=[REX_EXTRACT($1, '(?<first>[A-Z])(?<rest>.*)', 'rest')],"
            + " positions=[REX_OFFSET($1, '(?<first>[A-Z])(?<rest>.*)')])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `ENAME`, `REX_EXTRACT`(`ENAME`, '(?<first>[A-Z])(?<rest>.*)', 'first') `first`,"
            + " `REX_EXTRACT`(`ENAME`, '(?<first>[A-Z])(?<rest>.*)', 'rest') `rest`,"
            + " `REX_OFFSET`(`ENAME`, '(?<first>[A-Z])(?<rest>.*)') `positions`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testRexWithMaxMatchAndOffsetField() {
    String ppl =
        "source=EMP | rex field=ENAME '(?<letter>[A-Z])' max_match=3 offset_field=positions |"
            + " fields ENAME, letter, positions";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(ENAME=[$1], letter=[REX_EXTRACT_MULTI($1, '(?<letter>[A-Z])', 'letter',"
            + " 3)], positions=[REX_OFFSET($1, '(?<letter>[A-Z])')])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `ENAME`, `REX_EXTRACT_MULTI`(`ENAME`, '(?<letter>[A-Z])', 'letter', 3) `letter`,"
            + " `REX_OFFSET`(`ENAME`, '(?<letter>[A-Z])') `positions`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }
}
