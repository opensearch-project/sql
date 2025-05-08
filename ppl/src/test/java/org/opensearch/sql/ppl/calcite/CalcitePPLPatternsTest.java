/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import static org.mockito.Mockito.doReturn;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.test.CalciteAssert.SchemaSpec;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.sql.common.setting.Settings.Key;

public class CalcitePPLPatternsTest extends CalcitePPLAbstractTest {
  public CalcitePPLPatternsTest() {
    super(SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Before
  public void setUp() {
    doReturn("simple_pattern").when(settings).getSettingValue(Key.DEFAULT_PATTERN_METHOD);
  }

  @Test
  public void testPatternsForSimplePattern() {
    doReturn("simple_pattern").when(settings).getSettingValue(Key.DEFAULT_PATTERN_METHOD);
    String ppl = "source=EMP | patterns ENAME | fields ENAME, patterns_field";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(ENAME=[$1], patterns_field=[REGEXP_REPLACE($1, '[a-zA-Z0-9]':VARCHAR)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `ENAME`, REGEXP_REPLACE(`ENAME`, '[a-zA-Z0-9]') `patterns_field`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testPatternsWithCustomPatternForSimplePattern() {
    doReturn("simple_pattern").when(settings).getSettingValue(Key.DEFAULT_PATTERN_METHOD);
    String ppl = "source=EMP | patterns pattern='[A-H]' ENAME | fields ENAME, patterns_field";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(ENAME=[$1], patterns_field=[REGEXP_REPLACE($1, '[A-H]':VARCHAR)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `ENAME`, REGEXP_REPLACE(`ENAME`, '[A-H]') `patterns_field`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testPatternsWithCustomNewFieldForSimplePattern() {
    doReturn("simple_pattern").when(settings).getSettingValue(Key.DEFAULT_PATTERN_METHOD);
    String ppl =
        "source=EMP | patterns new_field='new_range' pattern='[A-H]' ENAME | fields ENAME,"
            + " new_range";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(ENAME=[$1], new_range=[REGEXP_REPLACE($1, '[A-H]':VARCHAR)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `ENAME`, REGEXP_REPLACE(`ENAME`, '[A-H]') `new_range`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testPatternsForBrain() {
    String ppl = "source=EMP | patterns ENAME BRAIN | fields ENAME, patterns_field";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(ENAME=[$1], patterns_field=[BRAIN_LOG_PARSER($1, brain($1) OVER ())])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `ENAME`, `BRAIN_LOG_PARSER`(`ENAME`, `brain`(`ENAME`) OVER (RANGE BETWEEN"
            + " UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) `patterns_field`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testPatternsWithCustomNewFieldForBrain() {
    String ppl = "source=EMP | patterns new_field='new_name' ENAME BRAIN | fields ENAME, new_name";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(ENAME=[$1], new_name=[BRAIN_LOG_PARSER($1, brain($1) OVER ())])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `ENAME`, `BRAIN_LOG_PARSER`(`ENAME`, `brain`(`ENAME`) OVER (RANGE BETWEEN"
            + " UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) `new_name`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testPatternsWithCustomVariableCountParameterForBrain() {
    String ppl =
        "source=EMP | patterns variable_count_threshold=2 ENAME BRAIN | fields ENAME,"
            + " patterns_field";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(ENAME=[$1], patterns_field=[BRAIN_LOG_PARSER($1, brain($1, 2) OVER ())])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `ENAME`, `BRAIN_LOG_PARSER`(`ENAME`, `brain`(`ENAME`, 2) OVER (RANGE BETWEEN"
            + " UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) `patterns_field`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testPatternsWithCustomThresholdPercentageParameterForBrain() {
    String ppl =
        "source=EMP | patterns frequency_threshold_percentage=0.2 ENAME BRAIN | fields ENAME,"
            + " patterns_field";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(ENAME=[$1], patterns_field=[BRAIN_LOG_PARSER($1, brain($1, 0.2E0:DOUBLE)"
            + " OVER ())])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `ENAME`, `BRAIN_LOG_PARSER`(`ENAME`, `brain`(`ENAME`, 2E-1) OVER (RANGE BETWEEN"
            + " UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) `patterns_field`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testPatternsWithAllValidCustomParameterForBrain() {
    String ppl =
        "source=EMP | patterns new_field='new_name' variable_count_threshold=2"
            + " frequency_threshold_percentage=0.2 ENAME BRAIN | fields ENAME, new_name";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(ENAME=[$1], new_name=[BRAIN_LOG_PARSER($1, brain($1, 0.2E0:DOUBLE, 2) OVER"
            + " ())])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `ENAME`, `BRAIN_LOG_PARSER`(`ENAME`, `brain`(`ENAME`, 2E-1, 2) OVER (RANGE BETWEEN"
            + " UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) `new_name`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }
}
