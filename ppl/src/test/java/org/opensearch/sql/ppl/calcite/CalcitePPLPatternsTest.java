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
  public void testPatternsLabelModeForSimplePatternMethod() {
    doReturn("simple_pattern").when(settings).getSettingValue(Key.DEFAULT_PATTERN_METHOD);
    String ppl = "source=EMP | patterns ENAME | fields ENAME, pattern, tokens";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(ENAME=[$1], pattern=[SAFE_CAST(ITEM(PATTERN_PARSER(REGEXP_REPLACE($1,"
            + " '[a-zA-Z0-9]+':VARCHAR, '<*>'), $1), 'pattern'))],"
            + " tokens=[SAFE_CAST(ITEM(PATTERN_PARSER(REGEXP_REPLACE($1, '[a-zA-Z0-9]+':VARCHAR,"
            + " '<*>'), $1), 'tokens'))])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `ENAME`, SAFE_CAST(`PATTERN_PARSER`(REGEXP_REPLACE(`ENAME`, '[a-zA-Z0-9]+', '<*>'),"
            + " `ENAME`)['pattern'] AS STRING) `pattern`,"
            + " SAFE_CAST(`PATTERN_PARSER`(REGEXP_REPLACE(`ENAME`, '[a-zA-Z0-9]+', '<*>'),"
            + " `ENAME`)['tokens'] AS MAP< VARCHAR, VARCHAR ARRAY >) `tokens`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testPatternsLabelModeWithCustomPatternForSimplePatternMethod() {
    doReturn("simple_pattern").when(settings).getSettingValue(Key.DEFAULT_PATTERN_METHOD);
    String ppl = "source=EMP | patterns ENAME pattern='[A-H]' | fields ENAME, pattern, tokens";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(ENAME=[$1], pattern=[SAFE_CAST(ITEM(PATTERN_PARSER(REGEXP_REPLACE($1,"
            + " '[A-H]':VARCHAR, '<*>'), $1), 'pattern'))],"
            + " tokens=[SAFE_CAST(ITEM(PATTERN_PARSER(REGEXP_REPLACE($1, '[A-H]':VARCHAR, '<*>'),"
            + " $1), 'tokens'))])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `ENAME`, SAFE_CAST(`PATTERN_PARSER`(REGEXP_REPLACE(`ENAME`, '[A-H]', '<*>'),"
            + " `ENAME`)['pattern'] AS STRING) `pattern`,"
            + " SAFE_CAST(`PATTERN_PARSER`(REGEXP_REPLACE(`ENAME`, '[A-H]', '<*>'),"
            + " `ENAME`)['tokens'] AS MAP< VARCHAR, VARCHAR ARRAY >) `tokens`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testPatternsLabelModeWithPartitionBySimplePatternMethod() {
    doReturn("simple_pattern").when(settings).getSettingValue(Key.DEFAULT_PATTERN_METHOD);
    String ppl = "source=EMP | patterns ENAME by DEPTNO | fields ENAME, DEPTNO, pattern, tokens";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(ENAME=[$1], DEPTNO=[$7],"
            + " pattern=[SAFE_CAST(ITEM(PATTERN_PARSER(REGEXP_REPLACE($1, '[a-zA-Z0-9]+':VARCHAR,"
            + " '<*>'), $1), 'pattern'))], tokens=[SAFE_CAST(ITEM(PATTERN_PARSER(REGEXP_REPLACE($1,"
            + " '[a-zA-Z0-9]+':VARCHAR, '<*>'), $1), 'tokens'))])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `ENAME`, `DEPTNO`, SAFE_CAST(`PATTERN_PARSER`(REGEXP_REPLACE(`ENAME`,"
            + " '[a-zA-Z0-9]+', '<*>'), `ENAME`)['pattern'] AS STRING) `pattern`,"
            + " SAFE_CAST(`PATTERN_PARSER`(REGEXP_REPLACE(`ENAME`, '[a-zA-Z0-9]+', '<*>'),"
            + " `ENAME`)['tokens'] AS MAP< VARCHAR, VARCHAR ARRAY >) `tokens`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testPatternsLabelModeForBrainMethod() {
    String ppl = "source=EMP | patterns ENAME pattern_method=BRAIN | fields ENAME, pattern, tokens";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(ENAME=[$1], pattern=[SAFE_CAST(ITEM(PATTERN_PARSER($1, pattern($1, 10,"
            + " 100000) OVER ()), 'pattern'))], tokens=[SAFE_CAST(ITEM(PATTERN_PARSER($1,"
            + " pattern($1, 10, 100000) OVER ()), 'tokens'))])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `ENAME`, SAFE_CAST(`PATTERN_PARSER`(`ENAME`, `pattern`(`ENAME`, 10, 100000) OVER"
            + " (RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))['pattern'] AS STRING)"
            + " `pattern`, SAFE_CAST(`PATTERN_PARSER`(`ENAME`, `pattern`(`ENAME`, 10, 100000) OVER"
            + " (RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))['tokens'] AS MAP<"
            + " VARCHAR, VARCHAR ARRAY >) `tokens`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testPatternsLabelModeWithPartitionByForBrainMethod() {
    String ppl =
        "source=EMP | patterns ENAME by DEPTNO pattern_method=BRAIN | fields ENAME, DEPTNO,"
            + " pattern, tokens";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(ENAME=[$1], DEPTNO=[$7], pattern=[SAFE_CAST(ITEM(PATTERN_PARSER($1,"
            + " pattern($1, 10, 100000) OVER (PARTITION BY $7)), 'pattern'))],"
            + " tokens=[SAFE_CAST(ITEM(PATTERN_PARSER($1, pattern($1, 10, 100000) OVER (PARTITION"
            + " BY $7)), 'tokens'))])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `ENAME`, `DEPTNO`, SAFE_CAST(`PATTERN_PARSER`(`ENAME`, `pattern`(`ENAME`, 10,"
            + " 100000) OVER (PARTITION BY `DEPTNO` RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED"
            + " FOLLOWING))['pattern'] AS STRING) `pattern`, SAFE_CAST(`PATTERN_PARSER`(`ENAME`,"
            + " `pattern`(`ENAME`, 10, 100000) OVER (PARTITION BY `DEPTNO` RANGE BETWEEN UNBOUNDED"
            + " PRECEDING AND UNBOUNDED FOLLOWING))['tokens'] AS MAP< VARCHAR, VARCHAR ARRAY >)"
            + " `tokens`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testPatternsAggregationModeForSimplePatternMethod() {
    String ppl = "source=EMP | patterns ENAME pattern_mode=aggregation";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(pattern_count=[$1], pattern=[SAFE_CAST(ITEM(PATTERN_PARSER($0, $2),"
            + " 'pattern'))], tokens=[SAFE_CAST(ITEM(PATTERN_PARSER($0, $2), 'tokens'))])\n"
            + "  LogicalAggregate(group=[{1}], pattern_count=[COUNT($1)], sample_logs=[TAKE($0,"
            + " $2)])\n"
            + "    LogicalProject(ENAME=[$1], patterns_field=[REGEXP_REPLACE($1,"
            + " '[a-zA-Z0-9]+':VARCHAR, '<*>')], $f9=[10])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT COUNT(REGEXP_REPLACE(`ENAME`, '[a-zA-Z0-9]+', '<*>')) `pattern_count`,"
            + " SAFE_CAST(`PATTERN_PARSER`(REGEXP_REPLACE(`ENAME`, '[a-zA-Z0-9]+', '<*>'),"
            + " `TAKE`(`ENAME`, 10))['pattern'] AS STRING) `pattern`,"
            + " SAFE_CAST(`PATTERN_PARSER`(REGEXP_REPLACE(`ENAME`, '[a-zA-Z0-9]+', '<*>'),"
            + " `TAKE`(`ENAME`, 10))['tokens'] AS MAP< VARCHAR, VARCHAR ARRAY >) `tokens`\n"
            + "FROM `scott`.`EMP`\n"
            + "GROUP BY REGEXP_REPLACE(`ENAME`, '[a-zA-Z0-9]+', '<*>')";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testPatternsAggregationModeWithGroupByForSimplePatternMethod() {
    String ppl = "source=EMP | patterns ENAME by DEPTNO pattern_mode=aggregation";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(DEPTNO=[$0], pattern_count=[$2], pattern=[SAFE_CAST(ITEM(PATTERN_PARSER($1,"
            + " $3), 'pattern'))], tokens=[SAFE_CAST(ITEM(PATTERN_PARSER($1, $3), 'tokens'))])\n"
            + "  LogicalAggregate(group=[{1, 2}], pattern_count=[COUNT($2)], sample_logs=[TAKE($0,"
            + " $3)])\n"
            + "    LogicalProject(ENAME=[$1], DEPTNO=[$7], patterns_field=[REGEXP_REPLACE($1,"
            + " '[a-zA-Z0-9]+':VARCHAR, '<*>')], $f9=[10])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `DEPTNO`, COUNT(REGEXP_REPLACE(`ENAME`, '[a-zA-Z0-9]+', '<*>')) `pattern_count`,"
            + " SAFE_CAST(`PATTERN_PARSER`(REGEXP_REPLACE(`ENAME`, '[a-zA-Z0-9]+', '<*>'),"
            + " `TAKE`(`ENAME`, 10))['pattern'] AS STRING) `pattern`,"
            + " SAFE_CAST(`PATTERN_PARSER`(REGEXP_REPLACE(`ENAME`, '[a-zA-Z0-9]+', '<*>'),"
            + " `TAKE`(`ENAME`, 10))['tokens'] AS MAP< VARCHAR, VARCHAR ARRAY >) `tokens`\n"
            + "FROM `scott`.`EMP`\n"
            + "GROUP BY `DEPTNO`, REGEXP_REPLACE(`ENAME`, '[a-zA-Z0-9]+', '<*>')";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testPatternsAggregationModeForBrainMethod() {
    String ppl = "source=EMP | patterns ENAME pattern_method=BRAIN pattern_mode=aggregation";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalAggregate(group=[{}], patterns_field=[pattern($0, $1, $2)])\n"
            + "  LogicalProject(ENAME=[$1], $f8=[10], $f9=[100000])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `pattern`(`ENAME`, 10, 100000) `patterns_field`\nFROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testPatternsAggregationModeWithGroupByForBrainMethod() {
    String ppl =
        "source=EMP | patterns ENAME by DEPTNO pattern_method=BRAIN pattern_mode=aggregation";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalAggregate(group=[{1}], patterns_field=[pattern($0, $2, $3)])\n"
            + "  LogicalProject(ENAME=[$1], DEPTNO=[$7], $f8=[10], $f9=[100000])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `DEPTNO`, `pattern`(`ENAME`, 10, 100000) `patterns_field`\n"
            + "FROM `scott`.`EMP`\n"
            + "GROUP BY `DEPTNO`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }
}
