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
    doReturn("simple_pattern").when(settings).getSettingValue(Key.PATTERN_METHOD);
    doReturn("label").when(settings).getSettingValue(Key.PATTERN_MODE);
    doReturn(10).when(settings).getSettingValue(Key.PATTERN_MAX_SAMPLE_COUNT);
    doReturn(100000).when(settings).getSettingValue(Key.PATTERN_BUFFER_LIMIT);
    doReturn(false).when(settings).getSettingValue(Key.PATTERN_SHOW_NUMBERED_TOKEN);
  }

  @Test
  public void testPatternsLabelMode_NotShowNumberedToken_ForSimplePatternMethod() {
    String ppl = "source=EMP | patterns ENAME | fields ENAME, patterns_field";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(ENAME=[$1], patterns_field=[CASE(SEARCH($1, Sarg['':VARCHAR; NULL AS"
            + " TRUE]:VARCHAR), '':VARCHAR, REGEXP_REPLACE($1, '[a-zA-Z0-9]+':VARCHAR,"
            + " '<*>':VARCHAR))])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `ENAME`, CASE WHEN `ENAME` IS NULL OR `ENAME` = '' THEN '' ELSE"
            + " REGEXP_REPLACE(`ENAME`, '[a-zA-Z0-9]+', '<*>') END `patterns_field`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testPatternsLabelMode_ShowNumberedToken_ForSimplePatternMethod() {
    String ppl =
        "source=EMP | patterns ENAME show_numbered_token=true | fields ENAME, patterns_field,"
            + " tokens";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(ENAME=[$1], patterns_field=[SAFE_CAST(ITEM(PATTERN_PARSER(CASE(SEARCH($1,"
            + " Sarg['':VARCHAR; NULL AS TRUE]:VARCHAR), '':VARCHAR, REGEXP_REPLACE($1,"
            + " '[a-zA-Z0-9]+':VARCHAR, '<*>':VARCHAR)), $1), 'pattern'))],"
            + " tokens=[SAFE_CAST(ITEM(PATTERN_PARSER(CASE(SEARCH($1, Sarg['':VARCHAR; NULL AS"
            + " TRUE]:VARCHAR), '':VARCHAR, REGEXP_REPLACE($1, '[a-zA-Z0-9]+':VARCHAR,"
            + " '<*>':VARCHAR)), $1), 'tokens'))])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `ENAME`, TRY_CAST(PATTERN_PARSER(CASE WHEN `ENAME` IS NULL OR `ENAME` = '' THEN"
            + " '' ELSE REGEXP_REPLACE(`ENAME`, '[a-zA-Z0-9]+', '<*>') END, `ENAME`)['pattern'] AS"
            + " STRING) `patterns_field`, TRY_CAST(PATTERN_PARSER(CASE WHEN `ENAME` IS NULL OR"
            + " `ENAME` = '' THEN '' ELSE REGEXP_REPLACE(`ENAME`, '[a-zA-Z0-9]+', '<*>') END,"
            + " `ENAME`)['tokens'] AS MAP< VARCHAR, VARCHAR ARRAY >) `tokens`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testPatternsLabelModeWithCustomPattern_ShowNumberedToken_ForSimplePatternMethod() {
    String ppl =
        "source=EMP | patterns ENAME show_numbered_token=true pattern='[A-H]' | fields ENAME,"
            + " patterns_field, tokens";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(ENAME=[$1], patterns_field=[SAFE_CAST(ITEM(PATTERN_PARSER(CASE(SEARCH($1,"
            + " Sarg['':VARCHAR; NULL AS TRUE]:VARCHAR), '':VARCHAR, REGEXP_REPLACE($1,"
            + " '[A-H]':VARCHAR, '<*>':VARCHAR)), $1), 'pattern'))],"
            + " tokens=[SAFE_CAST(ITEM(PATTERN_PARSER(CASE(SEARCH($1, Sarg['':VARCHAR; NULL AS"
            + " TRUE]:VARCHAR), '':VARCHAR, REGEXP_REPLACE($1, '[A-H]':VARCHAR, '<*>':VARCHAR)),"
            + " $1), 'tokens'))])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `ENAME`, TRY_CAST(PATTERN_PARSER(CASE WHEN `ENAME` IS NULL OR `ENAME` = '' THEN"
            + " '' ELSE REGEXP_REPLACE(`ENAME`, '[A-H]', '<*>') END, `ENAME`)['pattern'] AS STRING)"
            + " `patterns_field`, TRY_CAST(PATTERN_PARSER(CASE WHEN `ENAME` IS NULL OR `ENAME` ="
            + " '' THEN '' ELSE REGEXP_REPLACE(`ENAME`, '[A-H]', '<*>') END, `ENAME`)['tokens'] AS"
            + " MAP< VARCHAR, VARCHAR ARRAY >) `tokens`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testPatternsLabelModeWithCustomField_NotShowNumberedToken_ForSimplePatternMethod() {
    String ppl =
        "source=EMP | patterns ENAME new_field='upper' pattern='[A-H]' | fields ENAME," + " upper";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(ENAME=[$1], upper=[CASE(SEARCH($1, Sarg['':VARCHAR; NULL AS TRUE]:VARCHAR),"
            + " '':VARCHAR, REGEXP_REPLACE($1, '[A-H]':VARCHAR, '<*>':VARCHAR))])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `ENAME`, CASE WHEN `ENAME` IS NULL OR `ENAME` = '' THEN '' ELSE"
            + " REGEXP_REPLACE(`ENAME`, '[A-H]', '<*>') END `upper`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testPatternsLabelModeWithPartitionBy_ShowNumberedToken_SimplePatternMethod() {
    String ppl =
        "source=EMP | patterns ENAME by DEPTNO show_numbered_token=true | fields ENAME, DEPTNO,"
            + " patterns_field, tokens";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(ENAME=[$1], DEPTNO=[$7],"
            + " patterns_field=[SAFE_CAST(ITEM(PATTERN_PARSER(CASE(SEARCH($1, Sarg['':VARCHAR; NULL"
            + " AS TRUE]:VARCHAR), '':VARCHAR, REGEXP_REPLACE($1, '[a-zA-Z0-9]+':VARCHAR,"
            + " '<*>':VARCHAR)), $1), 'pattern'))],"
            + " tokens=[SAFE_CAST(ITEM(PATTERN_PARSER(CASE(SEARCH($1, Sarg['':VARCHAR; NULL AS"
            + " TRUE]:VARCHAR), '':VARCHAR, REGEXP_REPLACE($1, '[a-zA-Z0-9]+':VARCHAR,"
            + " '<*>':VARCHAR)), $1), 'tokens'))])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `ENAME`, `DEPTNO`, TRY_CAST(PATTERN_PARSER(CASE WHEN `ENAME` IS NULL OR `ENAME`"
            + " = '' THEN '' ELSE REGEXP_REPLACE(`ENAME`, '[a-zA-Z0-9]+', '<*>') END,"
            + " `ENAME`)['pattern'] AS STRING) `patterns_field`, TRY_CAST(PATTERN_PARSER(CASE"
            + " WHEN `ENAME` IS NULL OR `ENAME` = '' THEN '' ELSE REGEXP_REPLACE(`ENAME`,"
            + " '[a-zA-Z0-9]+', '<*>') END, `ENAME`)['tokens'] AS MAP< VARCHAR, VARCHAR ARRAY >)"
            + " `tokens`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testPatternsLabelMode_NotShowNumberedToken_ForBrainMethod() {
    String ppl = "source=EMP | patterns ENAME method=BRAIN | fields ENAME, patterns_field";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(ENAME=[$1], patterns_field=[SAFE_CAST(ITEM(PATTERN_PARSER($1, pattern($1,"
            + " 10, 100000, false) OVER (), false), 'pattern'))])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `ENAME`, TRY_CAST(PATTERN_PARSER(`ENAME`, `pattern`(`ENAME`, 10, 100000, FALSE)"
            + " OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), FALSE)['pattern']"
            + " AS STRING) `patterns_field`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testPatternsLabelMode_ShowNumberedToken_ForBrainMethod() {
    String ppl =
        "source=EMP | patterns ENAME method=BRAIN show_numbered_token=true | fields ENAME,"
            + " patterns_field, tokens";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(ENAME=[$1], patterns_field=[SAFE_CAST(ITEM(PATTERN_PARSER($1, pattern($1,"
            + " 10, 100000, true) OVER (), true), 'pattern'))],"
            + " tokens=[SAFE_CAST(ITEM(PATTERN_PARSER($1, pattern($1, 10, 100000, true) OVER (),"
            + " true), 'tokens'))])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `ENAME`, TRY_CAST(PATTERN_PARSER(`ENAME`, `pattern`(`ENAME`, 10, 100000, TRUE)"
            + " OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), TRUE)['pattern']"
            + " AS STRING) `patterns_field`, TRY_CAST(PATTERN_PARSER(`ENAME`, `pattern`(`ENAME`,"
            + " 10, 100000, TRUE) OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),"
            + " TRUE)['tokens'] AS MAP< VARCHAR, VARCHAR ARRAY >) `tokens`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testPatternsLabelModeWithPartitionBy_NotShowNumberedToken_ForBrainMethod() {
    String ppl =
        "source=EMP | patterns ENAME by DEPTNO method=BRAIN | fields ENAME, DEPTNO,"
            + " patterns_field";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(ENAME=[$1], DEPTNO=[$7], patterns_field=[SAFE_CAST(ITEM(PATTERN_PARSER($1,"
            + " pattern($1, 10, 100000, false) OVER (PARTITION BY $7), false), 'pattern'))])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `ENAME`, `DEPTNO`, TRY_CAST(PATTERN_PARSER(`ENAME`, `pattern`(`ENAME`, 10,"
            + " 100000, FALSE) OVER (PARTITION BY `DEPTNO` RANGE BETWEEN UNBOUNDED PRECEDING AND"
            + " UNBOUNDED FOLLOWING), FALSE)['pattern'] AS STRING) `patterns_field`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testPatternsLabelModeWithPartitionBy_ShowNumberedToken_ForBrainMethod() {
    String ppl =
        "source=EMP | patterns ENAME by DEPTNO method=BRAIN show_numbered_token=true | fields"
            + " ENAME, DEPTNO, patterns_field, tokens";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(ENAME=[$1], DEPTNO=[$7], patterns_field=[SAFE_CAST(ITEM(PATTERN_PARSER($1,"
            + " pattern($1, 10, 100000, true) OVER (PARTITION BY $7), true), 'pattern'))],"
            + " tokens=[SAFE_CAST(ITEM(PATTERN_PARSER($1, pattern($1, 10, 100000, true) OVER"
            + " (PARTITION BY $7), true), 'tokens'))])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `ENAME`, `DEPTNO`, TRY_CAST(PATTERN_PARSER(`ENAME`, `pattern`(`ENAME`, 10,"
            + " 100000, TRUE) OVER (PARTITION BY `DEPTNO` RANGE BETWEEN UNBOUNDED PRECEDING AND"
            + " UNBOUNDED FOLLOWING), TRUE)['pattern'] AS STRING) `patterns_field`,"
            + " TRY_CAST(PATTERN_PARSER(`ENAME`, `pattern`(`ENAME`, 10, 100000, TRUE) OVER"
            + " (PARTITION BY `DEPTNO` RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),"
            + " TRUE)['tokens'] AS MAP< VARCHAR, VARCHAR ARRAY >) `tokens`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testPatternsAggregationMode_NotShowNumberedToken_ForSimplePatternMethod() {
    String ppl = "source=EMP | patterns ENAME mode=aggregation";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalAggregate(group=[{1}], pattern_count=[COUNT($1)], sample_logs=[TAKE($0, $2)])\n"
            + "  LogicalProject(ENAME=[$1], patterns_field=[CASE(SEARCH($1, Sarg['':VARCHAR; NULL"
            + " AS TRUE]:VARCHAR), '':VARCHAR, REGEXP_REPLACE($1, '[a-zA-Z0-9]+':VARCHAR,"
            + " '<*>':VARCHAR))], $f9=[10])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT CASE WHEN `ENAME` IS NULL OR `ENAME` = '' THEN '' ELSE REGEXP_REPLACE(`ENAME`,"
            + " '[a-zA-Z0-9]+', '<*>') END `patterns_field`, COUNT(CASE WHEN `ENAME` IS NULL OR"
            + " `ENAME` = '' THEN '' ELSE REGEXP_REPLACE(`ENAME`, '[a-zA-Z0-9]+', '<*>') END)"
            + " `pattern_count`, `TAKE`(`ENAME`, 10) `sample_logs`\n"
            + "FROM `scott`.`EMP`\n"
            + "GROUP BY CASE WHEN `ENAME` IS NULL OR `ENAME` = '' THEN '' ELSE"
            + " REGEXP_REPLACE(`ENAME`, '[a-zA-Z0-9]+', '<*>') END";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testPatternsAggregationMode_ShowNumberedToken_ForSimplePatternMethod() {
    String ppl = "source=EMP | patterns ENAME mode=aggregation show_numbered_token=true";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(patterns_field=[SAFE_CAST(ITEM(PATTERN_PARSER($0, $2), 'pattern'))],"
            + " pattern_count=[$1], tokens=[SAFE_CAST(ITEM(PATTERN_PARSER($0, $2), 'tokens'))],"
            + " sample_logs=[$2])\n"
            + "  LogicalAggregate(group=[{1}], pattern_count=[COUNT($1)], sample_logs=[TAKE($0,"
            + " $2)])\n"
            + "    LogicalProject(ENAME=[$1], patterns_field=[CASE(SEARCH($1, Sarg['':VARCHAR; NULL"
            + " AS TRUE]:VARCHAR), '':VARCHAR, REGEXP_REPLACE($1, '[a-zA-Z0-9]+':VARCHAR,"
            + " '<*>':VARCHAR))], $f9=[10])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT TRY_CAST(PATTERN_PARSER(CASE WHEN `ENAME` IS NULL OR `ENAME` = '' THEN '' ELSE"
            + " REGEXP_REPLACE(`ENAME`, '[a-zA-Z0-9]+', '<*>') END, `TAKE`(`ENAME`, 10))['pattern']"
            + " AS STRING) `patterns_field`, COUNT(CASE WHEN `ENAME` IS NULL OR `ENAME` = '' THEN"
            + " '' ELSE REGEXP_REPLACE(`ENAME`, '[a-zA-Z0-9]+', '<*>') END) `pattern_count`,"
            + " TRY_CAST(PATTERN_PARSER(CASE WHEN `ENAME` IS NULL OR `ENAME` = '' THEN '' ELSE"
            + " REGEXP_REPLACE(`ENAME`, '[a-zA-Z0-9]+', '<*>') END, `TAKE`(`ENAME`, 10))['tokens']"
            + " AS MAP< VARCHAR, VARCHAR ARRAY >) `tokens`, `TAKE`(`ENAME`, 10) `sample_logs`\n"
            + "FROM `scott`.`EMP`\n"
            + "GROUP BY CASE WHEN `ENAME` IS NULL OR `ENAME` = '' THEN '' ELSE"
            + " REGEXP_REPLACE(`ENAME`, '[a-zA-Z0-9]+', '<*>') END";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testPatternsAggregationModeWithGroupBy_ShowNumberedToken_ForSimplePatternMethod() {
    String ppl = "source=EMP | patterns ENAME by DEPTNO mode=aggregation show_numbered_token=true";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(DEPTNO=[$0], patterns_field=[SAFE_CAST(ITEM(PATTERN_PARSER($1, $3),"
            + " 'pattern'))], pattern_count=[$2], tokens=[SAFE_CAST(ITEM(PATTERN_PARSER($1, $3),"
            + " 'tokens'))], sample_logs=[$3])\n"
            + "  LogicalAggregate(group=[{1, 2}], pattern_count=[COUNT($2)], sample_logs=[TAKE($0,"
            + " $3)])\n"
            + "    LogicalProject(ENAME=[$1], DEPTNO=[$7], patterns_field=[CASE(SEARCH($1,"
            + " Sarg['':VARCHAR; NULL AS TRUE]:VARCHAR), '':VARCHAR, REGEXP_REPLACE($1,"
            + " '[a-zA-Z0-9]+':VARCHAR, '<*>':VARCHAR))], $f9=[10])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `DEPTNO`, TRY_CAST(PATTERN_PARSER(CASE WHEN `ENAME` IS NULL OR `ENAME` = '' THEN ''"
            + " ELSE REGEXP_REPLACE(`ENAME`, '[a-zA-Z0-9]+', '<*>') END, `TAKE`(`ENAME`,"
            + " 10))['pattern'] AS STRING) `patterns_field`, COUNT(CASE WHEN `ENAME` IS NULL OR"
            + " `ENAME` = '' THEN '' ELSE REGEXP_REPLACE(`ENAME`, '[a-zA-Z0-9]+', '<*>') END)"
            + " `pattern_count`, TRY_CAST(PATTERN_PARSER(CASE WHEN `ENAME` IS NULL OR `ENAME` = ''"
            + " THEN '' ELSE REGEXP_REPLACE(`ENAME`, '[a-zA-Z0-9]+', '<*>') END, `TAKE`(`ENAME`,"
            + " 10))['tokens'] AS MAP< VARCHAR, VARCHAR ARRAY >) `tokens`, `TAKE`(`ENAME`, 10)"
            + " `sample_logs`\n"
            + "FROM `scott`.`EMP`\n"
            + "GROUP BY `DEPTNO`, CASE WHEN `ENAME` IS NULL OR `ENAME` = '' THEN '' ELSE"
            + " REGEXP_REPLACE(`ENAME`, '[a-zA-Z0-9]+', '<*>') END";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testPatternsAggregationMode_SpecifyAllParameters_ForBrainMethod() {
    String ppl =
        "source=EMP | patterns ENAME method=BRAIN mode=aggregation max_sample_count=2"
            + " buffer_limit=1000 show_numbered_token=false variable_count_threshold=3"
            + " frequency_threshold_percentage=0.1";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(patterns_field=[SAFE_CAST(ITEM($1, 'pattern'))],"
            + " pattern_count=[SAFE_CAST(ITEM($1, 'pattern_count'))],"
            + " sample_logs=[SAFE_CAST(ITEM($1, 'sample_logs'))])\n"
            + "  LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{0}])\n"
            + "    LogicalAggregate(group=[{}], patterns_field=[pattern($0, $1, $2, $3, $4, $5)])\n"
            + "      LogicalProject(ENAME=[$1], $f8=[2], $f9=[1000], $f10=[false],"
            + " $f11=[0.1:DECIMAL(2, 1)], $f12=[3])\n"
            + "        LogicalTableScan(table=[[scott, EMP]])\n"
            + "    Uncollect\n"
            + "      LogicalProject(patterns_field=[$cor0.patterns_field])\n"
            + "        LogicalValues(tuples=[[{ 0 }]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT TRY_CAST(`t20`.`patterns_field`['pattern'] AS STRING) `patterns_field`,"
            + " TRY_CAST(`t20`.`patterns_field`['pattern_count'] AS BIGINT) `pattern_count`,"
            + " TRY_CAST(`t20`.`patterns_field`['sample_logs'] AS ARRAY< STRING >) `sample_logs`\n"
            + "FROM (SELECT `pattern`(`ENAME`, 2, 1000, FALSE, 0.1, 3) `patterns_field`\n"
            + "FROM `scott`.`EMP`) `$cor0`,\n"
            + "LATERAL UNNEST((SELECT `$cor0`.`patterns_field`\n"
            + "FROM (VALUES (0)) `t` (`ZERO`))) `t20` (`patterns_field`)";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testPatternsAggregationMode_NotShowNumberedToken_ForBrainMethod() {
    String ppl = "source=EMP | patterns ENAME method=BRAIN mode=aggregation";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(patterns_field=[SAFE_CAST(ITEM($1, 'pattern'))],"
            + " pattern_count=[SAFE_CAST(ITEM($1, 'pattern_count'))],"
            + " sample_logs=[SAFE_CAST(ITEM($1, 'sample_logs'))])\n"
            + "  LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{0}])\n"
            + "    LogicalAggregate(group=[{}], patterns_field=[pattern($0, $1, $2, $3)])\n"
            + "      LogicalProject(ENAME=[$1], $f8=[10], $f9=[100000], $f10=[false])\n"
            + "        LogicalTableScan(table=[[scott, EMP]])\n"
            + "    Uncollect\n"
            + "      LogicalProject(patterns_field=[$cor0.patterns_field])\n"
            + "        LogicalValues(tuples=[[{ 0 }]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT TRY_CAST(`t20`.`patterns_field`['pattern'] AS STRING) `patterns_field`,"
            + " TRY_CAST(`t20`.`patterns_field`['pattern_count'] AS BIGINT) `pattern_count`,"
            + " TRY_CAST(`t20`.`patterns_field`['sample_logs'] AS ARRAY< STRING >) `sample_logs`\n"
            + "FROM (SELECT `pattern`(`ENAME`, 10, 100000, FALSE) `patterns_field`\n"
            + "FROM `scott`.`EMP`) `$cor0`,\n"
            + "LATERAL UNNEST((SELECT `$cor0`.`patterns_field`\n"
            + "FROM (VALUES (0)) `t` (`ZERO`))) `t20` (`patterns_field`)";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testPatternsAggregationMode_ShowNumberedToken_ForBrainMethod() {
    String ppl =
        "source=EMP | patterns ENAME method=BRAIN mode=aggregation show_numbered_token=true";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(patterns_field=[SAFE_CAST(ITEM(PATTERN_PARSER(SAFE_CAST(ITEM($1,"
            + " 'pattern')), ITEM($1, 'sample_logs'), true), 'pattern'))],"
            + " pattern_count=[SAFE_CAST(ITEM($1, 'pattern_count'))],"
            + " tokens=[SAFE_CAST(ITEM(PATTERN_PARSER(SAFE_CAST(ITEM($1, 'pattern')), ITEM($1,"
            + " 'sample_logs'), true), 'tokens'))], sample_logs=[SAFE_CAST(ITEM($1,"
            + " 'sample_logs'))])\n"
            + "  LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{0}])\n"
            + "    LogicalAggregate(group=[{}], patterns_field=[pattern($0, $1, $2, $3)])\n"
            + "      LogicalProject(ENAME=[$1], $f8=[10], $f9=[100000], $f10=[true])\n"
            + "        LogicalTableScan(table=[[scott, EMP]])\n"
            + "    Uncollect\n"
            + "      LogicalProject(patterns_field=[$cor0.patterns_field])\n"
            + "        LogicalValues(tuples=[[{ 0 }]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT TRY_CAST(PATTERN_PARSER(TRY_CAST(`t20`.`patterns_field`['pattern'] AS STRING),"
            + " `t20`.`patterns_field`['sample_logs'], TRUE)['pattern'] AS STRING)"
            + " `patterns_field`, TRY_CAST(`t20`.`patterns_field`['pattern_count'] AS BIGINT)"
            + " `pattern_count`, TRY_CAST(PATTERN_PARSER(TRY_CAST(`t20`.`patterns_field`['pattern']"
            + " AS STRING), `t20`.`patterns_field`['sample_logs'], TRUE)['tokens'] AS MAP< VARCHAR,"
            + " VARCHAR ARRAY >) `tokens`, TRY_CAST(`t20`.`patterns_field`['sample_logs'] AS ARRAY<"
            + " STRING >) `sample_logs`\n"
            + "FROM (SELECT `pattern`(`ENAME`, 10, 100000, TRUE) `patterns_field`\n"
            + "FROM `scott`.`EMP`) `$cor0`,\n"
            + "LATERAL UNNEST((SELECT `$cor0`.`patterns_field`\n"
            + "FROM (VALUES (0)) `t` (`ZERO`))) `t20` (`patterns_field`)";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testPatternsAggregationModeWithGroupBy_NotShowNumberedToken_ForBrainMethod() {
    String ppl = "source=EMP | patterns ENAME by DEPTNO method=BRAIN mode=aggregation";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(DEPTNO=[$0], patterns_field=[SAFE_CAST(ITEM($2, 'pattern'))],"
            + " pattern_count=[SAFE_CAST(ITEM($2, 'pattern_count'))],"
            + " sample_logs=[SAFE_CAST(ITEM($2, 'sample_logs'))])\n"
            + "  LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{1}])\n"
            + "    LogicalAggregate(group=[{1}], patterns_field=[pattern($0, $2, $3, $4)])\n"
            + "      LogicalProject(ENAME=[$1], DEPTNO=[$7], $f8=[10], $f9=[100000],"
            + " $f10=[false])\n"
            + "        LogicalTableScan(table=[[scott, EMP]])\n"
            + "    Uncollect\n"
            + "      LogicalProject(patterns_field=[$cor0.patterns_field])\n"
            + "        LogicalValues(tuples=[[{ 0 }]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `$cor0`.`DEPTNO`, TRY_CAST(`t20`.`patterns_field`['pattern'] AS STRING)"
            + " `patterns_field`, TRY_CAST(`t20`.`patterns_field`['pattern_count'] AS BIGINT)"
            + " `pattern_count`, TRY_CAST(`t20`.`patterns_field`['sample_logs'] AS ARRAY< STRING"
            + " >) `sample_logs`\n"
            + "FROM (SELECT `DEPTNO`, `pattern`(`ENAME`, 10, 100000, FALSE) `patterns_field`\n"
            + "FROM `scott`.`EMP`\n"
            + "GROUP BY `DEPTNO`) `$cor0`,\n"
            + "LATERAL UNNEST((SELECT `$cor0`.`patterns_field`\n"
            + "FROM (VALUES (0)) `t` (`ZERO`))) `t20` (`patterns_field`)";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testPatternsAggregationModeWithGroupBy_ShowNumberedToken_ForBrainMethod() {
    String ppl =
        "source=EMP | patterns ENAME by DEPTNO method=BRAIN mode=aggregation"
            + " show_numbered_token=true";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(DEPTNO=[$0],"
            + " patterns_field=[SAFE_CAST(ITEM(PATTERN_PARSER(SAFE_CAST(ITEM($2, 'pattern')),"
            + " ITEM($2, 'sample_logs'), true), 'pattern'))], pattern_count=[SAFE_CAST(ITEM($2,"
            + " 'pattern_count'))], tokens=[SAFE_CAST(ITEM(PATTERN_PARSER(SAFE_CAST(ITEM($2,"
            + " 'pattern')), ITEM($2, 'sample_logs'), true), 'tokens'))],"
            + " sample_logs=[SAFE_CAST(ITEM($2, 'sample_logs'))])\n"
            + "  LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{1}])\n"
            + "    LogicalAggregate(group=[{1}], patterns_field=[pattern($0, $2, $3, $4)])\n"
            + "      LogicalProject(ENAME=[$1], DEPTNO=[$7], $f8=[10], $f9=[100000], $f10=[true])\n"
            + "        LogicalTableScan(table=[[scott, EMP]])\n"
            + "    Uncollect\n"
            + "      LogicalProject(patterns_field=[$cor0.patterns_field])\n"
            + "        LogicalValues(tuples=[[{ 0 }]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `$cor0`.`DEPTNO`,"
            + " TRY_CAST(PATTERN_PARSER(TRY_CAST(`t20`.`patterns_field`['pattern'] AS STRING),"
            + " `t20`.`patterns_field`['sample_logs'], TRUE)['pattern'] AS STRING)"
            + " `patterns_field`, TRY_CAST(`t20`.`patterns_field`['pattern_count'] AS BIGINT)"
            + " `pattern_count`, TRY_CAST(PATTERN_PARSER(TRY_CAST(`t20`.`patterns_field`['pattern']"
            + " AS STRING), `t20`.`patterns_field`['sample_logs'], TRUE)['tokens'] AS MAP< VARCHAR,"
            + " VARCHAR ARRAY >) `tokens`, TRY_CAST(`t20`.`patterns_field`['sample_logs'] AS ARRAY<"
            + " STRING >) `sample_logs`\n"
            + "FROM (SELECT `DEPTNO`, `pattern`(`ENAME`, 10, 100000, TRUE) `patterns_field`\n"
            + "FROM `scott`.`EMP`\n"
            + "GROUP BY `DEPTNO`) `$cor0`,\n"
            + "LATERAL UNNEST((SELECT `$cor0`.`patterns_field`\n"
            + "FROM (VALUES (0)) `t` (`ZERO`))) `t20` (`patterns_field`)";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }
}
