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
  }

  @Test
  public void testPatternsLabelModeForSimplePatternMethod() {
    String ppl = "source=EMP | patterns ENAME | fields ENAME, patterns_field, tokens";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(ENAME=[$1],"
            + " patterns_field=[SAFE_CAST(ITEM(PATTERN_PARSER(REGEXP_REPLACE($1,"
            + " '[a-zA-Z0-9]+':VARCHAR, '<*>'), $1), 'pattern'))],"
            + " tokens=[SAFE_CAST(ITEM(PATTERN_PARSER(REGEXP_REPLACE($1, '[a-zA-Z0-9]+':VARCHAR,"
            + " '<*>'), $1), 'tokens'))])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `ENAME`, SAFE_CAST(`PATTERN_PARSER`(REGEXP_REPLACE(`ENAME`, '[a-zA-Z0-9]+', '<*>'),"
            + " `ENAME`)['pattern'] AS STRING) `patterns_field`,"
            + " SAFE_CAST(`PATTERN_PARSER`(REGEXP_REPLACE(`ENAME`, '[a-zA-Z0-9]+', '<*>'),"
            + " `ENAME`)['tokens'] AS MAP< VARCHAR, VARCHAR ARRAY >) `tokens`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testPatternsLabelModeWithCustomPatternForSimplePatternMethod() {
    String ppl =
        "source=EMP | patterns ENAME pattern='[A-H]' | fields ENAME, patterns_field, tokens";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(ENAME=[$1],"
            + " patterns_field=[SAFE_CAST(ITEM(PATTERN_PARSER(REGEXP_REPLACE($1, '[A-H]':VARCHAR,"
            + " '<*>'), $1), 'pattern'))], tokens=[SAFE_CAST(ITEM(PATTERN_PARSER(REGEXP_REPLACE($1,"
            + " '[A-H]':VARCHAR, '<*>'), $1), 'tokens'))])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `ENAME`, SAFE_CAST(`PATTERN_PARSER`(REGEXP_REPLACE(`ENAME`, '[A-H]', '<*>'),"
            + " `ENAME`)['pattern'] AS STRING) `patterns_field`,"
            + " SAFE_CAST(`PATTERN_PARSER`(REGEXP_REPLACE(`ENAME`, '[A-H]', '<*>'),"
            + " `ENAME`)['tokens'] AS MAP< VARCHAR, VARCHAR ARRAY >) `tokens`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testPatternsLabelModeWithPartitionBySimplePatternMethod() {
    String ppl =
        "source=EMP | patterns ENAME by DEPTNO | fields ENAME, DEPTNO, patterns_field, tokens";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(ENAME=[$1], DEPTNO=[$7],"
            + " patterns_field=[SAFE_CAST(ITEM(PATTERN_PARSER(REGEXP_REPLACE($1,"
            + " '[a-zA-Z0-9]+':VARCHAR, '<*>'), $1), 'pattern'))],"
            + " tokens=[SAFE_CAST(ITEM(PATTERN_PARSER(REGEXP_REPLACE($1, '[a-zA-Z0-9]+':VARCHAR,"
            + " '<*>'), $1), 'tokens'))])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `ENAME`, `DEPTNO`, SAFE_CAST(`PATTERN_PARSER`(REGEXP_REPLACE(`ENAME`,"
            + " '[a-zA-Z0-9]+', '<*>'), `ENAME`)['pattern'] AS STRING) `patterns_field`,"
            + " SAFE_CAST(`PATTERN_PARSER`(REGEXP_REPLACE(`ENAME`, '[a-zA-Z0-9]+', '<*>'),"
            + " `ENAME`)['tokens'] AS MAP< VARCHAR, VARCHAR ARRAY >) `tokens`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testPatternsLabelModeForBrainMethod() {
    String ppl = "source=EMP | patterns ENAME method=BRAIN | fields ENAME, patterns_field, tokens";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(ENAME=[$1], patterns_field=[SAFE_CAST(ITEM(PATTERN_PARSER($1, pattern($1,"
            + " 10, 100000) OVER ()), 'pattern'))], tokens=[SAFE_CAST(ITEM(PATTERN_PARSER($1,"
            + " pattern($1, 10, 100000) OVER ()), 'tokens'))])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `ENAME`, SAFE_CAST(`PATTERN_PARSER`(`ENAME`, `pattern`(`ENAME`, 10, 100000) OVER"
            + " (RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))['pattern'] AS STRING)"
            + " `patterns_field`, SAFE_CAST(`PATTERN_PARSER`(`ENAME`, `pattern`(`ENAME`, 10,"
            + " 100000) OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))['tokens']"
            + " AS MAP< VARCHAR, VARCHAR ARRAY >) `tokens`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testPatternsLabelModeWithPartitionByForBrainMethod() {
    String ppl =
        "source=EMP | patterns ENAME by DEPTNO method=BRAIN | fields ENAME, DEPTNO,"
            + " patterns_field, tokens";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(ENAME=[$1], DEPTNO=[$7], patterns_field=[SAFE_CAST(ITEM(PATTERN_PARSER($1,"
            + " pattern($1, 10, 100000) OVER (PARTITION BY $7)), 'pattern'))],"
            + " tokens=[SAFE_CAST(ITEM(PATTERN_PARSER($1, pattern($1, 10, 100000) OVER (PARTITION"
            + " BY $7)), 'tokens'))])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `ENAME`, `DEPTNO`, SAFE_CAST(`PATTERN_PARSER`(`ENAME`, `pattern`(`ENAME`, 10,"
            + " 100000) OVER (PARTITION BY `DEPTNO` RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED"
            + " FOLLOWING))['pattern'] AS STRING) `patterns_field`,"
            + " SAFE_CAST(`PATTERN_PARSER`(`ENAME`, `pattern`(`ENAME`, 10, 100000) OVER (PARTITION"
            + " BY `DEPTNO` RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))['tokens']"
            + " AS MAP< VARCHAR, VARCHAR ARRAY >) `tokens`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testPatternsAggregationModeForSimplePatternMethod() {
    String ppl = "source=EMP | patterns ENAME mode=aggregation";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(pattern_count=[$1], patterns_field=[SAFE_CAST(ITEM(PATTERN_PARSER($0, $2),"
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
            + " `TAKE`(`ENAME`, 10))['pattern'] AS STRING) `patterns_field`,"
            + " SAFE_CAST(`PATTERN_PARSER`(REGEXP_REPLACE(`ENAME`, '[a-zA-Z0-9]+', '<*>'),"
            + " `TAKE`(`ENAME`, 10))['tokens'] AS MAP< VARCHAR, VARCHAR ARRAY >) `tokens`\n"
            + "FROM `scott`.`EMP`\n"
            + "GROUP BY REGEXP_REPLACE(`ENAME`, '[a-zA-Z0-9]+', '<*>')";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testPatternsAggregationModeWithGroupByForSimplePatternMethod() {
    String ppl = "source=EMP | patterns ENAME by DEPTNO mode=aggregation";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(DEPTNO=[$0], pattern_count=[$2],"
            + " patterns_field=[SAFE_CAST(ITEM(PATTERN_PARSER($1, $3), 'pattern'))],"
            + " tokens=[SAFE_CAST(ITEM(PATTERN_PARSER($1, $3), 'tokens'))])\n"
            + "  LogicalAggregate(group=[{1, 2}], pattern_count=[COUNT($2)], sample_logs=[TAKE($0,"
            + " $3)])\n"
            + "    LogicalProject(ENAME=[$1], DEPTNO=[$7], patterns_field=[REGEXP_REPLACE($1,"
            + " '[a-zA-Z0-9]+':VARCHAR, '<*>')], $f9=[10])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `DEPTNO`, COUNT(REGEXP_REPLACE(`ENAME`, '[a-zA-Z0-9]+', '<*>')) `pattern_count`,"
            + " SAFE_CAST(`PATTERN_PARSER`(REGEXP_REPLACE(`ENAME`, '[a-zA-Z0-9]+', '<*>'),"
            + " `TAKE`(`ENAME`, 10))['pattern'] AS STRING) `patterns_field`,"
            + " SAFE_CAST(`PATTERN_PARSER`(REGEXP_REPLACE(`ENAME`, '[a-zA-Z0-9]+', '<*>'),"
            + " `TAKE`(`ENAME`, 10))['tokens'] AS MAP< VARCHAR, VARCHAR ARRAY >) `tokens`\n"
            + "FROM `scott`.`EMP`\n"
            + "GROUP BY `DEPTNO`, REGEXP_REPLACE(`ENAME`, '[a-zA-Z0-9]+', '<*>')";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testPatternsAggregationModeForBrainMethod() {
    String ppl = "source=EMP | patterns ENAME method=BRAIN mode=aggregation";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(patterns_field=[SAFE_CAST(ITEM($1, 'pattern'))],"
            + " pattern_count=[SAFE_CAST(ITEM($1, 'pattern_count'))], tokens=[SAFE_CAST(ITEM($1,"
            + " 'tokens'))])\n"
            + "  LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{0}])\n"
            + "    LogicalAggregate(group=[{}], patterns_field=[pattern($0, $1, $2)])\n"
            + "      LogicalProject(ENAME=[$1], $f8=[10], $f9=[100000])\n"
            + "        LogicalTableScan(table=[[scott, EMP]])\n"
            + "    Uncollect\n"
            + "      LogicalProject(patterns_field=[$cor0.patterns_field])\n"
            + "        LogicalValues(tuples=[[{ 0 }]])\n";
    verifyLogical(root, expectedLogical);

    /*
     * TODO: Fix Spark SQL conformance
     * Spark doesn't have SAFE_CAST and UNNEST
     */
    String expectedSparkSql =
        "SELECT SAFE_CAST(`t20`.`patterns_field`['pattern'] AS STRING) `patterns_field`,"
            + " SAFE_CAST(`t20`.`patterns_field`['pattern_count'] AS BIGINT) `pattern_count`,"
            + " SAFE_CAST(`t20`.`patterns_field`['tokens'] AS MAP< VARCHAR, VARCHAR ARRAY >)"
            + " `tokens`\n"
            + "FROM (SELECT `pattern`(`ENAME`, 10, 100000) `patterns_field`\n"
            + "FROM `scott`.`EMP`) `$cor0`,\n"
            + "LATERAL UNNEST (SELECT `$cor0`.`patterns_field`\n"
            + "FROM (VALUES (0)) `t` (`ZERO`)) `t2` (`patterns_field`) `t20`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testPatternsAggregationModeWithGroupByForBrainMethod() {
    String ppl = "source=EMP | patterns ENAME by DEPTNO method=BRAIN mode=aggregation";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(DEPTNO=[$0], patterns_field=[SAFE_CAST(ITEM($2, 'pattern'))],"
            + " pattern_count=[SAFE_CAST(ITEM($2, 'pattern_count'))], tokens=[SAFE_CAST(ITEM($2,"
            + " 'tokens'))])\n"
            + "  LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{1}])\n"
            + "    LogicalAggregate(group=[{1}], patterns_field=[pattern($0, $2, $3)])\n"
            + "      LogicalProject(ENAME=[$1], DEPTNO=[$7], $f8=[10], $f9=[100000])\n"
            + "        LogicalTableScan(table=[[scott, EMP]])\n"
            + "    Uncollect\n"
            + "      LogicalProject(patterns_field=[$cor0.patterns_field])\n"
            + "        LogicalValues(tuples=[[{ 0 }]])\n";
    verifyLogical(root, expectedLogical);

    /*
     * TODO: Fix Spark SQL conformance
     * Spark doesn't have SAFE_CAST and UNNEST
     */
    String expectedSparkSql =
        "SELECT `$cor0`.`DEPTNO`, SAFE_CAST(`t20`.`patterns_field`['pattern'] AS STRING)"
            + " `patterns_field`, SAFE_CAST(`t20`.`patterns_field`['pattern_count'] AS BIGINT)"
            + " `pattern_count`, SAFE_CAST(`t20`.`patterns_field`['tokens'] AS MAP< VARCHAR,"
            + " VARCHAR ARRAY >) `tokens`\n"
            + "FROM (SELECT `DEPTNO`, `pattern`(`ENAME`, 10, 100000) `patterns_field`\n"
            + "FROM `scott`.`EMP`\n"
            + "GROUP BY `DEPTNO`) `$cor0`,\n"
            + "LATERAL UNNEST (SELECT `$cor0`.`patterns_field`\n"
            + "FROM (VALUES (0)) `t` (`ZERO`)) `t2` (`patterns_field`) `t20`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }
}
