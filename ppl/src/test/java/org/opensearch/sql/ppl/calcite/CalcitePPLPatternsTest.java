/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import static org.mockito.Mockito.doReturn;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.test.CalciteAssert.SchemaSpec;
import org.junit.Test;
import org.opensearch.sql.common.setting.Settings.Key;

public class CalcitePPLPatternsTest extends CalcitePPLAbstractTest {
  public CalcitePPLPatternsTest() {
    super(SchemaSpec.SCOTT_WITH_TEMPORAL);
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
}
