/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.test.CalciteAssert;
import org.junit.Test;

public class CalcitePPLRegexTest extends CalcitePPLAbstractTest {
  public CalcitePPLRegexTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Test
  public void testRegexBasic() {
    String ppl = "source=EMP | regex ENAME='A.*' | fields ENAME, JOB";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(ENAME=[$1], JOB=[$2])\n"
            + "  LogicalFilter(condition=[REGEXP_CONTAINS($1, 'A.*':VARCHAR)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `ENAME`, `JOB`\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE REGEXP_CONTAINS(`ENAME`, 'A.*')";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testRegexChainedFilters() {
    String ppl = "source=EMP | regex ENAME='A.*' | regex JOB='.*CLERK' | fields ENAME, JOB";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(ENAME=[$1], JOB=[$2])\n"
            + "  LogicalFilter(condition=[REGEXP_CONTAINS($2, '.*CLERK':VARCHAR)])\n"
            + "    LogicalFilter(condition=[REGEXP_CONTAINS($1, 'A.*':VARCHAR)])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `ENAME`, `JOB`\n"
            + "FROM (SELECT *\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE REGEXP_CONTAINS(`ENAME`, 'A.*')) `t`\n"
            + "WHERE REGEXP_CONTAINS(`JOB`, '.*CLERK')";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testRegexWithNotEqual() {
    String ppl = "source=EMP | regex ENAME!='A.*' | fields ENAME, JOB";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(ENAME=[$1], JOB=[$2])\n"
            + "  LogicalFilter(condition=[NOT(REGEXP_CONTAINS($1, 'A.*':VARCHAR))])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `ENAME`, `JOB`\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE NOT REGEXP_CONTAINS(`ENAME`, 'A.*')";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testRegexComplexPattern() {
    String ppl = "source=EMP | regex ENAME='[A-Z]{2,}' | fields ENAME";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(ENAME=[$1])\n"
            + "  LogicalFilter(condition=[REGEXP_CONTAINS($1, '[A-Z]{2,}':VARCHAR)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `ENAME`\n" + "FROM `scott`.`EMP`\n" + "WHERE REGEXP_CONTAINS(`ENAME`, '[A-Z]{2,}')";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testRegexWithEscapedCharacters() {
    String ppl = "source=EMP | regex JOB='SALES\\sMAN' | fields JOB";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(JOB=[$2])\n"
            + "  LogicalFilter(condition=[REGEXP_CONTAINS($2, 'SALES\\sMAN':VARCHAR)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `JOB`\n" + "FROM `scott`.`EMP`\n" + "WHERE REGEXP_CONTAINS(`JOB`, 'SALES\\sMAN')";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testRegexChainedCommands() {
    String ppl = "source=EMP | regex ENAME='A.*' | fields ENAME | sort ENAME | head 5";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalSort(sort0=[$0], dir0=[ASC-nulls-first], fetch=[5])\n"
            + "  LogicalProject(ENAME=[$1])\n"
            + "    LogicalFilter(condition=[REGEXP_CONTAINS($1, 'A.*':VARCHAR)])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `ENAME`\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE REGEXP_CONTAINS(`ENAME`, 'A.*')\n"
            + "ORDER BY `ENAME`\n"
            + "LIMIT 5";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testRegexWithAggregation() {
    String ppl = "source=EMP | regex JOB='.*CLERK' | stats count() by JOB";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(count()=[$1], JOB=[$0])\n"
            + "  LogicalAggregate(group=[{0}], count()=[COUNT()])\n"
            + "    LogicalProject(JOB=[$2])\n"
            + "      LogicalFilter(condition=[REGEXP_CONTAINS($2, '.*CLERK':VARCHAR)])\n"
            + "        LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT COUNT(*) `count()`, `JOB`\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE REGEXP_CONTAINS(`JOB`, '.*CLERK')\n"
            + "GROUP BY `JOB`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testRegexCaseInsensitive() {
    String ppl = "source=EMP | regex ENAME='(?i)smith' | fields ENAME";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(ENAME=[$1])\n"
            + "  LogicalFilter(condition=[REGEXP_CONTAINS($1, '(?i)smith':VARCHAR)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `ENAME`\n" + "FROM `scott`.`EMP`\n" + "WHERE REGEXP_CONTAINS(`ENAME`, '(?i)smith')";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testRegexWithNumericPattern() {
    String ppl = "source=EMP | regex SAL='[0-9]{4,}' | fields ENAME, SAL";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(ENAME=[$1], SAL=[$5])\n"
            + "  LogicalFilter(condition=[REGEXP_CONTAINS($5, '[0-9]{4,}':VARCHAR)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `ENAME`, `SAL`\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE REGEXP_CONTAINS(`SAL`, '[0-9]{4,}')";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }
}
