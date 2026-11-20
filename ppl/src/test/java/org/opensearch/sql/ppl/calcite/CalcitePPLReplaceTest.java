/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;

import java.util.List;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Programs;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.Test;
import org.opensearch.sql.common.antlr.SyntaxCheckException;
import org.opensearch.sql.common.error.ErrorReport;

public class CalcitePPLReplaceTest extends CalcitePPLAbstractTest {

  private static final int WIDE_SUBFIELDS = 100_000;

  public CalcitePPLReplaceTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Override
  protected Frameworks.ConfigBuilder config(CalciteAssert.SchemaSpec... schemaSpecs) {
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    final SchemaPlus schema = CalciteAssert.addSchema(rootSchema, schemaSpecs);
    schema.add("WIDE", new CalcitePPLWideNestedSchemaTest.WideNestedTable(WIDE_SUBFIELDS));
    schema.add("REPEATED", new RepeatedNamesTable());
    return Frameworks.newConfigBuilder()
        .parserConfig(SqlParser.Config.DEFAULT)
        .defaultSchema(schema)
        .traitDefs((List<RelTraitDef>) null)
        .programs(Programs.heuristicJoinOrder(Programs.RULE_SET, true, 2));
  }

  /**
   * Resolving every column through {@code RelBuilder.field(String)}, which searches the field names
   * linearly on each call, made replace O(columns^2); this query took about 40s. The timeout
   * catches a regression.
   */
  @Test(timeout = 10_000)
  public void testReplaceOnWideSchemaIsLinear() {
    RelNode root = getRelNode("source=WIDE | replace \"a\" WITH \"b\" IN NAME");
    String expectedLogical =
        "LogicalProject(ID=[$0], ATTRS=[$1], LONE.X=[$3], NAME=[REPLACE($"
            + (WIDE_SUBFIELDS + 5)
            + ", 'a', 'b')])\n"
            + "  LogicalTableScan(table=[[scott, WIDE]])\n";
    verifyLogical(root, expectedLogical);
  }

  /**
   * Columns left unchanged resolve the way {@code RelBuilder.field(String)} does: names match
   * case-sensitively, and a repeated name resolves to its first column.
   */
  @Test
  public void testReplaceResolvesColumnsLikeFieldByName() {
    RelNode root = getRelNode("source=REPEATED | replace \"x\" WITH \"y\" IN B");
    String expectedLogical =
        "LogicalProject(A=[$0], a=[$1], B=[REPLACE($2, 'x', 'y')], A0=[$0])\n"
            + "  LogicalTableScan(table=[[scott, REPEATED]])\n";
    verifyLogical(root, expectedLogical);
  }

  @Test
  public void testBasicReplace() {
    String ppl = "source=EMP | replace \"CLERK\" WITH \"EMPLOYEE\" IN JOB";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[REPLACE($2, 'CLERK':VARCHAR,"
            + " 'EMPLOYEE':VARCHAR)], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";

    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`, REPLACE(`JOB`, 'CLERK', 'EMPLOYEE') `JOB`, `MGR`, `HIREDATE`,"
            + " `SAL`, `COMM`, `DEPTNO`\n"
            + "FROM `scott`.`EMP`";

    verifyPPLToSparkSQL(root, expectedSparkSql);

    String expectedResult =
        "EMPNO=7369; ENAME=SMITH; JOB=EMPLOYEE; MGR=7902; HIREDATE=1980-12-17; SAL=800.00;"
            + " COMM=null; DEPTNO=20\n"
            + "EMPNO=7499; ENAME=ALLEN; JOB=SALESMAN; MGR=7698; HIREDATE=1981-02-20; SAL=1600.00;"
            + " COMM=300.00; DEPTNO=30\n"
            + "EMPNO=7521; ENAME=WARD; JOB=SALESMAN; MGR=7698; HIREDATE=1981-02-22; SAL=1250.00;"
            + " COMM=500.00; DEPTNO=30\n"
            + "EMPNO=7566; ENAME=JONES; JOB=MANAGER; MGR=7839; HIREDATE=1981-02-04; SAL=2975.00;"
            + " COMM=null; DEPTNO=20\n"
            + "EMPNO=7654; ENAME=MARTIN; JOB=SALESMAN; MGR=7698; HIREDATE=1981-09-28; SAL=1250.00;"
            + " COMM=1400.00; DEPTNO=30\n"
            + "EMPNO=7698; ENAME=BLAKE; JOB=MANAGER; MGR=7839; HIREDATE=1981-01-05; SAL=2850.00;"
            + " COMM=null; DEPTNO=30\n"
            + "EMPNO=7782; ENAME=CLARK; JOB=MANAGER; MGR=7839; HIREDATE=1981-06-09; SAL=2450.00;"
            + " COMM=null; DEPTNO=10\n"
            + "EMPNO=7788; ENAME=SCOTT; JOB=ANALYST; MGR=7566; HIREDATE=1987-04-19; SAL=3000.00;"
            + " COMM=null; DEPTNO=20\n"
            + "EMPNO=7839; ENAME=KING; JOB=PRESIDENT; MGR=null; HIREDATE=1981-11-17; SAL=5000.00;"
            + " COMM=null; DEPTNO=10\n"
            + "EMPNO=7844; ENAME=TURNER; JOB=SALESMAN; MGR=7698; HIREDATE=1981-09-08; SAL=1500.00;"
            + " COMM=0.00; DEPTNO=30\n"
            + "EMPNO=7876; ENAME=ADAMS; JOB=EMPLOYEE; MGR=7788; HIREDATE=1987-05-23; SAL=1100.00;"
            + " COMM=null; DEPTNO=20\n"
            + "EMPNO=7900; ENAME=JAMES; JOB=EMPLOYEE; MGR=7698; HIREDATE=1981-12-03; SAL=950.00;"
            + " COMM=null; DEPTNO=30\n"
            + "EMPNO=7902; ENAME=FORD; JOB=ANALYST; MGR=7566; HIREDATE=1981-12-03; SAL=3000.00;"
            + " COMM=null; DEPTNO=20\n"
            + "EMPNO=7934; ENAME=MILLER; JOB=EMPLOYEE; MGR=7782; HIREDATE=1982-01-23; SAL=1300.00;"
            + " COMM=null; DEPTNO=10\n";

    verifyResult(root, expectedResult);
  }

  @Test
  public void testMultipleFieldsReplace() {
    String ppl =
        "source=EMP | replace \"CLERK\" WITH \"EMPLOYEE\" IN JOB | replace \"SMITH\" WITH \"SMYTH\""
            + " IN ENAME";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[REPLACE($1, 'SMITH':VARCHAR, 'SMYTH':VARCHAR)],"
            + " JOB=[REPLACE($2, 'CLERK':VARCHAR, 'EMPLOYEE':VARCHAR)], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";

    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, REPLACE(`ENAME`, 'SMITH', 'SMYTH') `ENAME`, REPLACE(`JOB`, 'CLERK',"
            + " 'EMPLOYEE') `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`\n"
            + "FROM `scott`.`EMP`";

    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testReplaceSameValueInMultipleFields() {
    // "ER" occurs in both fields, in JOB=MANAGER and in ENAME=TURNER and MILLER
    String ppl = "source=EMP | replace \"ER\" WITH \"OR\" IN JOB, ENAME";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[REPLACE($1, 'ER':VARCHAR, 'OR':VARCHAR)],"
            + " JOB=[REPLACE($2, 'ER':VARCHAR, 'OR':VARCHAR)], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";

    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, REPLACE(`ENAME`, 'ER', 'OR') `ENAME`, REPLACE(`JOB`, 'ER', 'OR') `JOB`,"
            + " `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`\n"
            + "FROM `scott`.`EMP`";

    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testReplaceNamesFirstNonStringField() {
    // JOB is a string, DEPTNO and MGR are numeric, so the error names DEPTNO
    String ppl = "source=EMP | replace \"CLERK\" WITH \"EMPLOYEE\" IN JOB, DEPTNO, MGR";
    ErrorReport e = assertThrows(ErrorReport.class, () -> getRelNode(ppl));
    assertThat(e.getMessage(), is("replace requires a string field, but [DEPTNO] is TINYINT"));
  }

  @Test
  public void testReplaceWithPipeline() {
    String ppl =
        "source=EMP | where JOB = 'CLERK' | replace \"CLERK\" WITH \"EMPLOYEE\" IN JOB | sort SAL";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalSort(sort0=[$5], dir0=[ASC-nulls-first])\n"
            + "  LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[REPLACE($2, 'CLERK':VARCHAR,"
            + " 'EMPLOYEE':VARCHAR)], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7])\n"
            + "    LogicalFilter(condition=[=($2, 'CLERK':VARCHAR)])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";

    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`, REPLACE(`JOB`, 'CLERK', 'EMPLOYEE') `JOB`, `MGR`, `HIREDATE`,"
            + " `SAL`, `COMM`, `DEPTNO`\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE `JOB` = 'CLERK'\n"
            + "ORDER BY `SAL`";

    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test(expected = SyntaxCheckException.class)
  public void testReplaceWithoutWithKeywordShouldFail() {
    String ppl = "source=EMP | replace \"CLERK\" \"EMPLOYEE\" IN JOB";
    getRelNode(ppl);
  }

  @Test(expected = SyntaxCheckException.class)
  public void testReplaceWithoutInKeywordShouldFail() {
    String ppl = "source=EMP | replace \"CLERK\" WITH \"EMPLOYEE\" JOB";
    getRelNode(ppl);
  }

  @Test(expected = SyntaxCheckException.class)
  public void testReplaceWithExpressionShouldFail() {
    String ppl = "source=EMP | replace EMPNO + 1 WITH \"EMPLOYEE\" IN JOB";
    getRelNode(ppl);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testReplaceWithInvalidFieldShouldFail() {
    String ppl = "source=EMP | replace \"CLERK\" WITH \"EMPLOYEE\" IN INVALID_FIELD";
    getRelNode(ppl);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testReplaceWithMultipleInKeywordsShouldFail() {
    String ppl = "source=EMP | replace \"CLERK\" WITH \"EMPLOYEE\" IN JOB IN ENAME";
    getRelNode(ppl);
  }

  @Test(expected = SyntaxCheckException.class)
  public void testReplaceWithMissingQuotesShouldFail() {
    String ppl = "source=EMP | replace CLERK WITH EMPLOYEE IN JOB";
    getRelNode(ppl);
  }

  @Test(expected = SyntaxCheckException.class)
  public void testReplaceWithMissingReplacementValueShouldFail() {
    String ppl = "source=EMP | replace \"CLERK\" WITH IN JOB";
    getRelNode(ppl);
  }

  @Test
  public void testReplaceWithEvalAndReplaceOnSameField() {
    // Test verifies that in-place replacement works correctly when there are additional fields
    // created by eval. The eval creates new_JOB, and replace modifies JOB in-place.
    String ppl =
        "source=EMP | eval new_JOB = 'existing' | replace \"CLERK\" WITH \"EMPLOYEE\" IN JOB";
    RelNode root = getRelNode(ppl);

    // With in-place replacement, JOB is modified and new_JOB remains as created by eval
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[REPLACE($2, 'CLERK':VARCHAR,"
            + " 'EMPLOYEE':VARCHAR)], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7], new_JOB=['existing':VARCHAR])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";

    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`, REPLACE(`JOB`, 'CLERK', 'EMPLOYEE') `JOB`, `MGR`, `HIREDATE`,"
            + " `SAL`, `COMM`, `DEPTNO`, 'existing' `new_JOB`\n"
            + "FROM `scott`.`EMP`";

    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testReplaceWithMultiplePairs() {
    // Test with multiple pattern/replacement pairs in a single command
    String ppl =
        "source=EMP | replace \"CLERK\" WITH \"EMPLOYEE\", \"MANAGER\" WITH \"SUPERVISOR\" IN JOB";
    RelNode root = getRelNode(ppl);

    // Should generate nested REPLACE calls: REPLACE(REPLACE(JOB, 'CLERK', 'EMPLOYEE'), 'MANAGER',
    // 'SUPERVISOR')
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[REPLACE(REPLACE($2, 'CLERK':VARCHAR,"
            + " 'EMPLOYEE':VARCHAR), 'MANAGER':VARCHAR, 'SUPERVISOR':VARCHAR)], MGR=[$3],"
            + " HIREDATE=[$4], SAL=[$5], COMM=[$6], DEPTNO=[$7])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";

    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`, REPLACE(REPLACE(`JOB`, 'CLERK', 'EMPLOYEE'), 'MANAGER',"
            + " 'SUPERVISOR') `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`\n"
            + "FROM `scott`.`EMP`";

    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testReplaceWithThreePairs() {
    // Test with three pattern/replacement pairs
    String ppl =
        "source=EMP | replace \"CLERK\" WITH \"EMPLOYEE\", \"MANAGER\" WITH \"SUPERVISOR\","
            + " \"ANALYST\" WITH \"RESEARCHER\" IN JOB";
    RelNode root = getRelNode(ppl);

    // Should generate triple nested REPLACE calls
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[REPLACE(REPLACE(REPLACE($2,"
            + " 'CLERK':VARCHAR, 'EMPLOYEE':VARCHAR), 'MANAGER':VARCHAR, 'SUPERVISOR':VARCHAR),"
            + " 'ANALYST':VARCHAR, 'RESEARCHER':VARCHAR)], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";

    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`, REPLACE(REPLACE(REPLACE(`JOB`, 'CLERK', 'EMPLOYEE'),"
            + " 'MANAGER', 'SUPERVISOR'), 'ANALYST', 'RESEARCHER') `JOB`, `MGR`, `HIREDATE`,"
            + " `SAL`, `COMM`, `DEPTNO`\n"
            + "FROM `scott`.`EMP`";

    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testReplaceWithMultiplePairsOnMultipleFields() {
    // Test with multiple pattern/replacement pairs applied to multiple fields
    String ppl =
        "source=EMP | replace \"CLERK\" WITH \"EMPLOYEE\", \"MANAGER\" WITH \"SUPERVISOR\" IN JOB,"
            + " ENAME";
    RelNode root = getRelNode(ppl);

    // Should apply the same nested REPLACE calls to both JOB and ENAME fields
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[REPLACE(REPLACE($1, 'CLERK':VARCHAR,"
            + " 'EMPLOYEE':VARCHAR), 'MANAGER':VARCHAR, 'SUPERVISOR':VARCHAR)],"
            + " JOB=[REPLACE(REPLACE($2, 'CLERK':VARCHAR, 'EMPLOYEE':VARCHAR),"
            + " 'MANAGER':VARCHAR, 'SUPERVISOR':VARCHAR)], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";

    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, REPLACE(REPLACE(`ENAME`, 'CLERK', 'EMPLOYEE'), 'MANAGER',"
            + " 'SUPERVISOR') `ENAME`, REPLACE(REPLACE(`JOB`, 'CLERK', 'EMPLOYEE'), 'MANAGER',"
            + " 'SUPERVISOR') `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`\n"
            + "FROM `scott`.`EMP`";

    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testReplaceWithMultiplePairsSequentialApplication() {
    // Pairs apply in order, so CLERK becomes MANAGER and then that MANAGER becomes BOSS
    String ppl =
        "source=EMP | replace \"CLERK\" WITH \"MANAGER\", \"MANAGER\" WITH \"BOSS\" IN JOB";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[REPLACE(REPLACE($2, 'CLERK':VARCHAR,"
            + " 'MANAGER':VARCHAR), 'MANAGER':VARCHAR, 'BOSS':VARCHAR)], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";

    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`, REPLACE(REPLACE(`JOB`, 'CLERK', 'MANAGER'), 'MANAGER', 'BOSS')"
            + " `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`\n"
            + "FROM `scott`.`EMP`";

    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test(expected = SyntaxCheckException.class)
  public void testReplaceWithMultiplePairsMissingWithKeywordShouldFail() {
    // Missing WITH keyword between pairs
    String ppl =
        "source=EMP | replace \"CLERK\" \"EMPLOYEE\", \"MANAGER\" WITH \"SUPERVISOR\" IN JOB";
    getRelNode(ppl);
  }

  @Test(expected = SyntaxCheckException.class)
  public void testReplaceWithMultiplePairsTrailingCommaShouldFail() {
    // Trailing comma after last pair
    String ppl = "source=EMP | replace \"CLERK\" WITH \"EMPLOYEE\", IN JOB";
    getRelNode(ppl);
  }

  @Test
  public void testWildcardReplace_prefixWildcard() {
    // Replace suffix wildcard - e.g., "*MAN" matches "SALESMAN" → "SELLER"
    // Wildcard pattern is converted to regex at planning time
    String ppl = "source=EMP | replace \"*MAN\" WITH \"SELLER\" IN JOB";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[REGEXP_REPLACE($2,"
            + " '^\\Q\\E(.*?)\\QMAN\\E$':VARCHAR, 'SELLER':VARCHAR)], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";

    verifyLogical(root, expectedLogical);
  }

  @Test
  public void testWildcardReplace_multipleWildcards() {
    // Replace with multiple wildcards for capture and substitution
    // Wildcard pattern "*_*" is converted to regex replacement "$1_$2"
    String ppl = "source=EMP | replace \"* - *\" WITH \"*_*\" IN JOB";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[REGEXP_REPLACE($2, '^\\Q\\E(.*?)\\Q -"
            + " \\E(.*?)\\Q\\E$':VARCHAR, '$1_$2':VARCHAR)], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";

    verifyLogical(root, expectedLogical);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testWildcardReplace_symmetryMismatch_shouldFail() {
    // Pattern has 2 wildcards, replacement has 1 - should throw error
    String ppl = "source=EMP | replace \"* - *\" WITH \"*\" IN JOB";
    getRelNode(ppl);
  }

  @Test
  public void testWildcardReplace_symmetryValid_zeroInReplacement() {
    // Pattern has 2 wildcards, replacement has 0 - should work
    // Literal replacement "FIXED" has no wildcards, which is valid
    String ppl = "source=EMP | replace \"* - *\" WITH \"FIXED\" IN JOB";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[REGEXP_REPLACE($2, '^\\Q\\E(.*?)\\Q -"
            + " \\E(.*?)\\Q\\E$':VARCHAR, 'FIXED':VARCHAR)], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";

    verifyLogical(root, expectedLogical);
  }

  @Test
  public void testWildcardAndLiteralReplace_mixedPairs() {
    // Multiple pairs: one with wildcard (converted to REGEXP_REPLACE), one literal (REPLACE)
    String ppl =
        "source=EMP | replace \"*CLERK\" WITH \"EMPLOYEE\", \"MANAGER\" WITH \"SUPERVISOR\" IN JOB";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[REPLACE(REGEXP_REPLACE($2,"
            + " '^\\Q\\E(.*?)\\QCLERK\\E$':VARCHAR, 'EMPLOYEE':VARCHAR), 'MANAGER':VARCHAR,"
            + " 'SUPERVISOR':VARCHAR)], MGR=[$3], HIREDATE=[$4], SAL=[$5], COMM=[$6],"
            + " DEPTNO=[$7])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";

    verifyLogical(root, expectedLogical);
  }

  /** Columns A, a, B, A: a repeated name and a name that differs only in case. */
  private static class RepeatedNamesTable extends AbstractTable implements ScannableTable {
    @Override
    public RelDataType getRowType(RelDataTypeFactory factory) {
      return factory
          .builder()
          .add("A", SqlTypeName.VARCHAR)
          .add("a", SqlTypeName.VARCHAR)
          .add("B", SqlTypeName.VARCHAR)
          .add("A", SqlTypeName.VARCHAR)
          .build();
    }

    @Override
    public Enumerable<@Nullable Object[]> scan(DataContext root) {
      return Linq4j.emptyEnumerable();
    }
  }
}
