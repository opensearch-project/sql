/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.test.CalciteAssert;
import org.junit.Test;

public class CalcitePPLStringFunctionTest extends CalcitePPLAbstractTest {

  public CalcitePPLStringFunctionTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Test
  public void testLower() {
    String ppl = "source=EMP | eval lower_name = lower(ENAME) | fields lower_name";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalProject(lower_name=[LOWER($1)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult =
        ""
            + "lower_name=smith\n"
            + "lower_name=allen\n"
            + "lower_name=ward\n"
            + "lower_name=jones\n"
            + "lower_name=martin\n"
            + "lower_name=blake\n"
            + "lower_name=clark\n"
            + "lower_name=scott\n"
            + "lower_name=king\n"
            + "lower_name=turner\n"
            + "lower_name=adams\n"
            + "lower_name=james\n"
            + "lower_name=ford\n"
            + "lower_name=miller\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql = "" + "SELECT LOWER(`ENAME`) `lower_name`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testLike() {
    String ppl = "source=EMP | where like(JOB, 'SALE%') | stats count() as cnt";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalAggregate(group=[{}], cnt=[COUNT()])\n"
            + "  LogicalFilter(condition=[ILIKE($2, 'SALE%', '\\')])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult = "cnt=4\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        ""
            + "SELECT COUNT(*) `cnt`\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE `JOB` ILIKE 'SALE%' ESCAPE '\\'";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testRegexMatch() {
    String ppl = "source=EMP | where regex_match(ENAME, '^[A-C]') | fields ENAME";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalProject(ENAME=[$1])\n"
            + "  LogicalFilter(condition=[REGEXP_CONTAINS($1, '^[A-C]':VARCHAR)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult =
        "" + "ENAME=ALLEN\n" + "ENAME=BLAKE\n" + "ENAME=CLARK\n" + "ENAME=ADAMS\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        ""
            + "SELECT `ENAME`\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE REGEXP_CONTAINS(`ENAME`, '^[A-C]')";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testRegexMatchWithPattern() {
    String ppl = "source=EMP | eval matches = regex_match(JOB, 'MAN.*') | fields JOB, matches";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalProject(JOB=[$2], matches=[REGEXP_CONTAINS($2, 'MAN.*':VARCHAR)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "" + "SELECT `JOB`, REGEXP_CONTAINS(`JOB`, 'MAN.*') `matches`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testRegexMatchInEval() {
    String ppl =
        "source=EMP | eval result = if(regex_match(ENAME, '^S'), 1, 0) | where result = 1 | fields"
            + " ENAME";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(ENAME=[$1])\n"
            + "  LogicalFilter(condition=[=($8, 1)])\n"
            + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], result=[CASE(REGEXP_CONTAINS($1, '^S':VARCHAR),"
            + " 1, 0)])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    // Expected: SMITH and SCOTT have names starting with 'S'

    String expectedSparkSql =
        ""
            + "SELECT `ENAME`\n"
            + "FROM (SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`, "
            + "CASE WHEN REGEXP_CONTAINS(`ENAME`, '^S') THEN 1 ELSE 0 END `result`\n"
            + "FROM `scott`.`EMP`) `t`\n"
            + "WHERE `result` = 1";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testRegexMatchInWhereClause() {
    // Test with WHERE clause to filter employees with names ending in 'ES'
    String ppl = "source=EMP | where regex_match(ENAME, 'ES$') | fields ENAME, JOB";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalProject(ENAME=[$1], JOB=[$2])\n"
            + "  LogicalFilter(condition=[REGEXP_CONTAINS($1, 'ES$':VARCHAR)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    // Expected: JONES and JAMES have names ending with 'ES'
    // verifyResult would check the actual data returned

    String expectedSparkSql =
        ""
            + "SELECT `ENAME`, `JOB`\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE REGEXP_CONTAINS(`ENAME`, 'ES$')";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testRegexMatchWithJobPattern() {
    // Test filtering ANALYST and MANAGER positions using regex
    String ppl =
        "source=EMP | where regex_match(JOB, '(ANALYST|MANAGER)') | fields ENAME, JOB, SAL";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalProject(ENAME=[$1], JOB=[$2], SAL=[$5])\n"
            + "  LogicalFilter(condition=[REGEXP_CONTAINS($2, '(ANALYST|MANAGER)':VARCHAR)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    // Expected: JONES, BLAKE, CLARK (MANAGER) and SCOTT, FORD (ANALYST)

    String expectedSparkSql =
        ""
            + "SELECT `ENAME`, `JOB`, `SAL`\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE REGEXP_CONTAINS(`JOB`, '(ANALYST|MANAGER)')";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testRegexMatchCaseInsensitive() {
    // Test case-insensitive pattern matching
    String ppl = "source=EMP | where regex_match(ENAME, '(?i)^[m-s]') | fields ENAME | head 5";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalSort(fetch=[5])\n"
            + "  LogicalProject(ENAME=[$1])\n"
            + "    LogicalFilter(condition=[REGEXP_CONTAINS($1, '(?i)^[m-s]':VARCHAR)])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    // Expected: Names starting with M-S (case insensitive)

    String expectedSparkSql =
        ""
            + "SELECT `ENAME`\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE REGEXP_CONTAINS(`ENAME`, '(?i)^[m-s]')\n"
            + "LIMIT 5";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testRegexMatchWithMultipleConditions() {
    // Test combining regex_match with other conditions
    String ppl =
        "source=EMP | where regex_match(JOB, 'CLERK') AND SAL > 1000 | fields ENAME, JOB, SAL";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(ENAME=[$1], JOB=[$2], SAL=[$5])\n"
            + "  LogicalFilter(condition=[AND(REGEXP_CONTAINS($2, 'CLERK':VARCHAR), >($5,"
            + " 1000))])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    // Expected: ADAMS and MILLER (CLERKs with SAL > 1000)

    String expectedSparkSql =
        ""
            + "SELECT `ENAME`, `JOB`, `SAL`\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE REGEXP_CONTAINS(`JOB`, 'CLERK') AND `SAL` > 1000";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testRegexMatchNegation() {
    // Test NOT regex_match pattern
    String ppl = "source=EMP | where NOT regex_match(JOB, 'CLERK|SALESMAN') | fields ENAME, JOB";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalProject(ENAME=[$1], JOB=[$2])\n"
            + "  LogicalFilter(condition=[NOT(REGEXP_CONTAINS($2, 'CLERK|SALESMAN':VARCHAR))])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    // Expected: All non-CLERK and non-SALESMAN employees

    String expectedSparkSql =
        ""
            + "SELECT `ENAME`, `JOB`\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE NOT REGEXP_CONTAINS(`JOB`, 'CLERK|SALESMAN')";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testRegexMatchWithStats() {
    // Test regex_match with aggregation
    String ppl =
        "source=EMP | where regex_match(JOB, 'MAN') | stats count() as manager_count, avg(SAL) as"
            + " avg_salary";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalAggregate(group=[{}], manager_count=[COUNT()], avg_salary=[AVG($0)])\n"
            + "  LogicalProject(SAL=[$5])\n"
            + "    LogicalFilter(condition=[REGEXP_CONTAINS($2, 'MAN':VARCHAR)])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    // Expected: Count and average salary for jobs containing 'MAN'

    String expectedSparkSql =
        ""
            + "SELECT COUNT(*) `manager_count`, AVG(`SAL`) `avg_salary`\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE REGEXP_CONTAINS(`JOB`, 'MAN')";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testReplaceLiteralString() {
    // Test basic literal string replacement - replaces all 'A' with 'X'
    String ppl = "source=EMP | eval new_name = replace(ENAME, 'A', 'X') | fields ENAME, new_name";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(ENAME=[$1], new_name=[REGEXP_REPLACE($1, 'A', 'X')])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `ENAME`, REGEXP_REPLACE(`ENAME`, 'A', 'X') `new_name`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testReplaceWithRegexPattern() {
    // Test regex pattern - remove all digits
    String ppl = "source=EMP | eval no_digits = replace(JOB, '\\\\d+', '') | fields JOB, no_digits";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(JOB=[$2], no_digits=[REGEXP_REPLACE($2, '\\d+':VARCHAR, '':VARCHAR)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `JOB`, REGEXP_REPLACE(`JOB`, '\\d+', '') `no_digits`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testReplaceWithRegexCaptureGroups() {
    // Test regex with capture groups - swap first two characters using \1 and \2 backreferences
    String ppl =
        "source=EMP | eval swapped = replace(ENAME, '^(.)(.)', '\\\\2\\\\1') | fields ENAME,"
            + " swapped";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(ENAME=[$1], swapped=[REGEXP_REPLACE($1, '^(.)(.)':VARCHAR,"
            + " '$2$1')])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `ENAME`, REGEXP_REPLACE(`ENAME`, '^(.)(.)', '$2$1') `swapped`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }
}
