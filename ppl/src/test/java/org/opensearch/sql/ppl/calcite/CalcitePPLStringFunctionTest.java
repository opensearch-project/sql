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

  // This test evalutes tostring where it gets converted to cast call

  @Test
  public void testToStringFormatNotSpecified() {
    String ppl =
        "source=EMP | eval string_value = tostring(MGR) | eval cast_value = cast(MGR as string)|"
            + " fields string_value, cast_value";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(string_value=[CAST($3):VARCHAR], cast_value=[SAFE_CAST($3)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    String expectedResult =
        "string_value=7902; cast_value=7902\n"
            + "string_value=7698; cast_value=7698\n"
            + "string_value=7698; cast_value=7698\n"
            + "string_value=7839; cast_value=7839\n"
            + "string_value=7698; cast_value=7698\n"
            + "string_value=7839; cast_value=7839\n"
            + "string_value=7839; cast_value=7839\n"
            + "string_value=7566; cast_value=7566\n"
            + "string_value=null; cast_value=null\n"
            + "string_value=7698; cast_value=7698\n"
            + "string_value=7788; cast_value=7788\n"
            + "string_value=7698; cast_value=7698\n"
            + "string_value=7566; cast_value=7566\n"
            + "string_value=7782; cast_value=7782\n";
    verifyLogical(root, expectedLogical);
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT CAST(`MGR` AS STRING) `string_value`, TRY_CAST(`MGR` AS STRING) `cast_value`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testToStringBoolean() {
    String ppl = "source=EMP | eval boolean_value = tostring(1==1) | fields boolean_value |head 1";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalSort(fetch=[1])\n"
            + "  LogicalProject(boolean_value=['TRUE':VARCHAR])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    String expectedResult = "boolean_value=TRUE\n";
    verifyLogical(root, expectedLogical);
    verifyResult(root, expectedResult);

    String expectedSparkSql = "SELECT 'TRUE' `boolean_value`\nFROM `scott`.`EMP`\nLIMIT 1";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testToStringBin() {
    String ppl =
        "source=EMP |  eval salary_binary = tostring(SAL, \"binary\") | fields ENAME,"
            + " salary_binary, SAL";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(ENAME=[$1], salary_binary=[TOSTRING($5, 'binary':VARCHAR)], SAL=[$5])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    String expectedResult =
        "ENAME=SMITH; salary_binary=1100100000; SAL=800.00\n"
            + "ENAME=ALLEN; salary_binary=11001000000; SAL=1600.00\n"
            + "ENAME=WARD; salary_binary=10011100010; SAL=1250.00\n"
            + "ENAME=JONES; salary_binary=101110011111; SAL=2975.00\n"
            + "ENAME=MARTIN; salary_binary=10011100010; SAL=1250.00\n"
            + "ENAME=BLAKE; salary_binary=101100100010; SAL=2850.00\n"
            + "ENAME=CLARK; salary_binary=100110010010; SAL=2450.00\n"
            + "ENAME=SCOTT; salary_binary=101110111000; SAL=3000.00\n"
            + "ENAME=KING; salary_binary=1001110001000; SAL=5000.00\n"
            + "ENAME=TURNER; salary_binary=10111011100; SAL=1500.00\n"
            + "ENAME=ADAMS; salary_binary=10001001100; SAL=1100.00\n"
            + "ENAME=JAMES; salary_binary=1110110110; SAL=950.00\n"
            + "ENAME=FORD; salary_binary=101110111000; SAL=3000.00\n"
            + "ENAME=MILLER; salary_binary=10100010100; SAL=1300.00\n";
    verifyLogical(root, expectedLogical);
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT `ENAME`, TOSTRING(`SAL`, 'binary') `salary_binary`, `SAL`\nFROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testToStringHex() {
    String ppl =
        "source=EMP |  eval salary_hex = tostring(SAL, \"hex\") | fields ENAME, salary_hex, SAL";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(ENAME=[$1], salary_hex=[TOSTRING($5, 'hex':VARCHAR)], SAL=[$5])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    String expectedResult =
        "ENAME=SMITH; salary_hex=320; SAL=800.00\n"
            + "ENAME=ALLEN; salary_hex=640; SAL=1600.00\n"
            + "ENAME=WARD; salary_hex=4e2; SAL=1250.00\n"
            + "ENAME=JONES; salary_hex=b9f; SAL=2975.00\n"
            + "ENAME=MARTIN; salary_hex=4e2; SAL=1250.00\n"
            + "ENAME=BLAKE; salary_hex=b22; SAL=2850.00\n"
            + "ENAME=CLARK; salary_hex=992; SAL=2450.00\n"
            + "ENAME=SCOTT; salary_hex=bb8; SAL=3000.00\n"
            + "ENAME=KING; salary_hex=1388; SAL=5000.00\n"
            + "ENAME=TURNER; salary_hex=5dc; SAL=1500.00\n"
            + "ENAME=ADAMS; salary_hex=44c; SAL=1100.00\n"
            + "ENAME=JAMES; salary_hex=3b6; SAL=950.00\n"
            + "ENAME=FORD; salary_hex=bb8; SAL=3000.00\n"
            + "ENAME=MILLER; salary_hex=514; SAL=1300.00\n";
    verifyLogical(root, expectedLogical);
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT `ENAME`, TOSTRING(`SAL`, 'hex') `salary_hex`, `SAL`\nFROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testToStringHexFromNumberAsString() {
    String ppl =
        "source=EMP |  eval salary_hex = tostring(\"1600\", \"hex\") | fields ENAME, salary_hex|"
            + " head 1";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalSort(fetch=[1])\n"
            + "  LogicalProject(ENAME=[$1], salary_hex=[TOSTRING('1600':VARCHAR, 'hex':VARCHAR)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    String expectedResult = "ENAME=SMITH; salary_hex=640\n";
    verifyLogical(root, expectedLogical);
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT `ENAME`, TOSTRING('1600', 'hex') `salary_hex`\nFROM `scott`.`EMP`\nLIMIT 1";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testToStringCommaFromNumberAsString() {
    String ppl =
        "source=EMP |  eval salary_comma = tostring(\"160040222\", \"commas\") | fields ENAME,"
            + " salary_comma| head 1";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalSort(fetch=[1])\n"
            + "  LogicalProject(ENAME=[$1], salary_comma=[TOSTRING('160040222':VARCHAR,"
            + " 'commas':VARCHAR)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    String expectedResult = "ENAME=SMITH; salary_comma=160,040,222\n";
    verifyLogical(root, expectedLogical);
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT `ENAME`, TOSTRING('160040222', 'commas') `salary_comma`\n"
            + "FROM `scott`.`EMP`\n"
            + "LIMIT 1";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testToStringBinaryFromNumberAsString() {
    String ppl =
        "source=EMP |  eval salary_binary = tostring(\"160040222\", \"binary\") | fields ENAME,"
            + " salary_binary| head 1";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalSort(fetch=[1])\n"
            + "  LogicalProject(ENAME=[$1], salary_binary=[TOSTRING('160040222':VARCHAR,"
            + " 'binary':VARCHAR)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    String expectedResult = "ENAME=SMITH; salary_binary=1001100010100000010100011110\n";
    verifyLogical(root, expectedLogical);
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT `ENAME`, TOSTRING('160040222', 'binary') `salary_binary`\n"
            + "FROM `scott`.`EMP`\n"
            + "LIMIT 1";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testToStringCommas() {
    String ppl =
        "source=EMP |  eval salary_commas = tostring(SAL, \"commas\") | fields ENAME,"
            + " salary_commas, SAL";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(ENAME=[$1], salary_commas=[TOSTRING($5, 'commas':VARCHAR)], SAL=[$5])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    String expectedResult =
        "ENAME=SMITH; salary_commas=800; SAL=800.00\n"
            + "ENAME=ALLEN; salary_commas=1,600; SAL=1600.00\n"
            + "ENAME=WARD; salary_commas=1,250; SAL=1250.00\n"
            + "ENAME=JONES; salary_commas=2,975; SAL=2975.00\n"
            + "ENAME=MARTIN; salary_commas=1,250; SAL=1250.00\n"
            + "ENAME=BLAKE; salary_commas=2,850; SAL=2850.00\n"
            + "ENAME=CLARK; salary_commas=2,450; SAL=2450.00\n"
            + "ENAME=SCOTT; salary_commas=3,000; SAL=3000.00\n"
            + "ENAME=KING; salary_commas=5,000; SAL=5000.00\n"
            + "ENAME=TURNER; salary_commas=1,500; SAL=1500.00\n"
            + "ENAME=ADAMS; salary_commas=1,100; SAL=1100.00\n"
            + "ENAME=JAMES; salary_commas=950; SAL=950.00\n"
            + "ENAME=FORD; salary_commas=3,000; SAL=3000.00\n"
            + "ENAME=MILLER; salary_commas=1,300; SAL=1300.00\n";
    verifyLogical(root, expectedLogical);
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT `ENAME`, TOSTRING(`SAL`, 'commas') `salary_commas`, `SAL`\nFROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testToStringDuration() {
    String ppl =
        "source=EMP |  eval duration_commas = tostring(6500, \"duration\") | fields ENAME,"
            + " duration_commas|HEAD 1";

    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalSort(fetch=[1])\n"
            + "  LogicalProject(ENAME=[$1], duration_commas=[TOSTRING(6500, 'duration':VARCHAR)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    String expectedResult = "ENAME=SMITH; duration_commas=01:48:20\n";
    verifyLogical(root, expectedLogical);
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT `ENAME`, TOSTRING(6500, 'duration') `duration_commas`\n"
            + "FROM `scott`.`EMP`\n"
            + "LIMIT 1";
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
  public void testRegexpMatch() {
    String ppl = "source=EMP | where regexp_match(ENAME, '^[A-C]') | fields ENAME";
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
  public void testRegexMatchCompatibility() {
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
  public void testRegexpMatchWithPattern() {
    String ppl = "source=EMP | eval matches = regexp_match(JOB, 'MAN.*') | fields JOB, matches";
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
  public void testRegexpMatchInEval() {
    String ppl =
        "source=EMP | eval result = if(regexp_match(ENAME, '^S'), 1, 0) | where result = 1 | fields"
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
  public void testRegexpMatchInWhereClause() {
    // Test with WHERE clause to filter employees with names ending in 'ES'
    String ppl = "source=EMP | where regexp_match(ENAME, 'ES$') | fields ENAME, JOB";
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
  public void testRegexpMatchWithJobPattern() {
    // Test filtering ANALYST and MANAGER positions using regex
    String ppl =
        "source=EMP | where regexp_match(JOB, '(ANALYST|MANAGER)') | fields ENAME, JOB, SAL";
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
  public void testRegexpMatchCaseInsensitive() {
    // Test case-insensitive pattern matching
    String ppl = "source=EMP | where regexp_match(ENAME, '(?i)^[m-s]') | fields ENAME | head 5";
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
  public void testRegexpMatchWithMultipleConditions() {
    // Test combining regexp_match with other conditions
    String ppl =
        "source=EMP | where regexp_match(JOB, 'CLERK') AND SAL > 1000 | fields ENAME, JOB, SAL";
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
  public void testRegexpMatchNegation() {
    // Test NOT regexp_match pattern
    String ppl = "source=EMP | where NOT regexp_match(JOB, 'CLERK|SALESMAN') | fields ENAME, JOB";
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
  public void testRegexpMatchWithStats() {
    // Test regexp_match with aggregation
    String ppl =
        "source=EMP | where regexp_match(JOB, 'MAN') | stats count() as manager_count, avg(SAL) as"
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
