/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.test.CalciteAssert;
import org.junit.Test;

public class CalcitePPLTransposeTest extends CalcitePPLAbstractTest {

  public CalcitePPLTransposeTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Test
  public void testSimpleCountWithTranspose() {
    String ppl = "source=EMP | stats count() as c|transpose";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(column=[$0], row 1=[$1], row 2=[$2], row 3=[$3], row 4=[$4], row 5=[$5])\n"
            + "  LogicalAggregate(group=[{1}], row 1_null=[MAX($0) FILTER $2], row 2_null=[MAX($0)"
            + " FILTER $3], row 3_null=[MAX($0) FILTER $4], row 4_null=[MAX($0) FILTER $5], row"
            + " 5_null=[MAX($0) FILTER $6])\n"
            + "    LogicalProject(value=[CAST($3):VARCHAR NOT NULL], $f4=[TRIM(FLAG(BOTH), ' ',"
            + " $2)], $f5=[=($1, 1)], $f6=[=($1, 2)], $f7=[=($1, 3)], $f8=[=($1, 4)], $f9=[=($1,"
            + " 5)])\n"
            + "      LogicalFilter(condition=[IS NOT NULL($3)])\n"
            + "        LogicalProject(c=[$0], _row_number_transpose_=[$1], column=[$2],"
            + " value=[CASE(=($2, 'c'), SAFE_CAST($0), null:NULL)])\n"
            + "          LogicalJoin(condition=[true], joinType=[inner])\n"
            + "            LogicalProject(c=[$0], _row_number_transpose_=[ROW_NUMBER() OVER ()])\n"
            + "              LogicalAggregate(group=[{}], c=[COUNT()])\n"
            + "                LogicalTableScan(table=[[scott, EMP]])\n"
            + "            LogicalValues(tuples=[[{ 'c' }]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult = "column=c; row 1=14; row 2=null; row 3=null; row 4=null; row 5=null\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT TRIM(`column`) `column`, MAX(CAST(`value` AS STRING)) FILTER (WHERE"
            + " `_row_number_transpose_` = 1) `row 1`, MAX(CAST(`value` AS STRING)) FILTER (WHERE"
            + " `_row_number_transpose_` = 2) `row 2`, MAX(CAST(`value` AS STRING)) FILTER (WHERE"
            + " `_row_number_transpose_` = 3) `row 3`, MAX(CAST(`value` AS STRING)) FILTER (WHERE"
            + " `_row_number_transpose_` = 4) `row 4`, MAX(CAST(`value` AS STRING)) FILTER (WHERE"
            + " `_row_number_transpose_` = 5) `row 5`\n"
            + "FROM (SELECT `t0`.`c`, `t0`.`_row_number_transpose_`, `t1`.`column`, CASE WHEN"
            + " `t1`.`column` = 'c' THEN TRY_CAST(`t0`.`c` AS STRING) ELSE NULL END `value`\n"
            + "FROM (SELECT COUNT(*) `c`, ROW_NUMBER() OVER () `_row_number_transpose_`\n"
            + "FROM `scott`.`EMP`) `t0`\n"
            + "CROSS JOIN (VALUES ('c')) `t1` (`column`)) `t2`\n"
            + "WHERE `t2`.`value` IS NOT NULL\n"
            + "GROUP BY TRIM(`column`)";

    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testMultipleAggregatesWithAliasesTranspose() {
    String ppl =
        "source=EMP | stats avg(SAL) as avg_sal, max(SAL) as max_sal, min(SAL) as min_sal, count()"
            + " as cnt|transpose ";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(column=[$0], row 1=[$1], row 2=[$2], row 3=[$3], row 4=[$4], row 5=[$5])\n"
            + "  LogicalAggregate(group=[{1}], row 1_null=[MAX($0) FILTER $2], row 2_null=[MAX($0)"
            + " FILTER $3], row 3_null=[MAX($0) FILTER $4], row 4_null=[MAX($0) FILTER $5], row"
            + " 5_null=[MAX($0) FILTER $6])\n"
            + "    LogicalProject(value=[CAST($6):VARCHAR NOT NULL], $f7=[TRIM(FLAG(BOTH), ' ',"
            + " $5)], $f8=[=($4, 1)], $f9=[=($4, 2)], $f10=[=($4, 3)], $f11=[=($4, 4)], $f12=[=($4,"
            + " 5)])\n"
            + "      LogicalFilter(condition=[IS NOT NULL($6)])\n"
            + "        LogicalProject(avg_sal=[$0], max_sal=[$1], min_sal=[$2], cnt=[$3],"
            + " _row_number_transpose_=[$4], column=[$5], value=[CASE(=($5, 'avg_sal'),"
            + " NUMBER_TO_STRING($0), =($5, 'max_sal'), NUMBER_TO_STRING($1), =($5, 'min_sal'),"
            + " NUMBER_TO_STRING($2), =($5, 'cnt'), SAFE_CAST($3), null:NULL)])\n"
            + "          LogicalJoin(condition=[true], joinType=[inner])\n"
            + "            LogicalProject(avg_sal=[$0], max_sal=[$1], min_sal=[$2], cnt=[$3],"
            + " _row_number_transpose_=[ROW_NUMBER() OVER ()])\n"
            + "              LogicalAggregate(group=[{}], avg_sal=[AVG($0)], max_sal=[MAX($0)],"
            + " min_sal=[MIN($0)], cnt=[COUNT()])\n"
            + "                LogicalProject(SAL=[$5])\n"
            + "                  LogicalTableScan(table=[[scott, EMP]])\n"
            + "            LogicalValues(tuples=[[{ 'avg_sal' }, { 'max_sal' }, { 'min_sal' }, {"
            + " 'cnt' }]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult =
        "column=avg_sal; row 1=2073.214285; row 2=null; row 3=null; row 4=null; row 5=null\n"
            + "column=max_sal; row 1=5000.00; row 2=null; row 3=null; row 4=null; row 5=null\n"
            + "column=cnt; row 1=14; row 2=null; row 3=null; row 4=null; row 5=null\n"
            + "column=min_sal; row 1=800.00; row 2=null; row 3=null; row 4=null; row 5=null\n";

    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT TRIM(`column`) `column`, MAX(CAST(`value` AS STRING)) FILTER (WHERE"
            + " `_row_number_transpose_` = 1) `row 1`, MAX(CAST(`value` AS STRING)) FILTER (WHERE"
            + " `_row_number_transpose_` = 2) `row 2`, MAX(CAST(`value` AS STRING)) FILTER (WHERE"
            + " `_row_number_transpose_` = 3) `row 3`, MAX(CAST(`value` AS STRING)) FILTER (WHERE"
            + " `_row_number_transpose_` = 4) `row 4`, MAX(CAST(`value` AS STRING)) FILTER (WHERE"
            + " `_row_number_transpose_` = 5) `row 5`\n"
            + "FROM (SELECT `t1`.`avg_sal`, `t1`.`max_sal`, `t1`.`min_sal`, `t1`.`cnt`,"
            + " `t1`.`_row_number_transpose_`, `t2`.`column`, CASE WHEN `t2`.`column` = 'avg_sal'"
            + " THEN NUMBER_TO_STRING(`t1`.`avg_sal`) WHEN `t2`.`column` = 'max_sal' THEN"
            + " NUMBER_TO_STRING(`t1`.`max_sal`) WHEN `t2`.`column` = 'min_sal' THEN"
            + " NUMBER_TO_STRING(`t1`.`min_sal`) WHEN `t2`.`column` = 'cnt' THEN"
            + " TRY_CAST(`t1`.`cnt` AS STRING) ELSE NULL END `value`\n"
            + "FROM (SELECT AVG(`SAL`) `avg_sal`, MAX(`SAL`) `max_sal`, MIN(`SAL`) `min_sal`,"
            + " COUNT(*) `cnt`, ROW_NUMBER() OVER () `_row_number_transpose_`\n"
            + "FROM `scott`.`EMP`) `t1`\n"
            + "CROSS JOIN (VALUES ('avg_sal'),\n"
            + "('max_sal'),\n"
            + "('min_sal'),\n"
            + "('cnt')) `t2` (`column`)) `t3`\n"
            + "WHERE `t3`.`value` IS NOT NULL\n"
            + "GROUP BY TRIM(`column`)";

    /*
    "SELECT `column`, MAX(CASE WHEN `__row_id__` = 1 THEN CAST(`value` AS STRING) ELSE NULL"
        + " END) `row 1`, MAX(CASE WHEN `__row_id__` = 2 THEN CAST(`value` AS STRING) ELSE NULL"
        + " END) `row 2`, MAX(CASE WHEN `__row_id__` = 3 THEN CAST(`value` AS STRING) ELSE NULL"
        + " END) `row 3`, MAX(CASE WHEN `__row_id__` = 4 THEN CAST(`value` AS STRING) ELSE NULL"
        + " END) `row 4`, MAX(CASE WHEN `__row_id__` = 5 THEN CAST(`value` AS STRING) ELSE NULL"
        + " END) `row 5`\n"
        + "FROM (SELECT `t1`.`avg_sal`, `t1`.`max_sal`, `t1`.`min_sal`, `t1`.`cnt`,"
        + " `t1`.`__row_id__`, `t2`.`column`, CASE WHEN `t2`.`column` = 'avg_sal' THEN"
        + " NUMBER_TO_STRING(`t1`.`avg_sal`) WHEN `t2`.`column` = 'max_sal' THEN"
        + " NUMBER_TO_STRING(`t1`.`max_sal`) WHEN `t2`.`column` = 'min_sal' THEN"
        + " NUMBER_TO_STRING(`t1`.`min_sal`) WHEN `t2`.`column` = 'cnt' THEN CAST(`t1`.`cnt` AS"
        + " STRING) ELSE NULL END `value`\n"
        + "FROM (SELECT AVG(`SAL`) `avg_sal`, MAX(`SAL`) `max_sal`, MIN(`SAL`) `min_sal`,"
        + " COUNT(*) `cnt`, ROW_NUMBER() OVER () `__row_id__`\n"
        + "FROM `scott`.`EMP`) `t1`\n"
        + "CROSS JOIN (VALUES ('avg_sal'),\n"
        + "('max_sal'),\n"
        + "('min_sal'),\n"
        + "('cnt')) `t2` (`column`)) `t3`\n"
        + "WHERE `t3`.`value` IS NOT NULL\n"
        + "GROUP BY `column`";

    */
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testTransposeWithLimit() {
    String ppl = "source=EMP | fields  ENAME, COMM, JOB, SAL |  transpose 3";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(column=[$0], row 1=[$1], row 2=[$2], row 3=[$3])\n"
            + "  LogicalAggregate(group=[{1}], row 1_null=[MAX($0) FILTER $2], row 2_null=[MAX($0)"
            + " FILTER $3], row 3_null=[MAX($0) FILTER $4])\n"
            + "    LogicalProject(value=[CAST($6):VARCHAR NOT NULL], $f7=[TRIM(FLAG(BOTH), ' ',"
            + " $5)], $f8=[=($4, 1)], $f9=[=($4, 2)], $f10=[=($4, 3)])\n"
            + "      LogicalFilter(condition=[IS NOT NULL($6)])\n"
            + "        LogicalProject(ENAME=[$0], COMM=[$1], JOB=[$2], SAL=[$3],"
            + " _row_number_transpose_=[$4], column=[$5], value=[CASE(=($5, 'ENAME'),"
            + " SAFE_CAST($0), =($5, 'COMM'), NUMBER_TO_STRING($1), =($5, 'JOB'),"
            + " SAFE_CAST($2), =($5, 'SAL'), NUMBER_TO_STRING($3), null:NULL)])\n"
            + "          LogicalJoin(condition=[true], joinType=[inner])\n"
            + "            LogicalProject(ENAME=[$1], COMM=[$6], JOB=[$2], SAL=[$5],"
            + " _row_number_transpose_=[ROW_NUMBER() OVER ()])\n"
            + "              LogicalTableScan(table=[[scott, EMP]])\n"
            + "            LogicalValues(tuples=[[{ 'ENAME' }, { 'COMM' }, { 'JOB' }, { 'SAL'"
            + " }]])\n";

    verifyLogical(root, expectedLogical);
    String expectedResult =
        "column=ENAME; row 1=SMITH; row 2=ALLEN; row 3=WARD\n"
            + "column=COMM; row 1=null; row 2=300.00; row 3=500.00\n"
            + "column=JOB; row 1=CLERK; row 2=SALESMAN; row 3=SALESMAN\n"
            + "column=SAL; row 1=800.00; row 2=1600.00; row 3=1250.00\n";

    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT TRIM(`column`) `column`, MAX(CAST(`value` AS STRING)) FILTER (WHERE"
            + " `_row_number_transpose_` = 1) `row 1`, MAX(CAST(`value` AS STRING)) FILTER (WHERE"
            + " `_row_number_transpose_` = 2) `row 2`, MAX(CAST(`value` AS STRING)) FILTER (WHERE"
            + " `_row_number_transpose_` = 3) `row 3`\n"
            + "FROM (SELECT `t`.`ENAME`, `t`.`COMM`, `t`.`JOB`, `t`.`SAL`,"
            + " `t`.`_row_number_transpose_`, `t0`.`column`, CASE WHEN `t0`.`column` = 'ENAME' THEN"
            + " TRY_CAST(`t`.`ENAME` AS STRING) WHEN `t0`.`column` = 'COMM' THEN"
            + " NUMBER_TO_STRING(`t`.`COMM`) WHEN `t0`.`column` = 'JOB' THEN TRY_CAST(`t`.`JOB` AS"
            + " STRING) WHEN `t0`.`column` = 'SAL' THEN NUMBER_TO_STRING(`t`.`SAL`) ELSE NULL END"
            + " `value`\n"
            + "FROM (SELECT `ENAME`, `COMM`, `JOB`, `SAL`, ROW_NUMBER() OVER ()"
            + " `_row_number_transpose_`\n"
            + "FROM `scott`.`EMP`) `t`\n"
            + "CROSS JOIN (VALUES ('ENAME'),\n"
            + "('COMM'),\n"
            + "('JOB'),\n"
            + "('SAL')) `t0` (`column`)) `t1`\n"
            + "WHERE `t1`.`value` IS NOT NULL\n"
            + "GROUP BY TRIM(`column`)";

    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testTransposeWithLimitColumnName() {
    String ppl =
        "source=EMP | fields  ENAME, COMM, JOB, SAL |  transpose 3 column_name='column_names'";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(column_names=[$0], row 1=[$1], row 2=[$2], row 3=[$3])\n"
            + "  LogicalAggregate(group=[{1}], row 1_null=[MAX($0) FILTER $2], row 2_null=[MAX($0)"
            + " FILTER $3], row 3_null=[MAX($0) FILTER $4])\n"
            + "    LogicalProject(value=[CAST($6):VARCHAR NOT NULL], $f7=[TRIM(FLAG(BOTH), ' ',"
            + " $5)], $f8=[=($4, 1)], $f9=[=($4, 2)], $f10=[=($4, 3)])\n"
            + "      LogicalFilter(condition=[IS NOT NULL($6)])\n"
            + "        LogicalProject(ENAME=[$0], COMM=[$1], JOB=[$2], SAL=[$3],"
            + " _row_number_transpose_=[$4], column_names=[$5], value=[CASE(=($5, 'ENAME'),"
            + " SAFE_CAST($0), =($5, 'COMM'), NUMBER_TO_STRING($1), =($5, 'JOB'),"
            + " SAFE_CAST($2), =($5, 'SAL'), NUMBER_TO_STRING($3), null:NULL)])\n"
            + "          LogicalJoin(condition=[true], joinType=[inner])\n"
            + "            LogicalProject(ENAME=[$1], COMM=[$6], JOB=[$2], SAL=[$5],"
            + " _row_number_transpose_=[ROW_NUMBER() OVER ()])\n"
            + "              LogicalTableScan(table=[[scott, EMP]])\n"
            + "            LogicalValues(tuples=[[{ 'ENAME' }, { 'COMM' }, { 'JOB' }, { 'SAL'"
            + " }]])\n";

    verifyLogical(root, expectedLogical);
    String expectedResult =
        "column_names=ENAME; row 1=SMITH; row 2=ALLEN; row 3=WARD\n"
            + "column_names=COMM; row 1=null; row 2=300.00; row 3=500.00\n"
            + "column_names=JOB; row 1=CLERK; row 2=SALESMAN; row 3=SALESMAN\n"
            + "column_names=SAL; row 1=800.00; row 2=1600.00; row 3=1250.00\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT TRIM(`column_names`) `column_names`, MAX(CAST(`value` AS STRING)) FILTER (WHERE"
            + " `_row_number_transpose_` = 1) `row 1`, MAX(CAST(`value` AS STRING)) FILTER (WHERE"
            + " `_row_number_transpose_` = 2) `row 2`, MAX(CAST(`value` AS STRING)) FILTER (WHERE"
            + " `_row_number_transpose_` = 3) `row 3`\n"
            + "FROM (SELECT `t`.`ENAME`, `t`.`COMM`, `t`.`JOB`, `t`.`SAL`,"
            + " `t`.`_row_number_transpose_`, `t0`.`column_names`, CASE WHEN `t0`.`column_names` ="
            + " 'ENAME' THEN TRY_CAST(`t`.`ENAME` AS STRING) WHEN `t0`.`column_names` = 'COMM' THEN"
            + " NUMBER_TO_STRING(`t`.`COMM`) WHEN `t0`.`column_names` = 'JOB' THEN"
            + " TRY_CAST(`t`.`JOB` AS STRING) WHEN `t0`.`column_names` = 'SAL' THEN"
            + " NUMBER_TO_STRING(`t`.`SAL`) ELSE NULL END `value`\n"
            + "FROM (SELECT `ENAME`, `COMM`, `JOB`, `SAL`, ROW_NUMBER() OVER ()"
            + " `_row_number_transpose_`\n"
            + "FROM `scott`.`EMP`) `t`\n"
            + "CROSS JOIN (VALUES ('ENAME'),\n"
            + "('COMM'),\n"
            + "('JOB'),\n"
            + "('SAL')) `t0` (`column_names`)) `t1`\n"
            + "WHERE `t1`.`value` IS NOT NULL\n"
            + "GROUP BY TRIM(`column_names`)";

    verifyPPLToSparkSQL(root, expectedSparkSql);
  }
}
