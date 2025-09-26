/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.test.CalciteAssert;
import org.junit.Test;

public class CalcitePPLSpathTest extends CalcitePPLAbstractTest {

  public CalcitePPLSpathTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Test
  public void testSimpleEval() {
    String ppl = "source=EMP | spath src.path input=ENAME";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7], src.path=[JSON_EXTRACT($1, 'src.path':VARCHAR)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`,"
            + " `JSON_EXTRACT`(`ENAME`, 'src.path') `src.path`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testEvalWithOutput() {
    String ppl = "source=EMP | spath src.path input=ENAME output=custom | fields custom";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(custom=[JSON_EXTRACT($1, 'src.path':VARCHAR)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `JSON_EXTRACT`(`ENAME`, 'src.path') `custom`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testDynamicColumnsBasic() {
    String ppl = "source=EMP | spath input=ENAME";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7], _dynamic_columns=[COALESCE(MAP_MERGE(null:(VARCHAR NOT"
            + " NULL, ANY NOT NULL) MAP, JSON_EXTRACT_ALL($1)), JSON_EXTRACT_ALL($1))])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`,"
            + " COALESCE(`MAP_MERGE`(NULL, `JSON_EXTRACT_ALL`(`ENAME`)),"
            + " `JSON_EXTRACT_ALL`(`ENAME`)) `_dynamic_columns`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testDynamicColumnsWithFields() {
    String ppl = "source=EMP | spath input=ENAME | fields EMPNO, name, age";
    RelNode root = getRelNode(ppl);
    // Updated to match Calcite's optimized query plan (inlines JSON_EXTRACT_ALL into MAP_GET)
    // This optimization eliminates intermediate _dynamic_columns field for better performance
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], name=[MAP_GET(COALESCE(MAP_MERGE(null:(VARCHAR NOT NULL, ANY"
            + " NOT NULL) MAP, JSON_EXTRACT_ALL($1)), JSON_EXTRACT_ALL($1)), 'name')],"
            + " age=[MAP_GET(COALESCE(MAP_MERGE(null:(VARCHAR NOT NULL, ANY NOT NULL) MAP,"
            + " JSON_EXTRACT_ALL($1)), JSON_EXTRACT_ALL($1)), 'age')])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, `MAP_GET`(COALESCE(`MAP_MERGE`(NULL, `JSON_EXTRACT_ALL`(`ENAME`)),"
            + " `JSON_EXTRACT_ALL`(`ENAME`)), 'name') `name`, `MAP_GET`(COALESCE(`MAP_MERGE`(NULL,"
            + " `JSON_EXTRACT_ALL`(`ENAME`)), `JSON_EXTRACT_ALL`(`ENAME`)), 'age') `age`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testDynamicColumnsWithCustomInput() {
    String ppl = "source=EMP | spath input=JOB";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7], _dynamic_columns=[COALESCE(MAP_MERGE(null:(VARCHAR NOT"
            + " NULL, ANY NOT NULL) MAP, JSON_EXTRACT_ALL($2)), JSON_EXTRACT_ALL($2))])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`,"
            + " COALESCE(`MAP_MERGE`(NULL, `JSON_EXTRACT_ALL`(`JOB`)), `JSON_EXTRACT_ALL`(`JOB`))"
            + " `_dynamic_columns`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testDynamicColumnsWithMultipleFields() {
    String ppl = "source=EMP | spath input=ENAME | fields name, age, city, country";
    RelNode root = getRelNode(ppl);
    // Updated to match Calcite's optimized query plan (inlines JSON_EXTRACT_ALL into MAP_GET)
    // NOTE: This creates multiple JSON_EXTRACT_ALL calls - potential optimization opportunity
    String expectedLogical =
        "LogicalProject(name=[MAP_GET(COALESCE(MAP_MERGE(null:(VARCHAR NOT NULL, ANY NOT NULL) MAP,"
            + " JSON_EXTRACT_ALL($1)), JSON_EXTRACT_ALL($1)), 'name')],"
            + " age=[MAP_GET(COALESCE(MAP_MERGE(null:(VARCHAR NOT NULL, ANY NOT NULL) MAP,"
            + " JSON_EXTRACT_ALL($1)), JSON_EXTRACT_ALL($1)), 'age')],"
            + " city=[MAP_GET(COALESCE(MAP_MERGE(null:(VARCHAR NOT NULL, ANY NOT NULL) MAP,"
            + " JSON_EXTRACT_ALL($1)), JSON_EXTRACT_ALL($1)), 'city')],"
            + " country=[MAP_GET(COALESCE(MAP_MERGE(null:(VARCHAR NOT NULL, ANY NOT NULL) MAP,"
            + " JSON_EXTRACT_ALL($1)), JSON_EXTRACT_ALL($1)), 'country')])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `MAP_GET`(COALESCE(`MAP_MERGE`(NULL, `JSON_EXTRACT_ALL`(`ENAME`)),"
            + " `JSON_EXTRACT_ALL`(`ENAME`)), 'name') `name`, `MAP_GET`(COALESCE(`MAP_MERGE`(NULL,"
            + " `JSON_EXTRACT_ALL`(`ENAME`)), `JSON_EXTRACT_ALL`(`ENAME`)), 'age') `age`,"
            + " `MAP_GET`(COALESCE(`MAP_MERGE`(NULL, `JSON_EXTRACT_ALL`(`ENAME`)),"
            + " `JSON_EXTRACT_ALL`(`ENAME`)), 'city') `city`, `MAP_GET`(COALESCE(`MAP_MERGE`(NULL,"
            + " `JSON_EXTRACT_ALL`(`ENAME`)), `JSON_EXTRACT_ALL`(`ENAME`)), 'country') `country`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }
}
