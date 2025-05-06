/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.fail;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.test.CalciteAssert;
import org.junit.Test;

public class CalcitePPLCastFunctionTest extends CalcitePPLAbstractTest {

  public CalcitePPLCastFunctionTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Test
  public void testCast() {
    String ppl = "source=EMP | eval a = cast(MGR as string) | fields a";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "" + "LogicalProject(a=[SAFE_CAST($3)])\n" + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    // TODO there is no SAFE_CAST() in Spark, the Spark CAST is always safe (return null).
    String expectedSparkSql = "SELECT SAFE_CAST(`MGR` AS STRING) `a`\nFROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testCastInsensitive() {
    String ppl = "source=EMP | eval a = cast(MGR as STRING) | fields a";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "" + "LogicalProject(a=[SAFE_CAST($3)])\n" + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql = "SELECT SAFE_CAST(`MGR` AS STRING) `a`\nFROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testCastOverriding() {
    String ppl = "source=EMP | eval age = cast(MGR as string)";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7], age=[SAFE_CAST($3)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`,"
            + " SAFE_CAST(`MGR` AS STRING) `age`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testCastUnknownType() {
    String ppl = "source=EMP | eval a = cast(MGR as UNKNOWN) | fields a";
    try {
      RelNode root = getRelNode(ppl);
      fail("expected error, got " + root);
    } catch (Exception e) {
      assertThat(e.getMessage(), containsString("cast(MGR as UNKNOWN' <-- HERE"));
    }
  }

  @Test
  public void testChainedCast() {
    String ppl = "source=EMP | eval a = cast(cast(MGR as string) as integer) | fields a";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalProject(a=[SAFE_CAST(SAFE_CAST($3))])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "" + "SELECT SAFE_CAST(SAFE_CAST(`MGR` AS STRING) AS INTEGER) `a`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testChainedCast2() {
    String ppl =
        "source=EMP | eval a = cast(concat(cast(MGR as string), '0') as integer) | fields a";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalProject(a=[SAFE_CAST(CONCAT(SAFE_CAST($3), '0'))])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedResult =
        "a=79020\n"
            + "a=76980\n"
            + "a=76980\n"
            + "a=78390\n"
            + "a=76980\n"
            + "a=78390\n"
            + "a=78390\n"
            + "a=75660\n"
            + "a=null\n"
            + "a=76980\n"
            + "a=77880\n"
            + "a=76980\n"
            + "a=75660\n"
            + "a=77820\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        ""
            + "SELECT SAFE_CAST(CONCAT(SAFE_CAST(`MGR` AS STRING), '0') AS INTEGER) `a`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }
}
