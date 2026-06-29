/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.test.CalciteAssert;
import org.junit.Test;

public class CalcitePPLForeachTest extends CalcitePPLAbstractTest {

  public CalcitePPLForeachTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Test
  public void testForeachExpandsExplicitFields() {
    String ppl =
        "source=EMP | foreach SAL COMM [ eval <<FIELD>>_double = <<FIELD>> * 2 ] | fields"
            + " EMPNO, SAL_double, COMM_double";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(EMPNO=[$0], SAL_double=[*($5, 2)], COMM_double=[*($6, 2)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, `SAL` * 2 `SAL_double`, `COMM` * 2 `COMM_double`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testForeachWildcardAndMatchstr() {
    String ppl =
        "source=EMP | foreach *NO [ eval copy_<<MATCHSTR>> = <<FIELD>> ] | fields copy_EMP,"
            + " copy_DEPT";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(copy_EMP=[$0], copy_DEPT=[$7])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
  }
}
