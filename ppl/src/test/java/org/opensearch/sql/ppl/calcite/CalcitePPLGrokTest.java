/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.test.CalciteAssert.SchemaSpec;
import org.junit.Test;

public class CalcitePPLGrokTest extends CalcitePPLAbstractTest {
  public CalcitePPLGrokTest() {
    super(SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Test
  public void testGrok() {
    String ppl = "source=EMP | grok ENAME '.+@%{HOSTNAME:host}' | fields ENAME, host";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(ENAME=[$1], host=[ITEM(GROK($1, '.+@%{HOSTNAME:host}':VARCHAR), 'host')])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `ENAME`, `GROK`(`ENAME`, '.+@%{HOSTNAME:host}')['host'] `host`\nFROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testGrokOverriding() {
    String ppl = "source=EMP | grok ENAME '%{NUMBER} %{GREEDYDATA:ENAME}' | fields ENAME";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(ENAME=[ITEM(GROK($1, '%{NUMBER} %{GREEDYDATA:ENAME}':VARCHAR), 'ENAME')])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `GROK`(`ENAME`, '%{NUMBER} %{GREEDYDATA:ENAME}')['ENAME'] `ENAME`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }
}
