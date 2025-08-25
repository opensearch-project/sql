/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.test.CalciteAssert;

public abstract class CalcitePPLAggregationTestBase extends CalcitePPLAbstractTest {

  public CalcitePPLAggregationTestBase() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  public CalcitePPLAggregationTestBase(CalciteAssert.SchemaSpec... schemaSpecs) {
    super(schemaSpecs);
  }

  protected void testAggregationWithAlias(
      String functionCall,
      String alias,
      String expectedLogicalPattern,
      String expectedResult,
      String expectedSparkSql) {
    String ppl = "source=EMP | stats " + functionCall + " as " + alias;
    RelNode root = getRelNode(ppl);
    verifyLogical(root, expectedLogicalPattern);
    verifyResult(root, expectedResult);
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  protected String createSimpleAggLogicalPattern(String aggFunction, String fieldProjection) {
    return "LogicalAggregate(group=[{}], "
        + aggFunction
        + ")\n"
        + "  "
        + fieldProjection
        + "\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n";
  }

  protected String createSimpleAggSparkSql(String selectClause) {
    return "SELECT " + selectClause + "\n" + "FROM `scott`.`EMP`";
  }
}
