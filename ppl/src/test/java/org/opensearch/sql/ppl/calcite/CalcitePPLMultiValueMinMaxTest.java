/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import java.util.List;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Programs;
import org.junit.Assert;
import org.junit.Test;

/** MIN/MAX over a multi_value (ARRAY) field aggregate over every element. */
public class CalcitePPLMultiValueMinMaxTest extends CalcitePPLAbstractTest {

  public CalcitePPLMultiValueMinMaxTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Override
  protected Frameworks.ConfigBuilder config(CalciteAssert.SchemaSpec... schemaSpecs) {
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    final SchemaPlus schema = CalciteAssert.addSchema(rootSchema, schemaSpecs);
    schema.add("DEPT", new CalcitePPLMvExpandTest.TableWithArray());
    return Frameworks.newConfigBuilder()
        .parserConfig(SqlParser.Config.DEFAULT)
        .defaultSchema(schema)
        .traitDefs((List<RelTraitDef>) null)
        .programs(Programs.heuristicJoinOrder(Programs.RULE_SET, true, 2));
  }

  @Test
  public void testMinMaxOverArrayReducesPerElement() {
    RelNode root = getRelNode("source=DEPT | stats max(EMPNOS) as mx, min(EMPNOS) as mn");
    String expectedLogical =
        "LogicalAggregate(group=[{}], mx=[MAX($0)], mn=[MIN($1)])\n"
            + "  LogicalProject($f2=[ARRAY_MAX($1)], $f3=[ARRAY_MIN($1)])\n"
            + "    LogicalTableScan(table=[[scott, DEPT]])\n";
    verifyLogical(root, expectedLogical);
    for (RelDataTypeField field : root.getRowType().getFieldList()) {
      Assert.assertEquals(field.toString(), SqlTypeName.INTEGER, field.getType().getSqlTypeName());
    }
  }

  @Test
  public void testMinMaxOverArrayByScalarKey() {
    RelNode root = getRelNode("source=DEPT | stats max(EMPNOS) as mx by DEPTNO");
    RelDataTypeField mx = root.getRowType().getField("mx", true, false);
    Assert.assertEquals(SqlTypeName.INTEGER, mx.getType().getSqlTypeName());
    Assert.assertTrue(root.explain(), root.explain().contains("ARRAY_MAX($1)"));
  }

  @Test
  public void testMinMaxOverScalarIsUnchanged() {
    RelNode root = getRelNode("source=DEPT | stats max(DEPTNO) as mx, min(DEPTNO) as mn");
    String expectedLogical =
        "LogicalAggregate(group=[{}], mx=[MAX($0)], mn=[MIN($0)])\n"
            + "  LogicalProject(DEPTNO=[$0])\n"
            + "    LogicalTableScan(table=[[scott, DEPT]])\n";
    verifyLogical(root, expectedLogical);
  }
}
