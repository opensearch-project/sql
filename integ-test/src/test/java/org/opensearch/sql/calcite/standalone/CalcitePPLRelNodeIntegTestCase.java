/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.standalone;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelBuilder;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.utils.CalciteToolsHelper.OpenSearchRelRunners;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.executor.QueryType;

/** Base class for integration test based on RelNode tree. Mainly for testing internal functions */
public abstract class CalcitePPLRelNodeIntegTestCase extends CalcitePPLIntegTestCase {
  TestContext context;

  @Override
  public void init() throws IOException {
    super.init();
    context = createTestContext();
    enableCalcite();
  }

  protected static class TestContext {
    final CalcitePlanContext planContext;
    final RelBuilder relBuilder;
    final RexBuilder rexBuilder;

    TestContext(CalcitePlanContext planContext, RelBuilder relBuilder, RexBuilder rexBuilder) {
      this.planContext = planContext;
      this.relBuilder = relBuilder;
      this.rexBuilder = rexBuilder;
    }
  }

  @FunctionalInterface
  protected interface ResultVerifier {
    void verify(ResultSet resultSet) throws SQLException;
  }

  protected TestContext createTestContext() {
    CalcitePlanContext planContext = createCalcitePlanContext();
    return new TestContext(planContext, planContext.relBuilder, planContext.rexBuilder);
  }

  protected RelDataType createMapType(RexBuilder rexBuilder) {
    RelDataType stringType = rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR);
    RelDataType anyType = rexBuilder.getTypeFactory().createSqlType(SqlTypeName.ANY);
    return rexBuilder.getTypeFactory().createMapType(stringType, anyType);
  }

  protected RelDataType createStringArrayType(RexBuilder rexBuilder) {
    RelDataType stringType = rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR);
    return rexBuilder.getTypeFactory().createArrayType(stringType, -1);
  }

  protected RexNode createStringArray(RexBuilder rexBuilder, String... values) {
    RelDataType stringType = rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR);
    RelDataType arrayType = rexBuilder.getTypeFactory().createArrayType(stringType, -1);

    List<RexNode> elements = new java.util.ArrayList<>();
    for (String value : values) {
      elements.add(rexBuilder.makeLiteral(value));
    }

    return rexBuilder.makeCall(arrayType, SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR, elements);
  }

  protected void executeRelNodeAndVerify(
      CalcitePlanContext planContext, RelNode relNode, ResultVerifier verifier)
      throws SQLException {
    try (PreparedStatement statement = OpenSearchRelRunners.run(planContext, relNode)) {
      ResultSet resultSet = statement.executeQuery();
      verifier.verify(resultSet);
    }
  }

  protected void verifyColumns(ResultSet resultSet, String... expectedColumnNames)
      throws SQLException {
    assertEquals(expectedColumnNames.length, resultSet.getMetaData().getColumnCount());

    for (int i = 0; i < expectedColumnNames.length; i++) {
      String expectedName = expectedColumnNames[i];
      String actualName = resultSet.getMetaData().getColumnName(i + 1);
      assertEquals(expectedName, actualName);
    }
  }

  protected CalcitePlanContext createCalcitePlanContext() {
    // Create a Frameworks.ConfigBuilder similar to CalcitePPLAbstractTest
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    Frameworks.ConfigBuilder config =
        Frameworks.newConfigBuilder()
            .parserConfig(SqlParser.Config.DEFAULT)
            .defaultSchema(rootSchema)
            .traitDefs((List<RelTraitDef>) null)
            .programs(Programs.heuristicJoinOrder(Programs.RULE_SET, true, 2));

    config.context(Contexts.of(RelBuilder.Config.DEFAULT));

    Settings settings = getSettings();
    return CalcitePlanContext.create(
        config.build(), settings.getSettingValue(Settings.Key.QUERY_SIZE_LIMIT), QueryType.PPL);
  }
}
