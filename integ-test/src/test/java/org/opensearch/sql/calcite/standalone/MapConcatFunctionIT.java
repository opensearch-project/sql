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
import java.util.Map;
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
import org.junit.jupiter.api.Test;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.utils.CalciteToolsHelper.OpenSearchRelRunners;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.executor.QueryType;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.function.PPLFuncImpTable;

public class MapConcatFunctionIT extends CalcitePPLIntegTestCase {

  private static final String MAP_FIELD = "map";
  private static final String ID_FIELD = "id";
  TestContext context;

  @Override
  public void init() throws IOException {
    super.init();
    context = createTestContext();
    enableCalcite();
  }

  @Test
  public void testMapConcatWithNullValues() throws Exception {
    RelDataType mapType = createMapType(context.rexBuilder);
    RexNode map1 = context.rexBuilder.makeNullLiteral(mapType);
    RexNode map2 = context.rexBuilder.makeNullLiteral(mapType);

    RexNode mapConcatCall =
        PPLFuncImpTable.INSTANCE.resolve(
            context.rexBuilder, BuiltinFunctionName.MAP_CONCAT, map1, map2);

    RelNode relNode =
        context
            .relBuilder
            .values(new String[] {ID_FIELD}, 1)
            .project(context.relBuilder.alias(mapConcatCall, MAP_FIELD))
            .build();

    executeRelNodeAndVerify(
        context.planContext,
        relNode,
        resultSet -> {
          assertTrue(resultSet.next());
          verifyColumns(resultSet, MAP_FIELD);
          assertNull(resultSet.getObject(1));
        });
  }

  @Test
  public void testMapConcat() throws Exception {
    RexNode map1 =
        context.rexBuilder.makeCall(
            SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
            context.rexBuilder.makeLiteral("key1"),
            context.rexBuilder.makeLiteral("value1"),
            context.rexBuilder.makeLiteral("key2"),
            context.rexBuilder.makeLiteral("value2"));
    RexNode map2 =
        context.rexBuilder.makeCall(
            SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
            context.rexBuilder.makeLiteral("key2"),
            context.rexBuilder.makeLiteral("updated_value2"),
            context.rexBuilder.makeLiteral("key3"),
            context.rexBuilder.makeLiteral("value3"));

    RexNode mapConcatCall =
        PPLFuncImpTable.INSTANCE.resolve(
            context.rexBuilder, BuiltinFunctionName.MAP_CONCAT, map1, map2);

    RelNode relNode =
        context
            .relBuilder
            .values(new String[] {ID_FIELD}, 1)
            .project(context.relBuilder.alias(mapConcatCall, MAP_FIELD))
            .build();

    executeRelNodeAndVerify(
        context.planContext,
        relNode,
        resultSet -> {
          assertTrue(resultSet.next());
          verifyColumns(resultSet, MAP_FIELD);
          Map<String, String> result = (Map<String, String>) resultSet.getObject(1);
          assertEquals("value1", result.get("key1"));
          assertEquals("updated_value2", result.get("key2"));
          assertEquals("value3", result.get("key3"));
        });
  }

  private static class TestContext {
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
  private interface ResultVerifier {
    void verify(ResultSet resultSet) throws SQLException;
  }

  private TestContext createTestContext() {
    CalcitePlanContext planContext = createCalcitePlanContext();
    return new TestContext(planContext, planContext.relBuilder, planContext.rexBuilder);
  }

  private RelDataType createMapType(RexBuilder rexBuilder) {
    RelDataType stringType = rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR);
    RelDataType anyType = rexBuilder.getTypeFactory().createSqlType(SqlTypeName.ANY);
    return rexBuilder.getTypeFactory().createMapType(stringType, anyType);
  }

  private void executeRelNodeAndVerify(
      CalcitePlanContext planContext, RelNode relNode, ResultVerifier verifier)
      throws SQLException {
    try (PreparedStatement statement = OpenSearchRelRunners.run(planContext, relNode)) {
      ResultSet resultSet = statement.executeQuery();
      verifier.verify(resultSet);
    }
  }

  private void verifyColumns(ResultSet resultSet, String... expectedColumnNames)
      throws SQLException {
    assertEquals(expectedColumnNames.length, resultSet.getMetaData().getColumnCount());

    for (int i = 0; i < expectedColumnNames.length; i++) {
      String expectedName = expectedColumnNames[i];
      String actualName = resultSet.getMetaData().getColumnName(i + 1);
      assertEquals(expectedName, actualName);
    }
  }

  private CalcitePlanContext createCalcitePlanContext() {
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
