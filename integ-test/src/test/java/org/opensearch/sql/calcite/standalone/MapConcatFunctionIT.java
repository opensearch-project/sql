/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.standalone;

import java.util.Map;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.function.PPLFuncImpTable;

public class MapConcatFunctionIT extends CalcitePPLRelNodeIntegTestCase {

  private static final String MAP_FIELD = "map";
  private static final String ID_FIELD = "id";

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
}
