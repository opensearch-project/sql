/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.standalone;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.function.PPLFuncImpTable;

public class MapRemoveFunctionIT extends CalcitePPLRelNodeIntegTestCase {

  private static final String MAP_FIELD = "map";
  private static final String ID_FIELD = "id";

  @Test
  public void testMapRemoveWithNullMap() throws Exception {
    RelDataType mapType = createMapType(context.rexBuilder);
    RexNode nullMap = context.rexBuilder.makeNullLiteral(mapType);
    RexNode keysArray = createStringArray(context.rexBuilder, "key1", "key2");
    RexNode mapRemoveCall =
        PPLFuncImpTable.INSTANCE.resolve(
            context.rexBuilder, BuiltinFunctionName.MAP_REMOVE, nullMap, keysArray);
    RelNode relNode =
        context
            .relBuilder
            .values(new String[] {ID_FIELD}, 1)
            .project(context.relBuilder.alias(mapRemoveCall, MAP_FIELD))
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
  public void testMapRemoveWithNullKeys() throws Exception {
    RexNode map = getBaseMap();
    RelDataType arrayType = createStringArrayType(context.rexBuilder);
    RexNode nullKeys = context.rexBuilder.makeNullLiteral(arrayType);
    RexNode mapRemoveCall =
        PPLFuncImpTable.INSTANCE.resolve(
            context.rexBuilder, BuiltinFunctionName.MAP_REMOVE, map, nullKeys);
    RelNode relNode =
        context
            .relBuilder
            .values(new String[] {ID_FIELD}, 1)
            .project(context.relBuilder.alias(mapRemoveCall, MAP_FIELD))
            .build();

    executeRelNodeAndVerify(
        context.planContext,
        relNode,
        resultSet -> {
          Map<String, String> result = getResultMapField(resultSet);
          assertEquals(3, result.size());
          assertEquals("value1", result.get("key1"));
          assertEquals("value2", result.get("key2"));
          assertEquals("value3", result.get("key3"));
        });
  }

  @Test
  public void testMapRemoveExistingKeys() throws Exception {
    RexNode map = getBaseMap();
    RexNode keysArray = createStringArray(context.rexBuilder, "key1", "key3");
    RexNode mapRemoveCall =
        PPLFuncImpTable.INSTANCE.resolve(
            context.rexBuilder, BuiltinFunctionName.MAP_REMOVE, map, keysArray);
    RelNode relNode =
        context
            .relBuilder
            .values(new String[] {ID_FIELD}, 1)
            .project(context.relBuilder.alias(mapRemoveCall, MAP_FIELD))
            .build();

    executeRelNodeAndVerify(
        context.planContext,
        relNode,
        resultSet -> {
          Map<String, String> result = getResultMapField(resultSet);
          assertEquals(1, result.size());
          assertEquals("value2", result.get("key2"));
          assertNull(result.get("key1"));
          assertNull(result.get("key3"));
        });
  }

  @Test
  public void testMapRemoveNonExistingKeys() throws Exception {
    RexNode map = getBaseMap();
    RexNode keysArray = createStringArray(context.rexBuilder, "key4", "key5");
    RexNode mapRemoveCall =
        PPLFuncImpTable.INSTANCE.resolve(
            context.rexBuilder, BuiltinFunctionName.MAP_REMOVE, map, keysArray);
    RelNode relNode =
        context
            .relBuilder
            .values(new String[] {ID_FIELD}, 1)
            .project(context.relBuilder.alias(mapRemoveCall, MAP_FIELD))
            .build();

    executeRelNodeAndVerify(
        context.planContext,
        relNode,
        resultSet -> {
          Map<String, String> result = getResultMapField(resultSet);
          assertEquals(3, result.size());
          assertEquals("value1", result.get("key1"));
          assertEquals("value2", result.get("key2"));
          assertEquals("value3", result.get("key3"));
        });
  }

  @Test
  public void testMapRemoveEmptyKeysArray() throws Exception {
    RexNode map = getBaseMap();
    RelDataType arrayType = createStringArrayType(context.rexBuilder);
    RexNode nullKeys = context.rexBuilder.makeNullLiteral(arrayType);
    RexNode mapRemoveCall =
        PPLFuncImpTable.INSTANCE.resolve(
            context.rexBuilder, BuiltinFunctionName.MAP_REMOVE, map, nullKeys);

    RelNode relNode =
        context
            .relBuilder
            .values(new String[] {ID_FIELD}, 1)
            .project(context.relBuilder.alias(mapRemoveCall, MAP_FIELD))
            .build();

    executeRelNodeAndVerify(
        context.planContext,
        relNode,
        resultSet -> {
          Map<String, String> result = getResultMapField(resultSet);
          assertEquals(3, result.size());
          assertEquals("value1", result.get("key1"));
          assertEquals("value2", result.get("key2"));
          assertEquals("value3", result.get("key3"));
        });
  }

  private RexNode getBaseMap() {
    return context.rexBuilder.makeCall(
        SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
        context.rexBuilder.makeLiteral("key1"),
        context.rexBuilder.makeLiteral("value1"),
        context.rexBuilder.makeLiteral("key2"),
        context.rexBuilder.makeLiteral("value2"),
        context.rexBuilder.makeLiteral("key3"),
        context.rexBuilder.makeLiteral("value3"));
  }

  private Map<String, String> getResultMapField(ResultSet resultSet) throws SQLException {
    assertTrue(resultSet.next());
    verifyColumns(resultSet, MAP_FIELD);
    Map<String, String> result = (Map<String, String>) resultSet.getObject(1);
    return result;
  }
}
