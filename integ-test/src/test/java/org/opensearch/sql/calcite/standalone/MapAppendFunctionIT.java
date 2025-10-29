/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.standalone;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.function.PPLFuncImpTable;

public class MapAppendFunctionIT extends CalcitePPLRelNodeIntegTestCase {

  private static final String MAP_FIELD = "map";
  private static final String ID_FIELD = "id";

  @Test
  public void testMapAppendWithNonOverlappingKeys() throws Exception {
    RexNode map1 = createMap("key1", "value1", "key2", "value2");
    RexNode map2 = createMap("key3", "value3", "key4", "value4");
    RexNode mapAppendCall =
        PPLFuncImpTable.INSTANCE.resolve(
            context.rexBuilder, BuiltinFunctionName.MAP_APPEND, map1, map2);
    RelNode relNode =
        context
            .relBuilder
            .values(new String[] {ID_FIELD}, 1)
            .project(context.relBuilder.alias(mapAppendCall, MAP_FIELD))
            .build();

    executeRelNodeAndVerify(
        context.planContext,
        relNode,
        resultSet -> {
          Map<String, Object> result = getResultMapField(resultSet);
          assertEquals(4, result.size());
          assertMapListValue(result, "key1", "value1");
          assertMapListValue(result, "key2", "value2");
          assertMapListValue(result, "key3", "value3");
          assertMapListValue(result, "key4", "value4");
        });
  }

  @Test
  public void testMapAppendWithOverlappingKeys() throws Exception {
    RexNode map1 = createMap("key1", "value1", "key2", "value2");
    RexNode map2 = createMap("key2", "value3", "key3", "value4");
    RexNode mapAppendCall =
        PPLFuncImpTable.INSTANCE.resolve(
            context.rexBuilder, BuiltinFunctionName.MAP_APPEND, map1, map2);
    RelNode relNode =
        context
            .relBuilder
            .values(new String[] {ID_FIELD}, 1)
            .project(context.relBuilder.alias(mapAppendCall, MAP_FIELD))
            .build();

    executeRelNodeAndVerify(
        context.planContext,
        relNode,
        resultSet -> {
          Map<String, Object> result = getResultMapField(resultSet);
          assertEquals(3, result.size());
          assertMapListValue(result, "key1", "value1");
          assertMapListValue(result, "key2", "value2", "value3");
          assertMapListValue(result, "key3", "value4");
        });
  }

  @Test
  public void testMapAppendWithSingleNull(RexNode map1, RexNode map2) throws Exception {
    RelDataType mapType = createMapType(context.rexBuilder);
    RexNode nullMap = context.rexBuilder.makeNullLiteral(mapType);
    RexNode map = createMap("key1", "value1");
    testWithSingleNull(map, nullMap);
    testWithSingleNull(nullMap, map);
  }

  private void testWithSingleNull(RexNode map1, RexNode map2) throws Exception {
    RexNode mapAppendCall =
        PPLFuncImpTable.INSTANCE.resolve(
            context.rexBuilder, BuiltinFunctionName.MAP_APPEND, map1, map2);
    RelNode relNode =
        context
            .relBuilder
            .values(new String[] {ID_FIELD}, 1)
            .project(context.relBuilder.alias(mapAppendCall, MAP_FIELD))
            .build();

    executeRelNodeAndVerify(
        context.planContext,
        relNode,
        resultSet -> {
          Map<String, Object> result = getResultMapField(resultSet);
          assertEquals(1, result.size());
          assertMapListValue(result, "key1", "value1");
        });
  }

  @Test
  public void testMapAppendWithNullMaps() throws Exception {
    RelDataType mapType = createMapType(context.rexBuilder);
    RexNode nullMap = context.rexBuilder.makeNullLiteral(mapType);
    RexNode mapAppendCall =
        PPLFuncImpTable.INSTANCE.resolve(
            context.rexBuilder, BuiltinFunctionName.MAP_APPEND, nullMap, nullMap);
    RelNode relNode =
        context
            .relBuilder
            .values(new String[] {ID_FIELD}, 1)
            .project(context.relBuilder.alias(mapAppendCall, MAP_FIELD))
            .build();

    executeRelNodeAndVerify(
        context.planContext,
        relNode,
        resultSet -> {
          assertNull(getResultMapField(resultSet));
        });
  }

  private RexNode createMap(String... keyValuePairs) {
    RexNode[] args = new RexNode[keyValuePairs.length];
    for (int i = 0; i < keyValuePairs.length; i++) {
      args[i] = context.rexBuilder.makeLiteral(keyValuePairs[i]);
    }

    return context.rexBuilder.makeCall(SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR, args);
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> getResultMapField(ResultSet resultSet) throws SQLException {
    assertTrue(resultSet.next());
    verifyColumns(resultSet, MAP_FIELD);
    Map<String, Object> result = (Map<String, Object>) resultSet.getObject(1);
    return result;
  }

  @SuppressWarnings("unchecked")
  private void assertMapListValue(Map<String, Object> map, String key, Object... expectedValues) {
    map.containsKey(key);
    Object value = map.get(key);
    assertTrue(value instanceof List);
    List<Object> list = (List<Object>) value;
    assertEquals(expectedValues.length, list.size());
    for (int i = 0; i < expectedValues.length; i++) {
      assertEquals(expectedValues[i], list.get(i));
    }
  }
}
