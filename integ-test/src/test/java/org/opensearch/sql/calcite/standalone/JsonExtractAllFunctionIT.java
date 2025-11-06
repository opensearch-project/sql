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
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.function.PPLFuncImpTable;

public class JsonExtractAllFunctionIT extends CalcitePPLRelNodeIntegTestCase {

  private static final String RESULT_FIELD = "result";
  private static final String ID_FIELD = "id";

  @Test
  public void testJsonExtractAllWithNullInput() throws Exception {
    RelDataType stringType = context.rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR);
    RexNode nullJson = context.rexBuilder.makeNullLiteral(stringType);

    RexNode jsonExtractAllCall =
        PPLFuncImpTable.INSTANCE.resolve(
            context.rexBuilder, BuiltinFunctionName.JSON_EXTRACT_ALL, nullJson);

    RelNode relNode =
        context
            .relBuilder
            .values(new String[] {ID_FIELD}, 1)
            .project(context.relBuilder.alias(jsonExtractAllCall, RESULT_FIELD))
            .build();

    executeRelNodeAndVerify(
        context.planContext,
        relNode,
        resultSet -> {
          assertTrue(resultSet.next());
          verifyColumns(resultSet, RESULT_FIELD);
          assertNull(resultSet.getObject(1));
        });
  }

  @Test
  public void testJsonExtractAllWithSimpleObject() throws Exception {
    String jsonString = "{\"name\": \"John\", \"age\": 30}";
    RexNode jsonLiteral = context.rexBuilder.makeLiteral(jsonString);

    RexNode jsonExtractAllCall =
        PPLFuncImpTable.INSTANCE.resolve(
            context.rexBuilder, BuiltinFunctionName.JSON_EXTRACT_ALL, jsonLiteral);

    RelNode relNode =
        context
            .relBuilder
            .values(new String[] {ID_FIELD}, 1)
            .project(context.relBuilder.alias(jsonExtractAllCall, RESULT_FIELD))
            .build();

    executeRelNodeAndVerify(
        context.planContext,
        relNode,
        resultSet -> {
          assertTrue(resultSet.next());
          verifyColumns(resultSet, RESULT_FIELD);

          Map<String, Object> map = getMap(resultSet, 1);
          assertEquals("John", map.get("name"));
          assertEquals(30, map.get("age"));
          assertEquals(2, map.size());
        });
  }

  private Map<String, Object> getMap(ResultSet resultSet, int columnIndex) throws SQLException {
    Object result = resultSet.getObject(columnIndex);
    assertNotNull(result);
    assertTrue(result instanceof Map);

    return (Map<String, Object>) result;
  }

  @Test
  public void testJsonExtractAllWithNestedObject() throws Exception {
    String jsonString = "{\"user\": {\"name\": \"John\", \"age\": 30}, \"active\": true}";
    RexNode jsonLiteral = context.rexBuilder.makeLiteral(jsonString);

    RexNode jsonExtractAllCall =
        PPLFuncImpTable.INSTANCE.resolve(
            context.rexBuilder, BuiltinFunctionName.JSON_EXTRACT_ALL, jsonLiteral);

    RelNode relNode =
        context
            .relBuilder
            .values(new String[] {ID_FIELD}, 1)
            .project(context.relBuilder.alias(jsonExtractAllCall, RESULT_FIELD))
            .build();

    executeRelNodeAndVerify(
        context.planContext,
        relNode,
        resultSet -> {
          assertTrue(resultSet.next());
          verifyColumns(resultSet, RESULT_FIELD);

          Map<String, Object> map = getMap(resultSet, 1);
          assertEquals("John", map.get("user.name"));
          assertEquals(30, map.get("user.age"));
          assertEquals(true, map.get("active"));
          assertEquals(3, map.size());
        });
  }

  @Test
  public void testJsonExtractAllWithArray() throws Exception {
    String jsonString = "{\"tags\": [\"java\", \"sql\", \"opensearch\"]}";
    RexNode jsonLiteral = context.rexBuilder.makeLiteral(jsonString);

    RexNode jsonExtractAllCall =
        PPLFuncImpTable.INSTANCE.resolve(
            context.rexBuilder, BuiltinFunctionName.JSON_EXTRACT_ALL, jsonLiteral);

    RelNode relNode =
        context
            .relBuilder
            .values(new String[] {ID_FIELD}, 1)
            .project(context.relBuilder.alias(jsonExtractAllCall, RESULT_FIELD))
            .build();

    executeRelNodeAndVerify(
        context.planContext,
        relNode,
        resultSet -> {
          assertTrue(resultSet.next());
          verifyColumns(resultSet, RESULT_FIELD);

          Map<String, Object> map = getMap(resultSet, 1);
          List<Object> tags = getList(map, "tags{}");

          assertEquals(3, tags.size());
          assertEquals("java", tags.get(0));
          assertEquals("sql", tags.get(1));
          assertEquals("opensearch", tags.get(2));
        });
  }

  @Test
  public void testJsonExtractAllWithArrayOfObjects() throws Exception {
    String jsonString = "{\"users\": [{\"name\": \"John\"}, {\"name\": \"Jane\"}]}";
    RexNode jsonLiteral = context.rexBuilder.makeLiteral(jsonString);

    RexNode jsonExtractAllCall =
        PPLFuncImpTable.INSTANCE.resolve(
            context.rexBuilder, BuiltinFunctionName.JSON_EXTRACT_ALL, jsonLiteral);

    RelNode relNode =
        context
            .relBuilder
            .values(new String[] {ID_FIELD}, 1)
            .project(context.relBuilder.alias(jsonExtractAllCall, RESULT_FIELD))
            .build();

    executeRelNodeAndVerify(
        context.planContext,
        relNode,
        resultSet -> {
          assertTrue(resultSet.next());
          verifyColumns(resultSet, RESULT_FIELD);

          Map<String, Object> map = getMap(resultSet, 1);
          List<Object> names = getList(map, "users{}.name");
          assertEquals(2, names.size());
          assertEquals("John", names.get(0));
          assertEquals("Jane", names.get(1));
          assertEquals(1, map.size()); // Only flattened key should exist
        });
  }

  @Test
  public void testJsonExtractAllWithTopLevelArray() throws Exception {
    String jsonString = "[{\"id\": 1}, {\"id\": 2}]";
    RexNode jsonLiteral = context.rexBuilder.makeLiteral(jsonString);

    RexNode jsonExtractAllCall =
        PPLFuncImpTable.INSTANCE.resolve(
            context.rexBuilder, BuiltinFunctionName.JSON_EXTRACT_ALL, jsonLiteral);

    RelNode relNode =
        context
            .relBuilder
            .values(new String[] {ID_FIELD}, 1)
            .project(context.relBuilder.alias(jsonExtractAllCall, RESULT_FIELD))
            .build();

    executeRelNodeAndVerify(
        context.planContext,
        relNode,
        resultSet -> {
          assertTrue(resultSet.next());
          verifyColumns(resultSet, RESULT_FIELD);

          Map<String, Object> map = getMap(resultSet, 1);
          List<Object> ids = getList(map, "{}.id");
          assertEquals(2, ids.size());
          assertEquals(1, ids.get(0));
          assertEquals(2, ids.get(1));
          assertEquals(1, map.size());
        });
  }

  @SuppressWarnings("unchecked")
  private List<Object> getList(Map<String, Object> map, String key) {
    Object value = map.get(key);
    assertNotNull(value);
    assertTrue(value instanceof List);

    return (List<Object>) value;
  }

  @Test
  public void testJsonExtractAllWithEmptyObject() throws Exception {
    String jsonString = "{}";
    RexNode jsonLiteral = context.rexBuilder.makeLiteral(jsonString);

    RexNode jsonExtractAllCall =
        PPLFuncImpTable.INSTANCE.resolve(
            context.rexBuilder, BuiltinFunctionName.JSON_EXTRACT_ALL, jsonLiteral);

    RelNode relNode =
        context
            .relBuilder
            .values(new String[] {ID_FIELD}, 1)
            .project(context.relBuilder.alias(jsonExtractAllCall, RESULT_FIELD))
            .build();

    executeRelNodeAndVerify(
        context.planContext,
        relNode,
        resultSet -> {
          assertTrue(resultSet.next());
          verifyColumns(resultSet, RESULT_FIELD);

          Map<String, Object> map = getMap(resultSet, 1);
          assertTrue(map.isEmpty());
        });
  }

  @Test
  public void testJsonExtractAllWithInvalidJson() throws Exception {
    String invalidJsonString = "{\"name\": \"John\", \"age\":}";
    RexNode jsonLiteral = context.rexBuilder.makeLiteral(invalidJsonString);

    RexNode jsonExtractAllCall =
        PPLFuncImpTable.INSTANCE.resolve(
            context.rexBuilder, BuiltinFunctionName.JSON_EXTRACT_ALL, jsonLiteral);

    RelNode relNode =
        context
            .relBuilder
            .values(new String[] {ID_FIELD}, 1)
            .project(context.relBuilder.alias(jsonExtractAllCall, RESULT_FIELD))
            .build();

    executeRelNodeAndVerify(
        context.planContext,
        relNode,
        resultSet -> {
          assertTrue(resultSet.next());
          verifyColumns(resultSet, RESULT_FIELD);

          Map<String, Object> map = getMap(resultSet, 1);
          assertEquals("John", map.get("name"));
          assertEquals(1, map.size());
        });
  }
}
