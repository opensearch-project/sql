/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.standalone;

import static org.hamcrest.Matchers.equalTo;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.function.PPLFuncImpTable;

public class AppendFunctionIT extends CalcitePPLRelNodeIntegTestCase {

  private static final String RESULT_FIELD = "result";
  private static final String ID_FIELD = "id";

  @Test
  public void testAppendWithNoArguments() throws Exception {
    RexNode appendCall =
        PPLFuncImpTable.INSTANCE.resolve(context.rexBuilder, BuiltinFunctionName.INTERNAL_APPEND);
    RelNode relNode =
        context
            .relBuilder
            .values(new String[] {ID_FIELD}, 1)
            .project(context.relBuilder.alias(appendCall, RESULT_FIELD))
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
  public void testAppendWithSingleElement() throws Exception {
    RexNode value = context.rexBuilder.makeExactLiteral(java.math.BigDecimal.valueOf(42));
    RexNode appendCall =
        PPLFuncImpTable.INSTANCE.resolve(
            context.rexBuilder, BuiltinFunctionName.INTERNAL_APPEND, value);
    RelNode relNode =
        context
            .relBuilder
            .values(new String[] {ID_FIELD}, 1)
            .project(context.relBuilder.alias(appendCall, RESULT_FIELD))
            .build();

    executeRelNodeAndVerify(
        context.planContext,
        relNode,
        resultSet -> {
          assertTrue(resultSet.next());
          verifyColumns(resultSet, RESULT_FIELD);
          assertEquals(42, resultSet.getObject(1));
        });
  }

  @Test
  public void testAppendWithMultipleElements() throws Exception {
    RexNode value1 = context.rexBuilder.makeExactLiteral(java.math.BigDecimal.valueOf(1));
    RexNode value2 = context.rexBuilder.makeExactLiteral(java.math.BigDecimal.valueOf(2));
    RexNode value3 = context.rexBuilder.makeExactLiteral(java.math.BigDecimal.valueOf(3));
    RexNode appendCall =
        PPLFuncImpTable.INSTANCE.resolve(
            context.rexBuilder, BuiltinFunctionName.INTERNAL_APPEND, value1, value2, value3);
    RelNode relNode =
        context
            .relBuilder
            .values(new String[] {ID_FIELD}, 1)
            .project(context.relBuilder.alias(appendCall, RESULT_FIELD))
            .build();

    executeRelNodeAndVerify(
        context.planContext,
        relNode,
        resultSet -> {
          assertTrue(resultSet.next());
          verifyColumns(resultSet, RESULT_FIELD);
          List<?> result = getResultList(resultSet);
          assertThat(result, equalTo(List.of(1, 2, 3)));
        });
  }

  @Test
  public void testAppendWithArrayFlattening() throws Exception {
    RexNode array1 = createStringArray(context.rexBuilder, "a", "b");
    RexNode array2 = createStringArray(context.rexBuilder, "c", "d");
    RexNode appendCall =
        PPLFuncImpTable.INSTANCE.resolve(
            context.rexBuilder, BuiltinFunctionName.INTERNAL_APPEND, array1, array2);
    RelNode relNode =
        context
            .relBuilder
            .values(new String[] {ID_FIELD}, 1)
            .project(context.relBuilder.alias(appendCall, RESULT_FIELD))
            .build();

    executeRelNodeAndVerify(
        context.planContext,
        relNode,
        resultSet -> {
          assertTrue(resultSet.next());
          verifyColumns(resultSet, RESULT_FIELD);
          List<?> result = getResultList(resultSet);
          assertThat(result, equalTo(List.of("a", "b", "c", "d")));
        });
  }

  @Test
  public void testAppendWithMixedTypes() throws Exception {
    RexNode array = createStringArray(context.rexBuilder, "a", "b");
    RexNode number = context.rexBuilder.makeExactLiteral(java.math.BigDecimal.valueOf(42));
    RexNode string = context.rexBuilder.makeLiteral("hello");
    RexNode appendCall =
        PPLFuncImpTable.INSTANCE.resolve(
            context.rexBuilder, BuiltinFunctionName.INTERNAL_APPEND, array, number, string);
    RelNode relNode =
        context
            .relBuilder
            .values(new String[] {ID_FIELD}, 1)
            .project(context.relBuilder.alias(appendCall, RESULT_FIELD))
            .build();

    executeRelNodeAndVerify(
        context.planContext,
        relNode,
        resultSet -> {
          assertTrue(resultSet.next());
          verifyColumns(resultSet, RESULT_FIELD);
          List<?> result = getResultList(resultSet);
          assertThat(result, equalTo(List.of("a", "b", 42, "hello")));
        });
  }

  @Test
  public void testAppendWithSingleString() throws Exception {
    RexNode value = context.rexBuilder.makeLiteral("test");
    RexNode appendCall =
        PPLFuncImpTable.INSTANCE.resolve(
            context.rexBuilder, BuiltinFunctionName.INTERNAL_APPEND, value);
    RelNode relNode =
        context
            .relBuilder
            .values(new String[] {ID_FIELD}, 1)
            .project(context.relBuilder.alias(appendCall, RESULT_FIELD))
            .build();

    executeRelNodeAndVerify(
        context.planContext,
        relNode,
        resultSet -> {
          assertTrue(resultSet.next());
          verifyColumns(resultSet, RESULT_FIELD);
          assertEquals("test", resultSet.getObject(1));
        });
  }

  @Test
  public void testAppendWithMultipleStrings() throws Exception {
    RexNode value1 = context.rexBuilder.makeLiteral("hello");
    RexNode value2 = context.rexBuilder.makeLiteral("world");
    RexNode appendCall =
        PPLFuncImpTable.INSTANCE.resolve(
            context.rexBuilder, BuiltinFunctionName.INTERNAL_APPEND, value1, value2);
    RelNode relNode =
        context
            .relBuilder
            .values(new String[] {ID_FIELD}, 1)
            .project(context.relBuilder.alias(appendCall, RESULT_FIELD))
            .build();

    executeRelNodeAndVerify(
        context.planContext,
        relNode,
        resultSet -> {
          assertTrue(resultSet.next());
          verifyColumns(resultSet, RESULT_FIELD);
          List<?> result = getResultList(resultSet);
          assertThat(result, equalTo(List.of("hello", "world")));
        });
  }

  private List<?> getResultList(ResultSet resultSet) throws SQLException {
    Object result = resultSet.getObject(1);
    assertTrue(result instanceof List);
    return (List<?>) result;
  }
}
