/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.TYPE_FACTORY;

import java.sql.Connection;
import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.RelBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.SysLimit;
import org.opensearch.sql.calcite.utils.CalciteToolsHelper;
import org.opensearch.sql.calcite.utils.CalciteToolsHelper.OpenSearchRelBuilder;
import org.opensearch.sql.executor.QueryType;
import org.opensearch.sql.expression.function.PPLFuncImpTable.AggHandler;

/**
 * MIN/MAX over a multi_value (ARRAY) field aggregate over every element: the argument is reduced
 * per row with ARRAY_MIN/ARRAY_MAX, so the call is typed as the element. Scalar fields are passed
 * through unchanged.
 */
@ExtendWith(MockitoExtension.class)
class PPLFuncImpTableMinMaxTest extends AggFunctionTestBase {

  @Mock private FrameworkConfig frameworkConfig;
  @Mock private Connection connection;
  @Mock private OpenSearchRelBuilder relBuilder;
  @Mock private RelBuilder.AggCall aggCall;
  @Mock private RexNode reduced;

  private final RexBuilder rexBuilder = new RexBuilder(TYPE_FACTORY);
  private MockedStatic<CalciteToolsHelper> toolsHelper;
  private CalcitePlanContext context;

  @BeforeEach
  void setUp() {
    lenient().when(relBuilder.getRexBuilder()).thenReturn(rexBuilder);
    toolsHelper = mockStatic(CalciteToolsHelper.class);
    toolsHelper.when(() -> CalciteToolsHelper.connect(any(), any())).thenReturn(connection);
    toolsHelper.when(() -> CalciteToolsHelper.create(any(), any(), any())).thenReturn(relBuilder);
    context = CalcitePlanContext.create(frameworkConfig, SysLimit.DEFAULT, QueryType.PPL);
  }

  @AfterEach
  void tearDown() {
    toolsHelper.close();
  }

  private AggHandler handler(BuiltinFunctionName name) {
    return getAggFunctionRegistry().get(name).getRight();
  }

  private RexNode field(RelDataType type) {
    return rexBuilder.makeInputRef(type, 0);
  }

  private RelDataType intArray() {
    return TYPE_FACTORY.createArrayType(TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER), -1);
  }

  private void assertReducedThenAggregated(
      BuiltinFunctionName name, SqlOperator reduction, SqlAggFunction aggregate) {
    RexNode codes = field(intArray());
    when(relBuilder.call(eq(reduction), eq(codes))).thenReturn(reduced);
    when(relBuilder.aggregateCall(eq(aggregate), eq(List.of(reduced)))).thenReturn(aggCall);

    assertSame(aggCall, handler(name).apply(false, codes, List.of(), context));
  }

  @Test
  void maxOverArrayReducesWithArrayMax() {
    assertReducedThenAggregated(
        BuiltinFunctionName.MAX, SqlLibraryOperators.ARRAY_MAX, SqlStdOperatorTable.MAX);
  }

  @Test
  void minOverArrayReducesWithArrayMin() {
    assertReducedThenAggregated(
        BuiltinFunctionName.MIN, SqlLibraryOperators.ARRAY_MIN, SqlStdOperatorTable.MIN);
  }

  @Test
  void maxOverScalarIsNotReduced() {
    RexNode id = field(TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER));
    when(relBuilder.aggregateCall(eq(SqlStdOperatorTable.MAX), eq(List.of(id))))
        .thenReturn(aggCall);

    assertSame(aggCall, handler(BuiltinFunctionName.MAX).apply(false, id, List.of(), context));
    verify(relBuilder, never()).call(any(SqlOperator.class), any(RexNode.class));
  }
}
