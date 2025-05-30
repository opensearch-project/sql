/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.TYPE_FACTORY;

import java.sql.Connection;
import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.ArraySqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.RelBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.ast.expression.LambdaFunction;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.calcite.utils.CalciteToolsHelper;
import org.opensearch.sql.executor.QueryType;

@ExtendWith(MockitoExtension.class)
public class CalciteRexNodeVisitorTest {
  @Mock LambdaFunction lambdaFunction;
  @Mock RexNode arrayArg;
  @Mock RexNode extraArg;
  @Mock RexNode accArg;
  ;
  @Mock ArraySqlType arraySqlType;
  @Mock RelDataType componentType;
  @Mock RelDataType extraType;
  @Mock RelDataType accType;
  @Mock QualifiedName functionArg1;
  @Mock QualifiedName functionArg2;

  static CalciteRexNodeVisitor visitor;
  static CalciteRelNodeVisitor relNodeVisitor;

  @Mock static FrameworkConfig frameworkConfig;
  @Mock static Connection connection;
  @Mock static RelBuilder relBuilder;
  @Mock static ExtendedRexBuilder rexBuilder;
  static CalcitePlanContext context;
  MockedStatic<CalciteToolsHelper> mockedStatic;

  @BeforeEach
  public void setUpContext() {
    relNodeVisitor = new CalciteRelNodeVisitor();
    visitor = new CalciteRexNodeVisitor(relNodeVisitor);
    when(relBuilder.getRexBuilder()).thenReturn(rexBuilder);
    when(rexBuilder.getTypeFactory()).thenReturn(TYPE_FACTORY);
    mockedStatic = Mockito.mockStatic(CalciteToolsHelper.class);
    mockedStatic.when(() -> CalciteToolsHelper.connect(any(), any())).thenReturn(connection);

    mockedStatic.when(() -> CalciteToolsHelper.create(any(), any(), any())).thenReturn(relBuilder);

    context = CalcitePlanContext.create(frameworkConfig, 100, QueryType.PPL);
  }

  @AfterEach
  public void tearDown() {
    mockedStatic.close();
  }

  @Test
  public void testPrepareLambdaForBasicLambda() {
    when(componentType.getSqlTypeName()).thenReturn(SqlTypeName.DOUBLE);
    when(arrayArg.getType()).thenReturn(arraySqlType);
    when(arraySqlType.getComponentType()).thenReturn(componentType);

    List<RexNode> previousArguments = List.of(arrayArg);
    when(functionArg1.toString()).thenReturn("arg1");
    when(lambdaFunction.getFuncArgs()).thenReturn(List.of(functionArg1));

    CalcitePlanContext lambdaContext =
        visitor.prepareLambdaContext(context, lambdaFunction, previousArguments, "forall");

    assertNotNull(lambdaContext);
    assertNotNull(lambdaContext.getRexLambdaRefMap());
    assertEquals(1, lambdaContext.getRexLambdaRefMap().size());
    assertTrue(lambdaContext.getRexLambdaRefMap().containsKey("arg1"));
    assertEquals(
        lambdaContext.getRexLambdaRefMap().get("arg1").getType().getSqlTypeName(),
        SqlTypeName.DOUBLE);
  }

  @Test
  public void testPrepareLambdaForTransform() {
    when(componentType.getSqlTypeName()).thenReturn(SqlTypeName.DOUBLE);
    when(arrayArg.getType()).thenReturn(arraySqlType);
    when(arraySqlType.getComponentType()).thenReturn(componentType);

    List<RexNode> previousArguments = List.of(arrayArg);
    when(functionArg1.toString()).thenReturn("arg1");
    when(functionArg2.toString()).thenReturn("i");
    when(lambdaFunction.getFuncArgs()).thenReturn(List.of(functionArg1, functionArg2));

    CalcitePlanContext lambdaContext =
        visitor.prepareLambdaContext(context, lambdaFunction, previousArguments, "transform");

    assertNotNull(lambdaContext);
    assertNotNull(lambdaContext.getRexLambdaRefMap());
    assertEquals(2, lambdaContext.getRexLambdaRefMap().size());
    assertTrue(lambdaContext.getRexLambdaRefMap().containsKey("arg1"));
    assertTrue(lambdaContext.getRexLambdaRefMap().containsKey("i"));
    assertEquals(
        lambdaContext.getRexLambdaRefMap().get("arg1").getType().getSqlTypeName(),
        SqlTypeName.DOUBLE);
    assertEquals(
        lambdaContext.getRexLambdaRefMap().get("i").getType().getSqlTypeName(),
        SqlTypeName.INTEGER);
  }

  @Test
  public void testPrepareLambdaForReduce() {
    when(componentType.getSqlTypeName()).thenReturn(SqlTypeName.DOUBLE);
    when(arrayArg.getType()).thenReturn(arraySqlType);
    when(arraySqlType.getComponentType()).thenReturn(componentType);
    when(extraArg.getType()).thenReturn(extraType);
    when(extraType.getSqlTypeName()).thenReturn(SqlTypeName.VARCHAR);

    List<RexNode> previousArguments = List.of(arrayArg, extraArg);
    when(functionArg1.toString()).thenReturn("acc");
    when(functionArg2.toString()).thenReturn("arg1");
    when(lambdaFunction.getFuncArgs()).thenReturn(List.of(functionArg1, functionArg2));

    CalcitePlanContext lambdaContext =
        visitor.prepareLambdaContext(context, lambdaFunction, previousArguments, "reduce");

    assertNotNull(lambdaContext);
    assertNotNull(lambdaContext.getRexLambdaRefMap());
    assertEquals(2, lambdaContext.getRexLambdaRefMap().size());
    assertTrue(lambdaContext.getRexLambdaRefMap().containsKey("arg1"));
    assertTrue(lambdaContext.getRexLambdaRefMap().containsKey("acc"));
    assertEquals(
        lambdaContext.getRexLambdaRefMap().get("arg1").getType().getSqlTypeName(),
        SqlTypeName.DOUBLE);
    assertEquals(
        lambdaContext.getRexLambdaRefMap().get("acc").getType().getSqlTypeName(),
        SqlTypeName.VARCHAR);
  }

  @Test
  public void testPrepareLambdaForReduceFinalizerFunction() {
    when(arrayArg.getType()).thenReturn(arraySqlType);
    when(arraySqlType.getComponentType()).thenReturn(componentType);
    when(extraArg.getType()).thenReturn(extraType);
    when(accArg.getType()).thenReturn(accType);
    when(accType.getSqlTypeName()).thenReturn(SqlTypeName.FLOAT);

    List<RexNode> previousArguments = List.of(arrayArg, extraArg, accArg);
    when(functionArg1.toString()).thenReturn("acc");
    when(lambdaFunction.getFuncArgs()).thenReturn(List.of(functionArg1));

    CalcitePlanContext lambdaContext =
        visitor.prepareLambdaContext(context, lambdaFunction, previousArguments, "reduce");

    assertNotNull(lambdaContext);
    assertNotNull(lambdaContext.getRexLambdaRefMap());
    assertEquals(1, lambdaContext.getRexLambdaRefMap().size());
    assertTrue(lambdaContext.getRexLambdaRefMap().containsKey("acc"));
    assertEquals(
        lambdaContext.getRexLambdaRefMap().get("acc").getType().getSqlTypeName(),
        SqlTypeName.FLOAT);
  }
}
