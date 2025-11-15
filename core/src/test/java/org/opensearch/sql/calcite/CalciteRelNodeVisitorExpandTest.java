/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.TYPE_FACTORY;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.ArraySqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.RelBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.calcite.utils.CalciteToolsHelper;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.executor.QueryType;

/** Negative tests for expand branch validations. */
@ExtendWith(MockitoExtension.class)
public class CalciteRelNodeVisitorExpandTest {

  private MockedStatic<CalciteToolsHelper> mockedCalciteToolsHelper;

  @SuppressWarnings("unused")
  private FrameworkConfig frameworkConfig = mock(FrameworkConfig.class);

  private final RelBuilder relBuilder = mock(RelBuilder.class);
  private final RelNode leftRelNode = mock(RelNode.class);
  private final RelDataType leftRowType = mock(RelDataType.class);
  private final RelDataTypeField arrayField = mock(RelDataTypeField.class);
  private final RelDataTypeField nonArrayField = mock(RelDataTypeField.class);
  private final ArraySqlType arraySqlType = mock(ArraySqlType.class);
  private final RelDataType nonArrayType = mock(RelDataType.class);
  private final DataSourceService dataSourceService = mock(DataSourceService.class);
  private final ExtendedRexBuilder rexBuilder = mock(ExtendedRexBuilder.class);

  private CalciteRelNodeVisitor visitor;
  private CalcitePlanContext context;

  @BeforeEach
  public void setUp() {
    // Intercept CalciteToolsHelper.create(...) so CalcitePlanContext.create(...) ends up using our
    // relBuilder.
    mockedCalciteToolsHelper = Mockito.mockStatic(CalciteToolsHelper.class);
    mockedCalciteToolsHelper
        .when(() -> CalciteToolsHelper.create(any(), any(), any()))
        .thenReturn(relBuilder);

    // Minimal relBuilder / row-type wiring used by the validation branches.
    lenient().when(relBuilder.peek()).thenReturn(leftRelNode);
    lenient().when(leftRelNode.getRowType()).thenReturn(leftRowType);

    // Some versions of Calcite require relBuilder.getRexBuilder()/getTypeFactory during context
    // creation.
    lenient().when(relBuilder.getRexBuilder()).thenReturn(rexBuilder);
    lenient().when(rexBuilder.getTypeFactory()).thenReturn(TYPE_FACTORY);

    // Create the plan context. Pass null for SysLimit (tests do not depend on it).
    context = CalcitePlanContext.create(frameworkConfig, null, QueryType.PPL);

    visitor = new CalciteRelNodeVisitor(dataSourceService);
  }

  @AfterEach
  public void tearDown() {
    mockedCalciteToolsHelper.close();
  }

  /**
   * Negative: requested field does not exist in current row type -> SemanticCheckException
   *
   * <p>This exercises the resolve-by-name branch early validation that throws when the named field
   * is not found in the current row type.
   */
  @Test
  public void expand_on_nonexistent_field_should_throw_user_friendly_error() throws Exception {
    lenient().when(leftRowType.getField("missing_field", false, false)).thenReturn(null);
    RexNode nonInputRexNode = mock(RexNode.class);

    Method m =
        CalciteRelNodeVisitor.class.getDeclaredMethod(
            "buildExpandRelNode",
            RexNode.class,
            String.class,
            String.class,
            Integer.class,
            CalcitePlanContext.class);
    m.setAccessible(true);

    InvocationTargetException ite =
        assertThrows(
            InvocationTargetException.class,
            () -> m.invoke(visitor, nonInputRexNode, "missing_field", null, null, context));
    Throwable cause = ite.getCause();
    assertTrue(cause instanceof SemanticCheckException);
    assertEquals(
        "Cannot expand field 'missing_field': field not found in input", cause.getMessage());
  }

  /**
   * Negative: requested field exists but is not an ARRAY -> SemanticCheckException
   *
   * <p>This exercises the resolve-by-name branch early validation that throws when the named field
   * exists but its type is not ArraySqlType.
   */
  @Test
  public void expand_on_non_array_field_should_throw_expected_array_message() throws Exception {
    // leftRowType.getField("not_array", false, false) -> nonArrayField and its type is non-array
    lenient().when(leftRowType.getField("not_array", false, false)).thenReturn(nonArrayField);
    lenient().when(nonArrayField.getType()).thenReturn(nonArrayType);
    lenient().when(nonArrayType.getSqlTypeName()).thenReturn(SqlTypeName.VARCHAR);

    RexNode nonInputRexNode = mock(RexNode.class);

    Method m =
        CalciteRelNodeVisitor.class.getDeclaredMethod(
            "buildExpandRelNode",
            RexNode.class,
            String.class,
            String.class,
            Integer.class,
            CalcitePlanContext.class);
    m.setAccessible(true);

    InvocationTargetException ite =
        assertThrows(
            InvocationTargetException.class,
            () -> m.invoke(visitor, nonInputRexNode, "not_array", null, null, context));
    Throwable cause = ite.getCause();
    assertTrue(cause instanceof SemanticCheckException);
    assertEquals(
        "Cannot expand field 'not_array': expected ARRAY type but found VARCHAR",
        cause.getMessage());
  }
}
