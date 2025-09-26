/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.TYPE_FACTORY;

import java.sql.Connection;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLambdaRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
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
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.calcite.utils.CalciteToolsHelper;
import org.opensearch.sql.executor.QueryType;

@ExtendWith(MockitoExtension.class)
class QualifiedNameResolverTest {

  @Mock private FrameworkConfig frameworkConfig;
  @Mock private Connection connection;
  @Mock private RelBuilder relBuilder;
  @Mock private ExtendedRexBuilder rexBuilder;
  @Mock private RelNode relNode;
  @Mock private RelRecordType recordType;
  @Mock private RexInputRef mockRexInputRef;
  @Mock private RexLiteral mockRexLiteral;
  @Mock private RexCorrelVariable mockCorrelVariable;

  private CalcitePlanContext context;
  private MockedStatic<CalciteToolsHelper> mockedStatic;

  @BeforeEach
  void setUp() {
    when(relBuilder.getRexBuilder()).thenReturn(rexBuilder);
    when(rexBuilder.getTypeFactory()).thenReturn(TYPE_FACTORY);
    mockedStatic = Mockito.mockStatic(CalciteToolsHelper.class);
    mockedStatic.when(() -> CalciteToolsHelper.connect(any(), any())).thenReturn(connection);
    mockedStatic.when(() -> CalciteToolsHelper.create(any(), any(), any())).thenReturn(relBuilder);

    context = CalcitePlanContext.create(frameworkConfig, 100, QueryType.PPL);
  }

  @AfterEach
  void tearDown() {
    mockedStatic.close();
  }

  private void setupJoinContext() {
    context.setResolvingJoinCondition(true);
  }

  private void setupSingleTableContext(List<String> fieldNames) {
    context.setResolvingJoinCondition(false);
    when(relBuilder.peek()).thenReturn(relNode);
    when(relNode.getRowType()).thenReturn(recordType);
    when(recordType.getFieldNames()).thenReturn(fieldNames);
  }

  private void setupCoalesceContext(List<String> fieldNames) {
    setupSingleTableContext(fieldNames);
    context.setInCoalesceFunction(true);
  }

  private void setupLambdaContext(Map<String, RexLambdaRef> lambdaRefs) {
    context.setResolvingJoinCondition(false);
    context.putRexLambdaRefMap(lambdaRefs);
  }

  private void setupCorrelatedSubqueryContext(List<String> fieldNames) {
    setupSingleTableContext(fieldNames);
    context.pushCorrelVar(mockCorrelVariable);
  }

  private void mockFieldResolutionInLeftTable(String fieldName, RexInputRef result) {
    when(relBuilder.field(2, 0, fieldName)).thenReturn(result);
  }

  private void mockFieldResolutionInRightTable(String fieldName, RexInputRef result) {
    when(relBuilder.field(2, 1, fieldName)).thenReturn(result);
  }

  private void mockFieldResolutionFailsInLeftTable(String fieldName) {
    when(relBuilder.field(2, 0, fieldName))
        .thenThrow(new IllegalArgumentException("Field not found"));
  }

  private void mockJoinFieldResolution(String tableAlias, String fieldName, RexInputRef result) {
    when(relBuilder.field(2, tableAlias, fieldName)).thenReturn(result);
  }

  private void mockDirectFieldResolution(String fieldName, RexInputRef result) {
    when(relBuilder.field(fieldName)).thenReturn(result);
  }

  private void mockDirectFieldResolutionFails(String fieldName) {
    when(relBuilder.field(fieldName)).thenThrow(new IllegalArgumentException("Field not found"));
  }

  private void mockTableFieldResolution(String tableName, String fieldName, RexInputRef result) {
    when(relBuilder.field(1, tableName, fieldName)).thenReturn(result);
  }

  private void mockTableFieldResolutionFails(String tableName, String fieldName) {
    when(relBuilder.field(1, tableName, fieldName))
        .thenThrow(new IllegalArgumentException("Field not found"));
  }

  private void mockCorrelatedFieldResolution(String fieldName, RexInputRef result) {
    when(relBuilder.field(mockCorrelVariable, fieldName)).thenReturn(result);
  }

  @Test
  void testResolveInJoinCondition_SinglePart() {
    setupJoinContext();
    mockFieldResolutionInLeftTable("id", mockRexInputRef);

    RexNode result = QualifiedNameResolver.resolve(QualifiedName.of("id"), context);

    assertEquals(mockRexInputRef, result);
  }

  @Test
  void testResolveInJoinCondition_SinglePart_FallbackToSecondTable() {
    setupJoinContext();
    mockFieldResolutionFailsInLeftTable("id");
    mockFieldResolutionInRightTable("id", mockRexInputRef);

    RexNode result = QualifiedNameResolver.resolve(QualifiedName.of("id"), context);

    assertEquals(mockRexInputRef, result);
  }

  @Test
  void testResolveInJoinCondition_TwoParts() {
    setupJoinContext();
    mockJoinFieldResolution("t1", "id", mockRexInputRef);

    RexNode result = QualifiedNameResolver.resolve(QualifiedName.of("t1", "id"), context);

    assertEquals(mockRexInputRef, result);
  }

  @Test
  void testResolveInJoinCondition_ThreeParts_ThrowsException() {
    setupJoinContext();

    assertThrows(
        UnsupportedOperationException.class,
        () ->
            QualifiedNameResolver.resolve(QualifiedName.of("catalog", "schema", "table"), context));
  }

  @Test
  void testResolveInNonJoinCondition_LambdaRef() {
    Map<String, RexLambdaRef> lambdaRefMap = new HashMap<>();
    RexLambdaRef lambdaRef = mock(RexLambdaRef.class);
    lambdaRefMap.put("x", lambdaRef);
    setupLambdaContext(lambdaRefMap);

    RexNode result = QualifiedNameResolver.resolve(QualifiedName.of("x"), context);

    assertEquals(lambdaRef, result);
  }

  @Test
  void testResolveInNonJoinCondition_CoalesceFunction() {
    setupCoalesceContext(Arrays.asList("field1", "field2"));

    RexNode result = QualifiedNameResolver.resolve(QualifiedName.of("nonexistent"), context);

    assertEquals("null:VARCHAR", result.toString());
  }

  @Test
  void testResolveInNonJoinCondition_DirectFieldMatch() {
    setupSingleTableContext(Arrays.asList("field1", "field2"));
    mockDirectFieldResolution("field1", mockRexInputRef);

    RexNode result = QualifiedNameResolver.resolve(QualifiedName.of("field1"), context);

    assertEquals(mockRexInputRef, result);
  }

  @Test
  void testResolveInNonJoinCondition_TwoParts_DirectMatch() {
    setupSingleTableContext(Arrays.asList("field1", "field2"));
    mockTableFieldResolution("table", "field", mockRexInputRef);

    RexNode result = QualifiedNameResolver.resolve(QualifiedName.of("table", "field"), context);

    assertEquals(mockRexInputRef, result);
  }

  @Test
  void testResolveInNonJoinCondition_TwoParts_WithCorrelVariable() {
    setupCorrelatedSubqueryContext(Arrays.asList("field1", "field2"));
    mockTableFieldResolutionFails("outer", "field");
    mockCorrelatedFieldResolution("field", mockRexInputRef);

    RexNode result = QualifiedNameResolver.resolve(QualifiedName.of("outer", "field"), context);

    assertEquals(mockRexInputRef, result);
  }

  @Test
  void testResolveInNonJoinCondition_FieldNotFound_ThrowsException() {
    setupSingleTableContext(Arrays.asList("field1", "field2"));
    mockDirectFieldResolutionFails("nonexistent");

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> QualifiedNameResolver.resolve(QualifiedName.of("nonexistent"), context));
    assertEquals("Field not found", exception.getMessage());
  }

  @Test
  void testResolveInNonJoinCondition_WithCorrelVariableForSinglePart() {
    setupCorrelatedSubqueryContext(Arrays.asList("other_field"));
    mockCorrelatedFieldResolution("field", mockRexInputRef);

    RexNode result = QualifiedNameResolver.resolve(QualifiedName.of("field"), context);

    assertEquals(mockRexInputRef, result);
  }
}
