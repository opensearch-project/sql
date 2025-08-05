/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.analysis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.analysis.symbol.Namespace;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.ReferenceExpression;

class WildcardFieldResolverTest {

  private AnalysisContext context;
  private TypeEnvironment typeEnvironment;
  private ExpressionAnalyzer expressionAnalyzer;
  private Map<String, ExprType> availableFields;

  @BeforeEach
  void setUp() {
    context = mock(AnalysisContext.class);
    typeEnvironment = mock(TypeEnvironment.class);
    expressionAnalyzer = mock(ExpressionAnalyzer.class);

    when(context.peek()).thenReturn(typeEnvironment);

    // Setup available fields
    availableFields = new HashMap<>();
    availableFields.put("account_number", ExprCoreType.INTEGER);
    availableFields.put("firstname", ExprCoreType.STRING);
    availableFields.put("lastname", ExprCoreType.STRING);
    availableFields.put("balance", ExprCoreType.DOUBLE);
    availableFields.put("age", ExprCoreType.INTEGER);
    availableFields.put("city", ExprCoreType.STRING);
    availableFields.put("state", ExprCoreType.STRING);

    when(typeEnvironment.lookupAllFields(Namespace.FIELD_NAME)).thenReturn(availableFields);
  }

  @Test
  void testPrefixWildcard() {
    // Test account* pattern
    Field wildcardField = new Field(QualifiedName.of("account*"));
    List<UnresolvedExpression> projectList = Arrays.asList(wildcardField);

    List<NamedExpression> result =
        WildcardFieldResolver.resolveWildcards(projectList, context, expressionAnalyzer);

    assertEquals(1, result.size());
    assertEquals("account_number", result.get(0).getNameOrAlias());
    assertTrue(result.get(0).getDelegated() instanceof ReferenceExpression);
  }

  @Test
  void testSuffixWildcard() {
    // Test *name pattern
    Field wildcardField = new Field(QualifiedName.of("*name"));
    List<UnresolvedExpression> projectList = Arrays.asList(wildcardField);

    List<NamedExpression> result =
        WildcardFieldResolver.resolveWildcards(projectList, context, expressionAnalyzer);

    assertEquals(2, result.size());
    // Verify expected fields are present (order should not be assumed)
    List<String> resultNames = result.stream().map(NamedExpression::getNameOrAlias).toList();
    assertTrue(resultNames.contains("firstname"));
    assertTrue(resultNames.contains("lastname"));
  }

  @Test
  void testContainsWildcard() {
    // Test *a* pattern (should match account_number, age, balance, firstname, lastname, state)
    Field wildcardField = new Field(QualifiedName.of("*a*"));
    List<UnresolvedExpression> projectList = Arrays.asList(wildcardField);

    List<NamedExpression> result =
        WildcardFieldResolver.resolveWildcards(projectList, context, expressionAnalyzer);

    assertEquals(6, result.size());
    // Verify all expected fields are present
    List<String> resultNames = result.stream().map(NamedExpression::getNameOrAlias).toList();
    assertTrue(resultNames.contains("account_number"));
    assertTrue(resultNames.contains("age"));
    assertTrue(resultNames.contains("balance"));
    assertTrue(resultNames.contains("firstname"));
    assertTrue(resultNames.contains("lastname"));
    assertTrue(resultNames.contains("state"));
  }

  @Test
  void testMixedWildcardAndRegularFields() {
    // Test mixing wildcards with regular fields
    Field wildcardField = new Field(QualifiedName.of("*name"));
    Field regularField = new Field(QualifiedName.of("age"));
    List<UnresolvedExpression> projectList = Arrays.asList(wildcardField, regularField);

    // Mock the expression analyzer for regular field
    when(expressionAnalyzer.analyze(regularField, context))
        .thenReturn(new ReferenceExpression("age", ExprCoreType.INTEGER));

    List<NamedExpression> result =
        WildcardFieldResolver.resolveWildcards(projectList, context, expressionAnalyzer);

    assertEquals(3, result.size());
    // Verify all expected fields are present
    List<String> resultNames = result.stream().map(NamedExpression::getNameOrAlias).toList();
    assertTrue(resultNames.contains("firstname"));
    assertTrue(resultNames.contains("lastname"));
    assertTrue(resultNames.contains("age"));
  }
}
