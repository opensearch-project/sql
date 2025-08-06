/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.analysis;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
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

    availableFields = new HashMap<>();
    availableFields.put("account_number", ExprCoreType.INTEGER);
    availableFields.put("firstname", ExprCoreType.STRING);
    availableFields.put("lastname", ExprCoreType.STRING);
    availableFields.put("balance", ExprCoreType.DOUBLE);
    availableFields.put("age", ExprCoreType.INTEGER);
    availableFields.put("city", ExprCoreType.STRING);
    availableFields.put("state", ExprCoreType.STRING);
    availableFields.put("gender", ExprCoreType.STRING);
    availableFields.put("employer", ExprCoreType.STRING);

    when(typeEnvironment.lookupAllFields(Namespace.FIELD_NAME)).thenReturn(availableFields);
  }

  private void testWildcard(List<String> wildcardPatterns, List<String> expectedFields) {
    List<UnresolvedExpression> projectList =
        wildcardPatterns.stream()
            .map(pattern -> new Field(QualifiedName.of(pattern)))
            .collect(java.util.stream.Collectors.toList());

    List<NamedExpression> result =
        WildcardFieldResolver.resolveWildcards(projectList, context, expressionAnalyzer);

    ImmutableList<String> resultNames =
        ImmutableList.copyOf(result.stream().map(NamedExpression::getNameOrAlias).toList());
    ImmutableList<String> expected = ImmutableList.copyOf(expectedFields);

    if (resultNames.size() != expected.size() || !resultNames.containsAll(expected)) {
      throw new AssertionError("Expected: " + expected + ", but got: " + resultNames);
    }
  }

  @Test
  void testFieldOrdering() {
    Field field1 = new Field(QualifiedName.of("balance"));
    Field field2 = new Field(QualifiedName.of("account*"));
    Field field3 = new Field(QualifiedName.of("firstname"));
    List<UnresolvedExpression> projectList = Arrays.asList(field1, field2, field3);

    when(expressionAnalyzer.analyze(field1, context))
        .thenReturn(new ReferenceExpression("balance", ExprCoreType.DOUBLE));
    when(expressionAnalyzer.analyze(field3, context))
        .thenReturn(new ReferenceExpression("firstname", ExprCoreType.STRING));

    List<NamedExpression> result =
        WildcardFieldResolver.resolveWildcards(projectList, context, expressionAnalyzer);

    ImmutableList<String> resultNames =
        ImmutableList.copyOf(result.stream().map(NamedExpression::getNameOrAlias).toList());
    ImmutableList<String> expected = ImmutableList.of("balance", "account_number", "firstname");

    if (!resultNames.equals(expected)) {
      throw new AssertionError("Expected: " + expected + ", but got: " + resultNames);
    }
  }

  @Test
  void testPrefixWildcard() {
    testWildcard(ImmutableList.of("account*"), ImmutableList.of("account_number"));
  }

  @Test
  void testSuffixWildcard() {
    testWildcard(ImmutableList.of("*name"), ImmutableList.of("firstname", "lastname"));
  }

  @Test
  void testContainsWildcard() {
    testWildcard(
        ImmutableList.of("*a*"),
        ImmutableList.of("account_number", "age", "balance", "firstname", "lastname", "state"));
  }

  @Test
  void testMixedWildcardAndRegularFields() {
    Field wildcardField = new Field(QualifiedName.of("*name"));
    Field regularField = new Field(QualifiedName.of("age"));
    List<UnresolvedExpression> projectList = Arrays.asList(wildcardField, regularField);

    when(expressionAnalyzer.analyze(regularField, context))
        .thenReturn(new ReferenceExpression("age", ExprCoreType.INTEGER));

    List<NamedExpression> result =
        WildcardFieldResolver.resolveWildcards(projectList, context, expressionAnalyzer);

    ImmutableList<String> resultNames =
        ImmutableList.copyOf(result.stream().map(NamedExpression::getNameOrAlias).toList());
    ImmutableList<String> expected = ImmutableList.of("firstname", "lastname", "age");

    if (resultNames.size() != 3 || !resultNames.containsAll(expected)) {
      throw new AssertionError("Expected to contain: " + expected + ", but got: " + resultNames);
    }
  }

  @Test
  void testComplexWildcardPattern() {
    testWildcard(
        ImmutableList.of("*a*e"),
        ImmutableList.of("balance", "firstname", "lastname", "age", "state"));
  }

  @Test
  void testNoMatchingWildcard() {
    testWildcard(ImmutableList.of("XYZ*"), ImmutableList.of());
  }

  @Test
  void testMultipleOverlappingWildcards() {
    testWildcard(
        ImmutableList.of("*a*", "*name"),
        ImmutableList.of("account_number", "firstname", "lastname", "balance", "age", "state"));
  }

  @Test
  void testDuplicateWildcardMatches() {
    testWildcard(
        ImmutableList.of("account*", "account_number"), ImmutableList.of("account_number"));
  }

  @Test
  void testOverlappingWildcardsDeduplication() {
    testWildcard(
        ImmutableList.of("*name", "first*", "last*"), ImmutableList.of("firstname", "lastname"));
  }
}
