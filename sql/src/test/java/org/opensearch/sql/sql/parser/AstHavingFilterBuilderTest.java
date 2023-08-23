/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql.parser;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.ast.dsl.AstDSL.aggregate;
import static org.opensearch.sql.ast.dsl.AstDSL.qualifiedName;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.IdentContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.QualifiedNameContext;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.sql.parser.context.QuerySpecification;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
@ExtendWith(MockitoExtension.class)
class AstHavingFilterBuilderTest {

  @Mock private QuerySpecification querySpec;

  private AstHavingFilterBuilder builder;

  @BeforeEach
  void setup() {
    builder = new AstHavingFilterBuilder(querySpec);
  }

  @Test
  void should_replace_alias_with_select_expression() {
    QualifiedNameContext qualifiedName = mock(QualifiedNameContext.class);
    IdentContext identifier = mock(IdentContext.class);
    UnresolvedExpression expression = aggregate("AVG", qualifiedName("age"));

    when(identifier.getText()).thenReturn("a");
    when(qualifiedName.ident()).thenReturn(ImmutableList.of(identifier));
    when(querySpec.isSelectAlias(any())).thenReturn(true);
    when(querySpec.getSelectItemByAlias(any())).thenReturn(expression);
    assertEquals(expression, builder.visitQualifiedName(qualifiedName));
  }
}
