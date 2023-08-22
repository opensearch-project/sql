/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.conditional.cases;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.data.model.ExprIntegerValue;
import org.opensearch.sql.data.model.ExprNullValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ExpressionTestBase;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
@ExtendWith(MockitoExtension.class)
class CaseClauseTest extends ExpressionTestBase {

  @Mock private WhenClause whenClause;

  @Test
  void should_return_when_clause_result_if_matched() {
    when(whenClause.isTrue(any())).thenReturn(true);
    when(whenClause.valueOf(any())).thenReturn(new ExprIntegerValue(30));

    CaseClause caseClause = new CaseClause(ImmutableList.of(whenClause), null);
    assertEquals(new ExprIntegerValue(30), caseClause.valueOf(valueEnv()));
  }

  @Test
  void should_return_default_result_if_none_matched() {
    when(whenClause.isTrue(any())).thenReturn(false);

    CaseClause caseClause = new CaseClause(ImmutableList.of(whenClause), DSL.literal(50));
    assertEquals(new ExprIntegerValue(50), caseClause.valueOf(valueEnv()));
  }

  @Test
  void should_return_default_result_if_none_matched_and_no_default() {
    when(whenClause.isTrue(any())).thenReturn(false);

    CaseClause caseClause = new CaseClause(ImmutableList.of(whenClause), null);
    assertEquals(ExprNullValue.of(), caseClause.valueOf(valueEnv()));
  }

  @Test
  void should_use_type_of_when_clause() {
    when(whenClause.type()).thenReturn(ExprCoreType.INTEGER);

    CaseClause caseClause = new CaseClause(ImmutableList.of(whenClause), null);
    assertEquals(ExprCoreType.INTEGER, caseClause.type());
  }

  @Test
  void should_use_type_of_nonnull_when_or_else_clause() {
    when(whenClause.type()).thenReturn(ExprCoreType.UNDEFINED);
    Expression defaultResult = mock(Expression.class);
    when(defaultResult.type()).thenReturn(ExprCoreType.STRING);

    CaseClause caseClause = new CaseClause(ImmutableList.of(whenClause), defaultResult);
    assertEquals(ExprCoreType.STRING, caseClause.type());
  }

  @Test
  void should_use_unknown_type_of_if_all_when_and_else_return_null() {
    when(whenClause.type()).thenReturn(ExprCoreType.UNDEFINED);
    Expression defaultResult = mock(Expression.class);
    when(defaultResult.type()).thenReturn(ExprCoreType.UNDEFINED);

    CaseClause caseClause = new CaseClause(ImmutableList.of(whenClause), defaultResult);
    assertEquals(ExprCoreType.UNDEFINED, caseClause.type());
  }

  @Test
  void should_return_all_result_types_including_default() {
    when(whenClause.type()).thenReturn(ExprCoreType.INTEGER);
    Expression defaultResult = mock(Expression.class);
    when(defaultResult.type()).thenReturn(ExprCoreType.STRING);

    CaseClause caseClause = new CaseClause(ImmutableList.of(whenClause), defaultResult);
    assertEquals(
        ImmutableList.of(ExprCoreType.INTEGER, ExprCoreType.STRING), caseClause.allResultTypes());
  }

  @Test
  void should_return_all_result_types_excluding_null_result() {
    when(whenClause.type()).thenReturn(ExprCoreType.UNDEFINED);
    Expression defaultResult = mock(Expression.class);
    when(defaultResult.type()).thenReturn(ExprCoreType.UNDEFINED);

    CaseClause caseClause = new CaseClause(ImmutableList.of(whenClause), defaultResult);
    assertEquals(ImmutableList.of(), caseClause.allResultTypes());
  }
}
