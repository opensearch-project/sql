/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.operator.predicate;

import java.util.regex.PatternSyntaxException;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ExpressionNodeVisitor;
import org.opensearch.sql.expression.env.Environment;
import org.opensearch.sql.expression.parse.RegexCommonUtils;

/**
 * Expression for regex matching using Java's built-in regex engine. Supports standard Java regex
 * features including named groups, lookahead/lookbehind, backreferences, and inline flags. Uses
 * find() for partial matching to align with SPL semantics.
 */
@ToString
@EqualsAndHashCode
public class RegexMatch implements Expression {
  @Getter private final Expression field;

  @Getter private final Expression pattern;

  @Getter private final boolean negated;

  public RegexMatch(Expression field, Expression pattern, boolean negated) {
    this.field = field;
    this.pattern = pattern;
    this.negated = negated;
  }

  @Override
  public ExprValue valueOf(Environment<Expression, ExprValue> valueEnv) {
    ExprValue fieldValue = field.valueOf(valueEnv);
    ExprValue patternValue = pattern.valueOf(valueEnv);

    if (fieldValue.isNull()
        || fieldValue.isMissing()
        || patternValue.isNull()
        || patternValue.isMissing()) {
      return ExprValueUtils.booleanValue(false);
    }

    String text = fieldValue.stringValue();
    String regex = patternValue.stringValue();

    if (text == null) {
      return ExprValueUtils.booleanValue(false);
    }

    try {
      boolean matches = RegexCommonUtils.matchesPartial(text, regex);

      return ExprValueUtils.booleanValue(negated ? !matches : matches);

    } catch (PatternSyntaxException e) {
      throw new IllegalArgumentException("Invalid regex pattern: " + e.getMessage());
    }
  }

  @Override
  public ExprType type() {
    return ExprCoreType.BOOLEAN;
  }

  @Override
  public <T, C> T accept(ExpressionNodeVisitor<T, C> visitor, C context) {
    return visitor.visitRegex(this, context);
  }
}
