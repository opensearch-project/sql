/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.operator.predicate;

import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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

  // Pattern cache to avoid recompiling the same patterns
  private static final ConcurrentHashMap<String, Pattern> patternCache = new ConcurrentHashMap<>();

  // Maximum cache size to prevent memory issues
  private static final int MAX_CACHE_SIZE = 1000;

  public RegexMatch(Expression field, Expression pattern, boolean negated) {
    this.field = field;
    this.pattern = pattern;
    this.negated = negated;
  }

  @Override
  public ExprValue valueOf(Environment<Expression, ExprValue> valueEnv) {
    ExprValue fieldValue = field.valueOf(valueEnv);
    ExprValue patternValue = pattern.valueOf(valueEnv);

    // Handle null/missing values
    if (fieldValue.isNull()
        || fieldValue.isMissing()
        || patternValue.isNull()
        || patternValue.isMissing()) {
      return ExprValueUtils.booleanValue(false);
    }

    String text = fieldValue.stringValue();
    String regex = patternValue.stringValue();

    try {
      // Get compiled pattern from cache or compile new one
      Pattern compiledPattern = getCompiledPattern(regex);

      // Create matcher and check for match
      Matcher matcher = compiledPattern.matcher(text);
      boolean matches = matcher.find(); // Use find() for partial match like SPL

      // Apply negation if needed
      return ExprValueUtils.booleanValue(negated ? !matches : matches);

    } catch (PatternSyntaxException e) {
      throw new IllegalArgumentException("Invalid regex pattern: " + e.getMessage());
    }
  }

  /** Get compiled pattern from cache or compile and cache it. */
  private Pattern getCompiledPattern(String regex) {
    // Check cache size and clear if needed (simple LRU-like behavior)
    if (patternCache.size() > MAX_CACHE_SIZE) {
      patternCache.clear();
    }

    return patternCache.computeIfAbsent(
        regex,
        r -> {
          // Compile with Java regex engine
          return Pattern.compile(r);
        });
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
