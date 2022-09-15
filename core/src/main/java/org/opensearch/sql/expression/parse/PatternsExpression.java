/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.parse;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.expression.Expression;

/**
 * PatternsExpression with regex filter.
 */
@EqualsAndHashCode(callSuper = true)
@ToString
public class PatternsExpression extends ParseExpression {
  /**
   * Default name of the derived field.
   */
  public static final String DEFAULT_NEW_FIELD = "patterns_field";

  private static final Pattern DEFAULT_IGNORED_CHARS = Pattern.compile("[a-zA-Z\\d]");
  @EqualsAndHashCode.Exclude
  private final Pattern pattern;

  /**
   * PatternsExpression.
   *
   * @param sourceField source text field
   * @param pattern     pattern used for parsing
   * @param identifier  derived field
   */
  public PatternsExpression(Expression sourceField, Expression pattern, Expression identifier) {
    super("patterns", sourceField, pattern, identifier);
    String patternStr = pattern.valueOf(null).stringValue();
    this.pattern = patternStr.isEmpty() ? DEFAULT_IGNORED_CHARS : Pattern.compile(patternStr);
  }

  @Override
  ExprValue parseValue(ExprValue value) throws ExpressionEvaluationException {
    String rawString = value.stringValue();
    return new ExprStringValue(pattern.matcher(rawString).replaceAll(""));
  }

  /**
   * Get list of derived fields.
   *
   * @param identifier identifier used to generate the field name
   * @return list of names of the derived fields
   */
  public static List<String> getNamedGroupCandidates(String identifier) {
    return ImmutableList.of(Objects.requireNonNullElse(identifier, DEFAULT_NEW_FIELD));
  }
}
