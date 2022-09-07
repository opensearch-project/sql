/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.parse;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.expression.PatternsMethod;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.Expression;

/**
 * ParseExpression with regex and named capture group.
 */
@EqualsAndHashCode(callSuper = true)
@ToString
public class PatternsExpression extends ParseExpression {
  /**
   * Key of the derived field name.
   */
  public static final String NEW_FIELD_KEY = "new_field";
  /**
   * Key of the pattern used to extract fields.
   */
  public static final String PATTERN_KEY = "pattern";
  /**
   * Default name of the derived field.
   */
  public static final String DEFAULT_NEW_FIELD = "patterns_field";

  private static final Pattern DEFAULT_IGNORED_CHARS = Pattern.compile("[a-zA-Z\\d]");
  private final PatternsMethod patternsMethod;
  private final Pattern pattern;

  /**
   * PatternsExpression.
   *
   * @param patternsMethod method used to extract patterns
   * @param sourceField    source text field
   * @param pattern        pattern used for parsing
   * @param identifier     derived field
   */
  public PatternsExpression(PatternsMethod patternsMethod, Expression sourceField,
                            Expression pattern, Expression identifier) {
    super("patterns", sourceField, pattern, identifier);
    this.patternsMethod = patternsMethod;
    switch (patternsMethod) {
      case REGEX:
        this.pattern = Pattern.compile(pattern.valueOf(null).stringValue());
        break;
      default:
        this.pattern = DEFAULT_IGNORED_CHARS;
        break;
    }
  }

  @Override
  ExprValue parseValue(ExprValue value) {
    String rawString = value.stringValue();
    return new ExprStringValue(pattern.matcher(rawString).replaceAll(""));
  }

  /**
   * Get list of derived fields.
   *
   * @param arguments patterns arguments used for parsing
   * @return list of names of the derived fields
   */
  public static List<String> getNamedGroupCandidates(Map<String, Literal> arguments) {
    String identifier = arguments.containsKey(PatternsExpression.NEW_FIELD_KEY) ?
        (String) arguments.get(PatternsExpression.NEW_FIELD_KEY).getValue() :
        PatternsExpression.DEFAULT_NEW_FIELD;
    return ImmutableList.of(identifier);
  }
}
