/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.parse;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.regex.Pattern;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.ast.expression.ParseMethod;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.Expression;

/**
 * ParseExpression with regex and named capture group.
 */
@EqualsAndHashCode(callSuper = true)
@ToString
public class PunctExpression extends ParseExpression {
  private static final Logger log = LogManager.getLogger(PunctExpression.class);
  private static final Pattern DEFAULT_IGNORED_CHARS = Pattern.compile("[\\w\\s]");

  /**
   * PunctExpression.
   *
   * @param sourceField source text field
   * @param pattern     pattern used for parsing
   * @param identifier  derived field
   */
  public PunctExpression(Expression sourceField, Expression pattern, Expression identifier) {
    super(ParseMethod.PUNCT, sourceField, pattern, identifier);
  }

  @Override
  ExprValue parseValue(ExprValue value) {
    String rawString = value.stringValue();
    return new ExprStringValue(DEFAULT_IGNORED_CHARS.matcher(rawString).replaceAll(""));
  }

  /**
   * Get list of derived fields based on parse pattern.
   *
   * @param pattern pattern used for parsing
   * @return list of names of the derived fields
   */
  public static List<String> getNamedGroupCandidates(String pattern) {
    return ImmutableList.of(pattern);
  }
}
