/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.parse;

import java.util.regex.Pattern;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.expression.Expression;

/** RegexExpression with regex and named capture group. */
@EqualsAndHashCode(callSuper = true)
@ToString
public class RegexExpression extends ParseExpression {
  private static final Logger log = LogManager.getLogger(RegexExpression.class);
  @Getter @EqualsAndHashCode.Exclude private final Pattern regexPattern;

  /**
   * RegexExpression.
   *
   * @param sourceField source text field
   * @param pattern pattern used for parsing
   * @param identifier derived field
   */
  public RegexExpression(Expression sourceField, Expression pattern, Expression identifier) {
    super("regex", sourceField, pattern, identifier);
    this.regexPattern = RegexCommonUtils.getCompiledPattern(pattern.valueOf().stringValue());
  }

  @Override
  public ExprValue parseValue(ExprValue value) throws ExpressionEvaluationException {
    String rawString = value.stringValue();

    String extracted = RegexCommonUtils.extractNamedGroup(rawString, regexPattern, identifierStr);

    if (extracted != null) {
      return new ExprStringValue(extracted);
    }
    log.debug("failed to extract pattern {} from input ***", regexPattern.pattern());
    return new ExprStringValue("");
  }
}
