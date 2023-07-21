/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.expression.parse;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.regex.Matcher;
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

/**
 * RegexExpression with regex and named capture group.
 */
@EqualsAndHashCode(callSuper = true)
@ToString
public class RegexExpression extends ParseExpression {
  private static final Logger log = LogManager.getLogger(RegexExpression.class);
  private static final Pattern GROUP_PATTERN = Pattern.compile("\\(\\?<([a-zA-Z][a-zA-Z0-9]*)>");
  @Getter
  @EqualsAndHashCode.Exclude
  private final Pattern regexPattern;

  /**
   * RegexExpression.
   *
   * @param sourceField source text field
   * @param pattern     pattern used for parsing
   * @param identifier  derived field
   */
  public RegexExpression(Expression sourceField, Expression pattern, Expression identifier) {
    super("regex", sourceField, pattern, identifier);
    this.regexPattern = Pattern.compile(pattern.valueOf().stringValue());
  }

  @Override
  ExprValue parseValue(ExprValue value) throws ExpressionEvaluationException {
    String rawString = value.stringValue();
    Matcher matcher = regexPattern.matcher(rawString);
    if (matcher.matches()) {
      return new ExprStringValue(matcher.group(identifierStr));
    }
    log.debug("failed to extract pattern {} from input ***", regexPattern.pattern());
    return new ExprStringValue("");
  }

  /**
   * Get list of derived fields based on parse pattern.
   *
   * @param pattern pattern used for parsing
   * @return list of names of the derived fields
   */
  public static List<String> getNamedGroupCandidates(String pattern) {
    ImmutableList.Builder<String> namedGroups = ImmutableList.builder();
    Matcher m = GROUP_PATTERN.matcher(pattern);
    while (m.find()) {
      namedGroups.add(m.group(1));
    }
    return namedGroups.build();
  }
}
