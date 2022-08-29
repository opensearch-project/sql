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
import org.opensearch.sql.ast.expression.ParseMethod;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.Expression;

/**
 * ParseExpression with regex and named capture group.
 */
@EqualsAndHashCode
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
    super(ParseMethod.REGEX, sourceField, pattern, identifier);
    this.regexPattern = Pattern.compile(pattern.valueOf(null).stringValue());
  }

  @Override
  ExprValue parseValue(ExprValue value) {
    String rawString = value.stringValue();
    Matcher matcher = regexPattern.matcher(rawString);
    if (matcher.matches()) {
      return new ExprStringValue(matcher.group(identifierStr));
    }
    log.warn("failed to extract pattern {} from input {}", regexPattern.pattern(), rawString);
    return new ExprStringValue("");
  }

  public static List<String> getNamedGroupCandidates(String pattern) {
    ImmutableList.Builder<String> namedGroups = ImmutableList.builder();
    Matcher m = GROUP_PATTERN.matcher(pattern);
    while (m.find()) {
      namedGroups.add(m.group(1));
    }
    return namedGroups.build();
  }
}
