/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.experimental.UtilityClass;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.expression.ParseExpression;

/**
 * Utils for {@link ParseExpression}.
 */
@UtilityClass
public class ParseUtils {
  private static final Logger log = LogManager.getLogger(ParseUtils.class);
  private static final Pattern GROUP_PATTERN = Pattern.compile("\\(\\?<([a-zA-Z][a-zA-Z0-9]*)>");

  /**
   * Get matched group value.
   *
   * @param value      text field
   * @param pattern    regex pattern
   * @param identifier named capture group
   * @return matched group value, empty string if pattern does not match
   */
  public static ExprValue parseValue(ExprValue value, Pattern pattern, String identifier) {
    if (value.isNull() || value.isMissing()) {
      return ExprValueUtils.nullValue();
    }

    String rawString = value.stringValue();
    Matcher matcher = pattern.matcher(rawString);
    if (matcher.matches()) {
      return new ExprStringValue(matcher.group(identifier));
    }
    log.warn("failed to extract pattern {} from input {}", pattern.pattern(), rawString);
    return new ExprStringValue("");
  }

  /**
   * Get capture groups from regex pattern.
   *
   * @param pattern regex pattern
   * @return list of named capture groups in regex pattern
   */
  public static List<String> getNamedGroupCandidates(String pattern) {
    List<String> namedGroups = new ArrayList<>();
    Matcher m = GROUP_PATTERN.matcher(pattern);
    while (m.find()) {
      namedGroups.add(m.group(1));
    }
    return namedGroups;
  }
}
