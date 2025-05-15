/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.common.patterns;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class PatternUtils {
  public static final String PATTERN = "pattern";
  public static final String COUNT = "count";
  public static final String SAMPLE_LOGS = "sampleLogs";
  public static final String TOKENS = "tokens";
  public static final String WILDCARD_PREFIX = "<*";
  public static final Pattern WILDCARD_PATTERN = Pattern.compile("<\\*[^>]*>");
  public static final String TOKEN_PREFIX = "<token";
  public static final Pattern TOKEN_PATTERN = Pattern.compile("<token\\d+>");

  public static Map<String, Map<String, Object>> mergePatternGroups(
      Map<String, Map<String, Object>> left,
      Map<String, Map<String, Object>> right,
      int maxSampleCount) {
    if (left == null) {
      return right;
    }
    if (right == null) {
      return left;
    }
    // Merge pattern count and sample logs, merged sample logs not exceed max sample count
    right.forEach(
        (pattern, stat) -> {
          if (left.containsKey(pattern)) {
            Map<String, Object> leftStat = left.get(pattern);
            leftStat.put(COUNT, (Long) leftStat.get(COUNT) + (Long) stat.get(COUNT));
            List<String> leftSampleLogs = (List<String>) leftStat.get(SAMPLE_LOGS);
            List<String> rightSampleLogs = (List<String>) stat.get(SAMPLE_LOGS);
            if (leftSampleLogs.size() < maxSampleCount) {
              leftSampleLogs.addAll(
                  rightSampleLogs.subList(
                      0, Math.min(rightSampleLogs.size(), maxSampleCount - leftSampleLogs.size())));
            }
          } else {
            left.put(pattern, stat);
          }
        });
    return left;
  }

  public static void extractVariables(
      ParseResult parseResult, String original, Map<String, List<String>> result, String prefix) {
    List<String> parts = parseResult.parts;
    List<String> tokenOrder = parseResult.tokenOrder;

    if (parts.isEmpty()) {
      return;
    }

    int pos = 0;
    int i = 0;
    int tokenIndex = 0;

    while (i < parts.size()) {
      String currentPart = parts.get(i);
      if (currentPart.startsWith(prefix)) { // Process already labeled part
        String tokenKey = tokenOrder.get(tokenIndex++);
        if (i == parts.size() - 1) { // The last part
          String value = original.substring(pos);
          addToResult(result, tokenKey, value);
          pos = original.length();
          i++;
        } else {
          String nextStatic = parts.get(i + 1);
          int index = original.indexOf(nextStatic, pos);
          if (index == -1) {
            return;
          }
          String value = original.substring(pos, index);
          addToResult(result, tokenKey, value);
          pos = index;
          i++;
        }
      } else { // Process static part
        if (original.startsWith(currentPart, pos)) {
          pos += currentPart.length();
          i++;
        } else {
          return;
        }
      }
    }
  }

  public static class ParseResult {
    List<String> parts;
    List<String> tokenOrder;

    public ParseResult(List<String> parts, List<String> tokenOrder) {
      this.parts = parts;
      this.tokenOrder = tokenOrder;
    }

    public String toTokenOrderString(String prefix) {
      StringBuilder result = new StringBuilder();
      int tokenIndex = 0;
      for (String currentPart : parts) {
        if (currentPart.startsWith(prefix)) {
          result.append(tokenOrder.get(tokenIndex++));
        } else {
          result.append(currentPart);
        }
      }
      return result.toString();
    }
  }

  /*
   *  Parse pattern string:
   *  1. Generates static parts and '<*>' like variable placeholders
   *  2. Generates new placeholder list called token orders in format <token{$order_number}>
   */
  public static ParseResult parsePattern(String pattern, Pattern compiledPattern) {
    List<String> parts = new ArrayList<>();
    List<String> tokenOrder = new ArrayList<>();
    Matcher matcher = compiledPattern.matcher(pattern);
    int lastEnd = 0;
    int tokenCount = 1;

    while (matcher.find()) {
      int start = matcher.start();
      int end = matcher.end();
      // Add static part before the found match if there is
      if (start > lastEnd) {
        parts.add(pattern.substring(lastEnd, start));
      }
      // Add matched wildcard part and generate token order key
      String wildcard = matcher.group();
      parts.add(wildcard);
      tokenOrder.add("<token" + tokenCount++ + ">");
      lastEnd = end;
    }

    // Add static part at the end
    if (lastEnd < pattern.length()) {
      parts.add(pattern.substring(lastEnd));
    }

    return new ParseResult(parts, tokenOrder);
  }

  private static void addToResult(Map<String, List<String>> result, String key, String value) {
    result.computeIfAbsent(key, k -> new ArrayList<>()).add(value);
  }
}
