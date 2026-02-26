/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql.dialect.clickhouse;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.opensearch.sql.api.dialect.QueryPreprocessor;

/**
 * Strips ClickHouse-specific clauses: FORMAT, SETTINGS, FINAL. Uses regex patterns to identify and
 * remove these clauses before the query reaches the Calcite SQL parser.
 */
/**
 * Strips ClickHouse-specific clauses: FORMAT, SETTINGS, FINAL. Uses a two-pass approach:
 * first masks string literals and comments to avoid false matches, then applies regex
 * patterns to identify and remove these clauses, and finally restores the masked content.
 */
public class ClickHouseQueryPreprocessor implements QueryPreprocessor {

  // Pattern: FORMAT <identifier> at end of query
  private static final Pattern FORMAT_PATTERN =
      Pattern.compile("\\bFORMAT\\s+\\w+\\s*$", Pattern.CASE_INSENSITIVE);

  // Pattern: SETTINGS key=value[, key=value]* at end of query
  private static final Pattern SETTINGS_PATTERN =
      Pattern.compile(
          "\\bSETTINGS\\s+[\\w.]+\\s*=\\s*[^,]+(?:,\\s*[\\w.]+\\s*=\\s*[^,]+)*\\s*$",
          Pattern.CASE_INSENSITIVE);

  // Pattern: FINAL keyword (word boundary)
  private static final Pattern FINAL_PATTERN =
      Pattern.compile("\\bFINAL\\b", Pattern.CASE_INSENSITIVE);

  // Pattern to match single-quoted string literals (with escaped quotes) or -- line comments
  private static final Pattern STRING_OR_COMMENT_PATTERN =
      Pattern.compile("'(?:[^'\\\\]|\\\\.)*'|--[^\\n]*");

  @Override
  public String preprocess(String query) {
    // 1. Mask string literals and comments so regex won't match inside them
    List<String> masked = new ArrayList<>();
    StringBuffer sb = new StringBuffer();
    Matcher m = STRING_OR_COMMENT_PATTERN.matcher(query);
    while (m.find()) {
      masked.add(m.group());
      m.appendReplacement(sb, buildPlaceholder(masked.size() - 1));
    }
    m.appendTail(sb);
    String work = sb.toString();

    // 2. Strip dialect-specific clauses from the masked query
    work = SETTINGS_PATTERN.matcher(work).replaceAll("");
    work = FORMAT_PATTERN.matcher(work).replaceAll("");
    work = FINAL_PATTERN.matcher(work).replaceAll("");

    // 3. Restore masked content
    for (int i = 0; i < masked.size(); i++) {
      work = work.replace(buildPlaceholder(i), masked.get(i));
    }

    return work.trim();
  }

  /** Build a placeholder token that won't collide with SQL content. */
  private static String buildPlaceholder(int index) {
    return "\u0000MASK" + index + "\u0000";
  }
}
