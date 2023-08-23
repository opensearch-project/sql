/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.executor.format;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.opensearch.client.Client;

public abstract class ResultSet {

  protected Schema schema;
  protected DataRows dataRows;

  protected Client client;
  protected String clusterName;

  public Schema getSchema() {
    return schema;
  }

  public DataRows getDataRows() {
    return dataRows;
  }

  protected String getClusterName() {
    return client.admin().cluster().prepareHealth().get().getClusterName();
  }

  /**
   * Check if given string matches the pattern. Do this check only if the pattern is a regex.
   * Otherwise skip the matching process and consider it's a match. This is a quick fix to support
   * SHOW/DESCRIBE alias by skip mismatch between actual index name and pattern (alias).
   *
   * @param string string to match
   * @param pattern pattern
   * @return true if match or pattern is not regular expression. otherwise false.
   */
  protected boolean matchesPatternIfRegex(String string, String pattern) {
    return isNotRegexPattern(pattern) || matchesPattern(string, pattern);
  }

  protected boolean matchesPattern(String string, String pattern) {
    Pattern p = Pattern.compile(pattern);
    Matcher matcher = p.matcher(string);
    return matcher.find();
  }

  private boolean isNotRegexPattern(String pattern) {
    return !pattern.contains(".") && !pattern.contains("*");
  }
}
