/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.rest.analytics.stub;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Temporary index detection logic for routing queries to the analytics engine. Uses a regex to
 * extract the index name and a "parquet_" prefix convention to identify analytics indices.
 *
 * <p>Will be replaced by {@code UnifiedQueryParser} for index extraction and index settings (e.g.,
 * {@code index.storage_type}) for detection when available.
 */
public class StubIndexDetector {

  /**
   * Pattern to extract index name from PPL source clause. Matches: source = index, source=index,
   * source = `index`, source = catalog.index
   */
  private static final Pattern SOURCE_PATTERN =
      Pattern.compile(
          "source\\s*=\\s*`?([a-zA-Z0-9_.*]+(?:\\.[a-zA-Z0-9_.*]+)*)`?", Pattern.CASE_INSENSITIVE);

  /**
   * Check if the query targets an analytics engine index (e.g., Parquet-backed). Currently uses a
   * prefix convention ("parquet_"). In production, this will check index settings such as
   * index.storage_type.
   */
  public static boolean isAnalyticsIndex(String query) {
    if (query == null) {
      return false;
    }
    String indexName = extractIndexName(query);
    if (indexName == null) {
      return false;
    }
    int lastDot = indexName.lastIndexOf('.');
    String tableName = lastDot >= 0 ? indexName.substring(lastDot + 1) : indexName;
    return tableName.startsWith("parquet_");
  }

  /**
   * Extract the source index name from a PPL query string.
   *
   * @param query the PPL query string
   * @return the index name, or null if not found
   */
  static String extractIndexName(String query) {
    Matcher matcher = SOURCE_PATTERN.matcher(query);
    if (matcher.find()) {
      return matcher.group(1);
    }
    return null;
  }
}
