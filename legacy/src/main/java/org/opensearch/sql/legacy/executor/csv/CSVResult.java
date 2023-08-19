/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.executor.csv;

import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/** Created by Eliran on 27/12/2015. */
public class CSVResult {

  private static final Set<String> SENSITIVE_CHAR = ImmutableSet.of("=", "+", "-", "@");

  private final List<String> headers;
  private final List<String> lines;

  /**
   * Skip sanitizing if string line provided. This constructor is basically used by assertion in
   * test code.
   */
  public CSVResult(List<String> headers, List<String> lines) {
    this.headers = headers;
    this.lines = lines;
  }

    /**
     * Sanitize both headers and data lines by:
     * <ol>
     *  <li>First prepend single quote if first char is sensitive (= - + @)
     *  <li>Second double quote entire cell if any comma found
     *  </ol>
     */
    public CSVResult(String separator, List<String> headers, List<List<String>> lines) {
        this.headers = sanitizeHeaders(separator, headers);
        this.lines = sanitizeLines(separator, lines);
    }

  /**
   * Return CSV header names which are sanitized because OpenSearch allows special character present
   * in field name too.
   *
   * @return CSV header name list after sanitized
   */
  public List<String> getHeaders() {
    return headers;
  }

  /**
   * Return CSV lines in which each cell is sanitized to avoid CSV injection.
   *
   * @return CSV lines after sanitized
   */
  public List<String> getLines() {
    return lines;
  }

  private List<String> sanitizeHeaders(String separator, List<String> headers) {
    return headers.stream()
        .map(this::sanitizeCell)
        .map(cell -> quoteIfRequired(separator, cell))
        .collect(Collectors.toList());
  }

  private List<String> sanitizeLines(String separator, List<List<String>> lines) {
    List<String> result = new ArrayList<>();
    for (List<String> line : lines) {
      result.add(
          line.stream()
              .map(this::sanitizeCell)
              .map(cell -> quoteIfRequired(separator, cell))
              .collect(Collectors.joining(separator)));
    }
    return result;
  }

  private String sanitizeCell(String cell) {
    if (isStartWithSensitiveChar(cell)) {
      return "'" + cell;
    }
    return cell;
  }

  private String quoteIfRequired(String separator, String cell) {
    final String quote = "\"";
    return cell.contains(separator) ? quote + cell.replaceAll("\"", "\"\"") + quote : cell;
  }

  private boolean isStartWithSensitiveChar(String cell) {
    return SENSITIVE_CHAR.stream().anyMatch(cell::startsWith);
  }
}
