/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.protocol.response.format;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.protocol.response.QueryResult;

@RequiredArgsConstructor
public abstract class FlatResponseFormatter implements ResponseFormatter<QueryResult> {
  private static String INLINE_SEPARATOR = ",";
  private static final String INTERLINE_SEPARATOR = System.lineSeparator();
  private static final Set<String> SENSITIVE_CHAR = ImmutableSet.of("=", "+", "-", "@");

  public static final String CONTENT_TYPE = "plain/text; charset=UTF-8";

  private boolean sanitize = false;
  private boolean pretty = false;

  public FlatResponseFormatter(String seperator, boolean sanitize, boolean pretty) {
    this.INLINE_SEPARATOR = seperator;
    this.sanitize = sanitize;
    this.pretty = pretty;
  }

  public String contentType() {
    return CONTENT_TYPE;
  }

  @Override
  public String format(QueryResult response) {
    FlatResult result = new FlatResult(response, sanitize, pretty);
    return result.getFlat();
  }

  @Override
  public String format(Throwable t) {
    return ErrorFormatter.prettyFormat(t);
  }

  /**
   * Sanitize methods are migrated from legacy CSV result. Sanitize both headers and data lines by:
   * 1) Second double quote entire cell if any comma is found.
   */
  @Getter
  @RequiredArgsConstructor
  static class FlatResult {
    private final QueryResult response;
    private final boolean sanitize;
    private final boolean pretty;

    private List<String> headers;
    private List<List<String>> data;
    private int[] maxWidths;

    public String getFlat() {
      // Store headers and data in variables to avoid redundant calls
      headers = getHeaders(response, sanitize);
      data = getData(response, sanitize);

      calculateMaxWidths();

      List<String> headersAndData = new ArrayList<>();
      headersAndData.add(getHeaderLine());
      headersAndData.addAll(getDataLines());
      return String.join(INTERLINE_SEPARATOR, headersAndData);
    }

    private void calculateMaxWidths() {
      int columns = headers.size();
      maxWidths = new int[columns];

      for (int i = 0; i < columns; i++) {
        int maxWidth = headers.get(i).length();
        for (List<String> row : data) {
          maxWidth = Math.max(maxWidth, row.get(i).length());
        }
        maxWidths[i] = maxWidth;
      }
    }

    private List<String> getDataLines() {
      if (pretty) {
        return data.stream().map(this::prettyFormatLine).collect(Collectors.toList());
      } else {
        return data.stream()
            .map(v -> String.join(INLINE_SEPARATOR, v))
            .collect(Collectors.toList());
      }
    }

    private String getHeaderLine() {
      if (pretty) {
        return prettyFormatLine(headers);
      } else {
        return String.join(INLINE_SEPARATOR, headers);
      }
    }

    private List<String> getHeaders(QueryResult response, boolean sanitize) {
      ImmutableList.Builder<String> headers = ImmutableList.builder();
      response.columnNameTypes().forEach((column, type) -> headers.add(column));
      List<String> result = headers.build();
      return sanitizeHeaders(result);
    }

    private List<List<String>> getData(QueryResult response, boolean sanitize) {
      ImmutableList.Builder<List<String>> dataLines = new ImmutableList.Builder<>();
      response
          .iterator()
          .forEachRemaining(
              row -> {
                ImmutableList.Builder<String> line = new ImmutableList.Builder<>();
                // replace null values with empty string
                Arrays.asList(row).forEach(val -> line.add(val == null ? "" : val.toString()));
                dataLines.add(line.build());
              });
      List<List<String>> result = dataLines.build();
      return sanitizeData(result);
    }

    /** Sanitize headers because OpenSearch allows special character present in field names. */
    private List<String> sanitizeHeaders(List<String> headers) {
      if (sanitize) {
        return headers.stream()
            .map(this::sanitizeCell)
            .map(cell -> quoteIfRequired(INLINE_SEPARATOR, cell))
            .collect(Collectors.toList());
      } else {
        return headers.stream()
            .map(cell -> quoteIfRequired(INLINE_SEPARATOR, cell))
            .collect(Collectors.toList());
      }
    }

    private List<List<String>> sanitizeData(List<List<String>> lines) {
      List<List<String>> result = new ArrayList<>();
      if (sanitize) {
        for (List<String> line : lines) {
          result.add(
              line.stream()
                  .map(this::sanitizeCell)
                  .map(cell -> quoteIfRequired(INLINE_SEPARATOR, cell))
                  .collect(Collectors.toList()));
        }
      } else {
        for (List<String> line : lines) {
          result.add(
              line.stream()
                  .map(cell -> quoteIfRequired(INLINE_SEPARATOR, cell))
                  .collect(Collectors.toList()));
        }
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

    private String prettyFormatLine(List<String> line) {
      return IntStream.range(0, line.size())
          .mapToObj(i -> padRight(line.get(i), maxWidths[i]))
          .collect(Collectors.joining(INLINE_SEPARATOR));
    }

    private String padRight(String s, int n) {
      return String.format("%-" + n + "s", s);
    }
  }
}
