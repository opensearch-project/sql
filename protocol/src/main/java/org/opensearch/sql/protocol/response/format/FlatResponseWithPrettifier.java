/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.protocol.response.format;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.opensearch.sql.protocol.response.QueryResult;

public class FlatResponseWithPrettifier extends FlatResponseBase {
  private int[] maxWidths;

  FlatResponseWithPrettifier(QueryResult response, String inlineSeparator) {
    super(response, inlineSeparator);
    calculateMaxWidths();
  }

  private void calculateMaxWidths() {
    int columns = getHeaders().size();
    maxWidths = new int[columns];

    for (int i = 0; i < columns; i++) {
      int maxWidth = getHeaders().get(i).length();
      for (List<String> row : getData()) {
        maxWidth = Math.max(maxWidth, row.get(i).length());
      }
      maxWidths[i] = maxWidth;
    }
  }

  @Override
  protected List<String> getDataLines() {
    return getData().stream().map(this::prettyFormatLine).collect(Collectors.toList());
  }

  @Override
  protected String getHeaderLine() {
    return prettyFormatLine(getHeaders());
  }

  private String prettyFormatLine(List<String> line) {
    return IntStream.range(0, line.size())
        .mapToObj(i -> padRight(line.get(i), maxWidths[i]))
        .collect(Collectors.joining(separator));
  }

  private String padRight(String s, int n) {
    return String.format("%-" + n + "s", s);
  }
}
