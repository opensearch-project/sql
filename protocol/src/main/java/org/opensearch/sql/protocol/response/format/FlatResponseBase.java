/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.protocol.response.format;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Getter;
import org.opensearch.sql.protocol.response.QueryResult;

@Getter
public class FlatResponseBase {
  protected static final String INTERLINE_SEPARATOR = System.lineSeparator();

  private final QueryResult response;
  protected final String separator;

  private final List<String> headers;
  private final List<List<String>> data;

  FlatResponseBase(QueryResult response, String separator) {
    this.response = response;
    this.separator = separator;
    this.headers = getOriginalHeaders(response);
    this.data = getOriginalData(response);
  }

  public String format() {
    List<String> headersAndData = new ArrayList<>();
    headersAndData.add(getHeaderLine());
    headersAndData.addAll(getDataLines());
    return String.join(INTERLINE_SEPARATOR, headersAndData);
  }

  protected String getHeaderLine() {
    return String.join(separator, headers);
  }

  private List<String> getOriginalHeaders(QueryResult response) {
    ImmutableList.Builder<String> headers = ImmutableList.builder();
    response.columnNameTypes().forEach((column, type) -> headers.add(column));
    List<String> result = headers.build();
    return formatHeaders(result);
  }

  protected List<String> getDataLines() {
    return data.stream().map(v -> String.join(separator, v)).collect(Collectors.toList());
  }

  private List<List<String>> getOriginalData(QueryResult response) {
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
    return formatData(result);
  }

  protected List<String> formatHeaders(List<String> headers) {
    return headers.stream()
        .map(cell -> quoteIfRequired(separator, cell))
        .collect(Collectors.toList());
  }

  protected List<List<String>> formatData(List<List<String>> lines) {
    List<List<String>> result = new ArrayList<>();
    for (List<String> line : lines) {
      result.add(
          line.stream().map(cell -> quoteIfRequired(separator, cell)).collect(Collectors.toList()));
    }
    return result;
  }

  protected String quoteIfRequired(String separator, String cell) {
    final String quote = "\"";
    return cell.contains(separator) ? quote + cell.replaceAll("\"", "\"\"") + quote : cell;
  }
}
