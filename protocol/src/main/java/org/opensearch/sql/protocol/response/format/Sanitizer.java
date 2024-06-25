package org.opensearch.sql.protocol.response.format;

import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.opensearch.sql.protocol.response.QueryResult;

/**
 * Sanitize methods are migrated from legacy CSV result. Sanitize both headers and data lines by: 1)
 * Second double quote entire cell if any comma is found.
 */
public class Sanitizer extends FlatResponse {
  private static final Set<String> SENSITIVE_CHAR = ImmutableSet.of("=", "+", "-", "@");

  private final boolean sanitize;

  Sanitizer(QueryResult response, String inlineSeparator, boolean sanitize) {
    super(response, inlineSeparator);
    this.sanitize = sanitize;
  }

  /** Sanitize headers because OpenSearch allows special character present in field names. */
  @Override
  protected List<String> formatHeaders(List<String> headers) {
    if (sanitize) {
      return headers.stream()
          .map(this::sanitizeCell)
          .map(cell -> quoteIfRequired(separator, cell))
          .collect(Collectors.toList());
    } else {
      return super.formatHeaders(headers);
    }
  }

  @Override
  protected List<List<String>> formatData(List<List<String>> lines) {
    if (sanitize) {
      List<List<String>> result = new ArrayList<>();
      for (List<String> line : lines) {
        result.add(
            line.stream()
                .map(this::sanitizeCell)
                .map(cell -> quoteIfRequired(separator, cell))
                .collect(Collectors.toList()));
      }
      return result;
    } else {
      return super.formatData(lines);
    }
  }

  private String sanitizeCell(String cell) {
    if (isStartWithSensitiveChar(cell)) {
      return "'" + cell;
    }
    return cell;
  }

  private boolean isStartWithSensitiveChar(String cell) {
    return SENSITIVE_CHAR.stream().anyMatch(cell::startsWith);
  }
}
