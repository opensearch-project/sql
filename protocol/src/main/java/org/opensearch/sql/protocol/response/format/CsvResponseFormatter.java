/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.protocol.response.format;

import org.opensearch.sql.protocol.response.QueryResult;

/** Response formatter to format response to csv format. */
public class CsvResponseFormatter implements ResponseFormatter<QueryResult> {
  public static final String CONTENT_TYPE = "plain/text; charset=UTF-8";
  private final String separator;
  private final boolean sanitize;

  public CsvResponseFormatter() {
    this(",", true);
  }

  public CsvResponseFormatter(boolean sanitize) {
    this(",", sanitize);
  }

  public CsvResponseFormatter(String separator, boolean sanitize) {
    this.separator = separator;
    this.sanitize = sanitize;
  }

  @Override
  public String format(QueryResult response) {
    FlatResponseBase flatResponse;
    if (sanitize) {
      flatResponse = new FlatResponseWithSanitizer(response, separator);
    } else {
      flatResponse = new FlatResponseBase(response, separator);
    }
    return flatResponse.format();
  }

  @Override
  public String format(Throwable t) {
    return ErrorFormatter.prettyFormat(t);
  }

  @Override
  public String contentType() {
    return CONTENT_TYPE;
  }
}
