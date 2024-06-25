/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.protocol.response.format;

import org.opensearch.sql.protocol.response.QueryResult;

/** Response formatter to format response to raw format. */
public class RawResponseFormatter implements ResponseFormatter<QueryResult> {
  public static final String CONTENT_TYPE = "plain/text; charset=UTF-8";
  private final String separator;
  private final boolean pretty;

  public RawResponseFormatter() {
    this("|", false);
  }

  public RawResponseFormatter(boolean pretty) {
    this("|", pretty);
  }

  /**
   * Create a raw response formatter with separator and pretty parameter.
   *
   * @param pretty if true, display the columns with proper padding. Tracks the maximum width for
   *     each column to ensure proper formatting.
   */
  public RawResponseFormatter(String separator, boolean pretty) {
    this.separator = separator;
    this.pretty = pretty;
  }

  @Override
  public String format(QueryResult response) {
    FlatResponseBase flatResponse;
    if (pretty) {
      flatResponse = new FlatResponseWithPrettifier(response, separator);
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
