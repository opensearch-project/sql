/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.protocol.response.format;

/** Response formatter to format response to different formats. */
public interface ResponseFormatter<R> {

  /**
   * Format response into string in expected format.
   *
   * @param response response
   * @return string with response content formatted
   */
  String format(R response);

  /**
   * Format an exception into string.
   *
   * @param t exception occurred
   * @return string with exception content formatted
   */
  String format(Throwable t);

  /**
   * Getter for the content type header of the response.
   *
   * @return string
   */
  String contentType();
}
