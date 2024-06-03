/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.protocol.response.format;

/** Response formatter to format response to raw format. */
public class RawResponseFormatter extends FlatResponseFormatter {
  public RawResponseFormatter() {
    super("|", false, false);
  }

  /**
   * Create a raw response formatter with pretty parameter.
   *
   * @param pretty if true, display the columns with proper padding. Tracks the maximum width for
   *     each column to ensure proper formatting.
   */
  public RawResponseFormatter(boolean pretty) {
    super("|", false, pretty);
  }
}
