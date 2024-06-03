/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.protocol.response.format;

/** Response formatter to format response to csv format. */
public class CsvResponseFormatter extends FlatResponseFormatter {
  public CsvResponseFormatter() {
    super(",", true, false);
  }

  public CsvResponseFormatter(boolean sanitize) {
    super(",", sanitize, false);
  }
}
