/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.protocol.response.format;

public class CsvResponseFormatter extends FlatResponseFormatter {
  public CsvResponseFormatter() {
    super(",", true);
  }

  public CsvResponseFormatter(boolean sanitize) {
    super(",", sanitize);
  }
}
