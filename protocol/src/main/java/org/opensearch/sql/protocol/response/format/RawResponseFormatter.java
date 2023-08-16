/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.protocol.response.format;

/** Response formatter to format response to csv or raw format. */
public class RawResponseFormatter extends FlatResponseFormatter {
  public RawResponseFormatter() {
    super("|", false);
  }
}
