/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.doctest.core.builder;

import static org.opensearch.sql.doctest.core.response.SqlResponseFormat.TABLE_RESPONSE;
import static org.opensearch.sql.doctest.core.response.SqlResponseFormat.TABLE_UNSORTED_RESPONSE;

import org.opensearch.sql.doctest.core.request.SqlRequest;
import org.opensearch.sql.doctest.core.request.SqlRequestFormat;
import org.opensearch.sql.doctest.core.response.SqlResponse;
import org.opensearch.sql.doctest.core.response.SqlResponseFormat;

/**
 * Request and response format tuple.
 */
class Formats {

  private final SqlRequestFormat requestFormat;
  private final SqlResponseFormat responseFormat;

  Formats(SqlRequestFormat requestFormat, SqlResponseFormat responseFormat) {
    this.requestFormat = requestFormat;
    this.responseFormat = responseFormat;
  }

  String format(SqlRequest request) {
    return requestFormat.format(request);
  }

  String format(SqlResponse response) {
    return responseFormat.format(response);
  }

  boolean isTableFormat() {
    return responseFormat == TABLE_RESPONSE || responseFormat == TABLE_UNSORTED_RESPONSE;
  }
}
