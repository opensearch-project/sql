/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

/*
 *   Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
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
