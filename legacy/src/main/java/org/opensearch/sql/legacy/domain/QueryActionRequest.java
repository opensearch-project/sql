/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.domain;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.legacy.executor.Format;

/** The definition of QueryActionRequest. */
@Getter
@RequiredArgsConstructor
public class QueryActionRequest {
  private final String sql;
  private final ColumnTypeProvider typeProvider;
  private final Format format;
}
