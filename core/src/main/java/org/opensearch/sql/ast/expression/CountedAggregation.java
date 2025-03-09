/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.expression;

import java.util.Optional;

/** marker interface for numeric based count aggregation (specific number of returned results) */
public interface CountedAggregation {
  Optional<Literal> getResults();
}
