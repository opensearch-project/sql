/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql.parser.bucket;

import java.util.List;
import org.opensearch.sql.ast.expression.UnresolvedExpression;

/**
 * Parse-time expander for a bucket function call. Each implementation lowers calls to one bucket
 * function (e.g. {@code histogram}) into standard SQL constructs the rest of the engine already
 * understands.
 *
 * <p>Implementations are stateless and registered by name in {@link BucketFunctionRegistry}.
 */
public interface BucketFunctionExpander {

  /** Lowers a bucket function call into its bucket-key expression. */
  UnresolvedExpression expand(List<UnresolvedExpression> args);
}
