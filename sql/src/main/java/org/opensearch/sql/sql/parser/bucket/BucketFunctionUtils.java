/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql.parser.bucket;

import java.util.List;
import org.opensearch.sql.ast.dsl.AstDSL;
import org.opensearch.sql.ast.expression.DataType;
import org.opensearch.sql.ast.expression.Function;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.expression.UnresolvedExpression;

/**
 * Shared parameter helpers for bucket-function expanders. Operates on values pulled from a {@link
 * NamedArguments} or from a positional argument list.
 */
final class BucketFunctionUtils {

  private BucketFunctionUtils() {}

  /**
   * Named-argument form accepts string-literal field names ({@code 'field'='age'}). Coerce them to
   * {@link QualifiedName} so downstream sees a column reference regardless of how the user spelled
   * it.
   */
  static UnresolvedExpression normalizeFieldRef(UnresolvedExpression expr) {
    if (expr instanceof Literal lit && lit.getType() == DataType.STRING) {
      return AstDSL.qualifiedName(lit.getValue().toString());
    }
    return expr;
  }

  /** If {@code missingOrNull} is non-null, wrap field with {@code COALESCE(field, missing)}. */
  static UnresolvedExpression applyMissing(
      UnresolvedExpression field, UnresolvedExpression missingOrNull) {
    if (missingOrNull == null) {
      return field;
    }
    return new Function("coalesce", List.of(field, missingOrNull));
  }
}
