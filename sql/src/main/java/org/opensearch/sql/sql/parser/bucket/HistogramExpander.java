/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql.parser.bucket;

import static org.opensearch.sql.sql.parser.bucket.BucketFunctionUtils.applyMissing;
import static org.opensearch.sql.sql.parser.bucket.BucketFunctionUtils.normalizeFieldRef;

import java.util.List;
import org.opensearch.sql.ast.dsl.AstDSL;
import org.opensearch.sql.ast.expression.Function;
import org.opensearch.sql.ast.expression.Span;
import org.opensearch.sql.ast.expression.SpanUnit;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.exception.SemanticCheckException;

/**
 * Lowers {@code histogram(...)} calls to a {@link Span} expression with {@code SpanUnit.NONE}.
 * Optional parameters wrap the bucket key:
 *
 * <ul>
 *   <li>{@code missing} — wraps the field with {@code COALESCE(field, missing)} before bucketing.
 *   <li>{@code offset} — wraps as {@code +(Span(-(field, offset), interval, NONE), offset)} to
 *       preserve the standard {@code [k*interval+offset, (k+1)*interval+offset)} boundaries.
 * </ul>
 *
 * <p>TODO: V1 also accepts the following parameters; they are currently rejected:
 *
 * <ul>
 *   <li>{@code min_doc_count} — would lower to {@code HAVING COUNT(*) >= N}. Needs parser-side
 *       plumbing to inject a HAVING clause from inside a scalar function call.
 *   <li>{@code order} — would lower to {@code ORDER BY}. Same plumbing requirement as above.
 *   <li>{@code alias} — would set the surrounding SELECT-list alias. Needs reaching outside the
 *       function call to mutate the parent SELECT element.
 * </ul>
 */
final class HistogramExpander implements BucketFunctionExpander {

  static final String FUNCTION_NAME = "HISTOGRAM";

  @Override
  public UnresolvedExpression expand(List<UnresolvedExpression> args) {
    if (!NamedArguments.isNamedArguments(args)) {
      throw new SemanticCheckException(
          "histogram requires named arguments: histogram('field'=<column>, 'interval'=<n>)");
    }
    NamedArguments named = NamedArguments.parse(args);
    UnresolvedExpression field = named.require("field", FUNCTION_NAME);
    UnresolvedExpression interval = named.require("interval", FUNCTION_NAME);
    UnresolvedExpression offset = named.remove("offset");
    UnresolvedExpression missing = named.remove("missing");
    named.rejectRemaining(FUNCTION_NAME);
    return buildBucket(field, interval, offset, missing);
  }

  private static UnresolvedExpression buildBucket(
      UnresolvedExpression field,
      UnresolvedExpression interval,
      UnresolvedExpression offsetOrNull,
      UnresolvedExpression missingOrNull) {
    UnresolvedExpression resolvedField = applyMissing(normalizeFieldRef(field), missingOrNull);
    if (offsetOrNull == null) {
      return AstDSL.span(resolvedField, interval, SpanUnit.NONE);
    }
    UnresolvedExpression shifted = new Function("-", List.of(resolvedField, offsetOrNull));
    Span bucket = (Span) AstDSL.span(shifted, interval, SpanUnit.NONE);
    return new Function("+", List.of(bucket, offsetOrNull));
  }
}
