/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql.parser.bucket;

import static org.opensearch.sql.sql.parser.bucket.BucketFunctionUtils.applyMissing;
import static org.opensearch.sql.sql.parser.bucket.BucketFunctionUtils.normalizeFieldRef;

import java.time.ZoneOffset;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;
import org.opensearch.sql.ast.dsl.AstDSL;
import org.opensearch.sql.ast.expression.Function;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.expression.Span;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.exception.SemanticCheckException;

/**
 * Lowers {@code date_histogram(...)} calls to a {@link Span} expression with the time unit inferred
 * from the interval string. Optional parameters wrap the bucket key:
 *
 * <ul>
 *   <li>{@code missing} — wraps the field with {@code COALESCE(field, missing)} before bucketing.
 *   <li>{@code time_zone} — shifts the field with {@code TIMESTAMPADD(SECOND, offset, field)}
 *       before bucketing. Validated as a {@link java.time.ZoneOffset} at parse time.
 *   <li>{@code format} — wraps the bucket with {@code DATE_FORMAT(span, format)}.
 * </ul>
 *
 * <p>{@code interval}, {@code fixed_interval}, and {@code calendar_interval} are accepted as
 * mutually-exclusive syntactic synonyms; this lowering does not preserve the calendar-vs-fixed
 * distinction across them.
 *
 * <p>TODO: V1 also accepts the following parameters; they are currently rejected:
 *
 * <ul>
 *   <li>{@code min_doc_count} — would lower to {@code HAVING COUNT(*) >= N}. Needs parser-side
 *       plumbing to inject a HAVING clause from inside a scalar function call.
 *   <li>{@code order} — would lower to {@code ORDER BY}. Same plumbing requirement as above.
 *   <li>{@code alias} — would set the surrounding SELECT-list alias. Needs reaching outside the
 *       function call to mutate the parent SELECT element.
 *   <li>{@code offset} — would shift bucket boundaries via {@code TIMESTAMPADD(SECOND, -offset,
 *       field)} before bucketing and {@code TIMESTAMPADD(SECOND, offset, span)} after. Needs a
 *       duration-string parser ({@code '1h'}, {@code '2d'}, etc.) distinct from {@code time_zone}'s
 *       {@code ZoneOffset} format.
 * </ul>
 */
final class DateHistogramExpander implements BucketFunctionExpander {

  static final String FUNCTION_NAME = "DATE_HISTOGRAM";

  @Override
  public UnresolvedExpression expand(List<UnresolvedExpression> args) {
    if (!NamedArguments.isNamedArguments(args)) {
      throw new SemanticCheckException(
          "date_histogram requires named arguments: date_histogram('field'=<column>,"
              + " 'interval'=<duration>)");
    }
    NamedArguments named = NamedArguments.parse(args);
    UnresolvedExpression field = named.require("field", FUNCTION_NAME);
    Literal intervalLiteral = extractIntervalLiteral(named);
    Literal formatLiteral = named.requireStringIfPresent("format");
    Literal timeZoneLiteral = named.requireStringIfPresent("time_zone");
    UnresolvedExpression missing = named.remove("missing");
    named.rejectRemaining(FUNCTION_NAME);
    return buildBucket(field, intervalLiteral, formatLiteral, timeZoneLiteral, missing);
  }

  /**
   * Pulls the interval from the named arguments accepting any of {@code interval}, {@code
   * fixed_interval}, {@code calendar_interval}. Exactly one must be present.
   */
  private static Literal extractIntervalLiteral(NamedArguments named) {
    Literal interval = named.requireStringIfPresent("interval");
    Literal fixedInterval = named.requireStringIfPresent("fixed_interval");
    Literal calendarInterval = named.requireStringIfPresent("calendar_interval");

    List<Literal> suppliedIntervals =
        Stream.of(interval, fixedInterval, calendarInterval).filter(Objects::nonNull).toList();

    if (suppliedIntervals.isEmpty()) {
      throw new SemanticCheckException(
          "date_histogram requires one of: interval, fixed_interval, calendar_interval");
    }
    if (suppliedIntervals.size() > 1) {
      throw new SemanticCheckException(
          "date_histogram accepts only one of: interval, fixed_interval, calendar_interval");
    }
    return suppliedIntervals.get(0);
  }

  private static UnresolvedExpression buildBucket(
      UnresolvedExpression field,
      Literal intervalLiteral,
      Literal formatLiteral,
      Literal timeZoneLiteral,
      UnresolvedExpression missingOrNull) {
    UnresolvedExpression resolvedField = applyMissing(normalizeFieldRef(field), missingOrNull);
    UnresolvedExpression shiftedField =
        timeZoneLiteral != null
            ? applyTimeZoneShift(resolvedField, timeZoneLiteral)
            : resolvedField;
    Span span = AstDSL.spanFromSpanLengthLiteral(shiftedField, intervalLiteral);
    if (formatLiteral == null) {
      return span;
    }
    return new Function("date_format", List.of(span, formatLiteral));
  }

  /**
   * Wraps the field with a {@code TIMESTAMPADD(SECOND, offset, field)} shift derived from a
   * timezone literal. Validates the literal at parse time as a {@link ZoneOffset} (e.g. {@code
   * '+05:30'}, {@code 'Z'}); runtime arithmetic is plain second addition.
   */
  private static UnresolvedExpression applyTimeZoneShift(
      UnresolvedExpression field, Literal timeZoneLiteral) {
    String tzString = timeZoneLiteral.getValue().toString();
    int offsetSeconds;
    try {
      offsetSeconds = ZoneOffset.of(tzString).getTotalSeconds();
    } catch (RuntimeException ex) {
      throw new SemanticCheckException(
          "time_zone must be a valid offset like '+05:30' or 'Z'; got '" + tzString + "'");
    }
    return new Function(
        "timestampadd",
        List.of(AstDSL.stringLiteral("SECOND"), AstDSL.intLiteral(offsetSeconds), field));
  }
}
