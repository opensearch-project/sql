/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql.parser.bucket;

import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ast.dsl.AstDSL;
import org.opensearch.sql.ast.expression.AllFields;
import org.opensearch.sql.ast.expression.Function;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.expression.Span;
import org.opensearch.sql.ast.expression.SpanUnit;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.sql.parser.AstBuilderTestBase;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class DateHistogramExpanderTest extends AstBuilderTestBase {

  private final DateHistogramExpander expander = new DateHistogramExpander();

  @Test
  void rejects_positional_invocation_with_clear_message() {
    SemanticCheckException ex =
        assertThrows(
            SemanticCheckException.class,
            () -> expander.expand(List.of(AstDSL.qualifiedName("ts"), AstDSL.stringLiteral("1d"))));
    assertTrue(ex.getMessage().contains("named arguments"));
    assertTrue(ex.getMessage().contains("date_histogram"));
  }

  @Test
  void property_bag_with_interval_param_lowers_to_span() {
    UnresolvedExpression result =
        expander.expand(
            List.of(
                kv("field", AstDSL.stringLiteral("ts")),
                kv("interval", AstDSL.stringLiteral("1d"))));

    assertEquals(new Span(AstDSL.qualifiedName("ts"), AstDSL.intLiteral(1), SpanUnit.D), result);
  }

  @Test
  void property_bag_with_qualified_name_field_passes_through_unchanged() {
    QualifiedName ts = AstDSL.qualifiedName("ts");
    UnresolvedExpression result =
        expander.expand(List.of(kv("field", ts), kv("interval", AstDSL.stringLiteral("1d"))));

    assertEquals(new Span(ts, AstDSL.intLiteral(1), SpanUnit.D), result);
  }

  @Test
  void property_bag_with_fixed_interval_param_lowers_to_span() {
    UnresolvedExpression result =
        expander.expand(
            List.of(
                kv("field", AstDSL.stringLiteral("ts")),
                kv("fixed_interval", AstDSL.stringLiteral("15m"))));

    assertEquals(new Span(AstDSL.qualifiedName("ts"), AstDSL.intLiteral(15), SpanUnit.m), result);
  }

  @Test
  void property_bag_with_calendar_interval_param_lowers_to_span() {
    UnresolvedExpression result =
        expander.expand(
            List.of(
                kv("field", AstDSL.stringLiteral("ts")),
                kv("calendar_interval", AstDSL.stringLiteral("1d"))));

    assertEquals(new Span(AstDSL.qualifiedName("ts"), AstDSL.intLiteral(1), SpanUnit.D), result);
  }

  @Test
  void property_bag_rejects_both_interval_and_fixed_interval() {
    assertThrows(
        SemanticCheckException.class,
        () ->
            expander.expand(
                List.of(
                    kv("field", AstDSL.stringLiteral("ts")),
                    kv("interval", AstDSL.stringLiteral("1d")),
                    kv("fixed_interval", AstDSL.stringLiteral("15m")))));
  }

  @Test
  void property_bag_format_wraps_with_date_format() {
    UnresolvedExpression result =
        expander.expand(
            List.of(
                kv("field", AstDSL.stringLiteral("ts")),
                kv("interval", AstDSL.stringLiteral("1d")),
                kv("format", AstDSL.stringLiteral("yyyy-MM-dd"))));

    Span innerSpan = new Span(AstDSL.qualifiedName("ts"), AstDSL.intLiteral(1), SpanUnit.D);
    Function expected =
        new Function("date_format", List.of(innerSpan, AstDSL.stringLiteral("yyyy-MM-dd")));
    assertEquals(expected, result);
  }

  @Test
  void property_bag_time_zone_wraps_field_with_timestampadd() {
    UnresolvedExpression result =
        expander.expand(
            List.of(
                kv("field", AstDSL.stringLiteral("ts")),
                kv("interval", AstDSL.stringLiteral("1d")),
                kv("time_zone", AstDSL.stringLiteral("+05:30"))));

    // +05:30 = 5*3600 + 30*60 = 19800 seconds
    Function shiftedField =
        new Function(
            "timestampadd",
            List.of(
                AstDSL.stringLiteral("SECOND"),
                AstDSL.intLiteral(19800),
                AstDSL.qualifiedName("ts")));
    Span expected = new Span(shiftedField, AstDSL.intLiteral(1), SpanUnit.D);
    assertEquals(expected, result);
  }

  @Test
  void property_bag_format_and_time_zone_compose() {
    UnresolvedExpression result =
        expander.expand(
            List.of(
                kv("field", AstDSL.stringLiteral("ts")),
                kv("interval", AstDSL.stringLiteral("1d")),
                kv("format", AstDSL.stringLiteral("yyyy")),
                kv("time_zone", AstDSL.stringLiteral("Z"))));

    // Z = 0 offset
    Function shiftedField =
        new Function(
            "timestampadd",
            List.of(
                AstDSL.stringLiteral("SECOND"), AstDSL.intLiteral(0), AstDSL.qualifiedName("ts")));
    Span innerSpan = new Span(shiftedField, AstDSL.intLiteral(1), SpanUnit.D);
    Function expected =
        new Function("date_format", List.of(innerSpan, AstDSL.stringLiteral("yyyy")));
    assertEquals(expected, result);
  }

  @Test
  void property_bag_rejects_invalid_time_zone() {
    SemanticCheckException ex =
        assertThrows(
            SemanticCheckException.class,
            () ->
                expander.expand(
                    List.of(
                        kv("field", AstDSL.stringLiteral("ts")),
                        kv("interval", AstDSL.stringLiteral("1d")),
                        kv("time_zone", AstDSL.stringLiteral("not-a-tz")))));
    assertTrue(ex.getMessage().contains("time_zone"));
  }

  @Test
  void property_bag_rejects_alias() {
    assertThrows(
        SemanticCheckException.class,
        () ->
            expander.expand(
                List.of(
                    kv("field", AstDSL.stringLiteral("ts")),
                    kv("interval", AstDSL.stringLiteral("1d")),
                    kv("alias", AstDSL.stringLiteral("my_label")))));
  }

  @Test
  void property_bag_rejects_nested() {
    assertThrows(
        SemanticCheckException.class,
        () ->
            expander.expand(
                List.of(
                    kv("field", AstDSL.stringLiteral("ts")),
                    kv("interval", AstDSL.stringLiteral("1d")),
                    kv("nested", AstDSL.stringLiteral("path")))));
  }

  @Test
  void property_bag_rejects_reverse_nested() {
    assertThrows(
        SemanticCheckException.class,
        () ->
            expander.expand(
                List.of(
                    kv("field", AstDSL.stringLiteral("ts")),
                    kv("interval", AstDSL.stringLiteral("1d")),
                    kv("reverse_nested", AstDSL.stringLiteral("path")))));
  }

  @Test
  void property_bag_rejects_children() {
    assertThrows(
        SemanticCheckException.class,
        () ->
            expander.expand(
                List.of(
                    kv("field", AstDSL.stringLiteral("ts")),
                    kv("interval", AstDSL.stringLiteral("1d")),
                    kv("children", AstDSL.stringLiteral("ignored")))));
  }

  @Test
  void property_bag_missing_wraps_field_with_coalesce() {
    UnresolvedExpression result =
        expander.expand(
            List.of(
                kv("field", AstDSL.stringLiteral("ts")),
                kv("interval", AstDSL.stringLiteral("1d")),
                kv("missing", AstDSL.stringLiteral("2024-01-01"))));

    Function coalesced =
        new Function(
            "coalesce", List.of(AstDSL.qualifiedName("ts"), AstDSL.stringLiteral("2024-01-01")));
    assertEquals(new Span(coalesced, AstDSL.intLiteral(1), SpanUnit.D), result);
  }

  @Test
  void property_bag_rejects_offset() {
    SemanticCheckException ex =
        assertThrows(
            SemanticCheckException.class,
            () ->
                expander.expand(
                    List.of(
                        kv("field", AstDSL.stringLiteral("ts")),
                        kv("interval", AstDSL.stringLiteral("1d")),
                        kv("offset", AstDSL.stringLiteral("1h")))));
    assertTrue(ex.getMessage().contains("offset"));
    assertTrue(ex.getMessage().contains("does not accept"));
  }

  @Test
  void property_bag_rejects_min_doc_count() {
    assertThrows(
        SemanticCheckException.class,
        () ->
            expander.expand(
                List.of(
                    kv("field", AstDSL.stringLiteral("ts")),
                    kv("interval", AstDSL.stringLiteral("1d")),
                    kv("min_doc_count", AstDSL.intLiteral(5)))));
  }

  @Test
  void property_bag_rejects_extended_bounds() {
    assertThrows(
        SemanticCheckException.class,
        () ->
            expander.expand(
                List.of(
                    kv("field", AstDSL.stringLiteral("ts")),
                    kv("interval", AstDSL.stringLiteral("1d")),
                    kv("extended_bounds", AstDSL.stringLiteral("a:b")))));
  }

  @Test
  void property_bag_rejects_unknown_param() {
    SemanticCheckException ex =
        assertThrows(
            SemanticCheckException.class,
            () ->
                expander.expand(
                    List.of(
                        kv("field", AstDSL.stringLiteral("ts")),
                        kv("interval", AstDSL.stringLiteral("1d")),
                        kv("missing_param", AstDSL.stringLiteral("foo")))));
    assertTrue(ex.getMessage().contains("missing_param"));
  }

  @Test
  void property_bag_rejects_duplicate_keys() {
    assertThrows(
        SemanticCheckException.class,
        () ->
            expander.expand(
                List.of(
                    kv("field", AstDSL.stringLiteral("ts")),
                    kv("field", AstDSL.stringLiteral("created_at")),
                    kv("interval", AstDSL.stringLiteral("1d")))));
  }

  @Test
  void property_bag_rejects_missing_field() {
    assertThrows(
        SemanticCheckException.class,
        () -> expander.expand(List.of(kv("interval", AstDSL.stringLiteral("1d")))));
  }

  @Test
  void property_bag_rejects_when_no_interval_synonym_provided() {
    SemanticCheckException ex =
        assertThrows(
            SemanticCheckException.class,
            () -> expander.expand(List.of(kv("field", AstDSL.stringLiteral("ts")))));
    assertTrue(ex.getMessage().contains("requires one of"));
  }

  @Test
  void via_sql_with_interval_param_lowers_to_span() {
    QualifiedName ts = AstDSL.qualifiedName("ts");
    Span bucket = AstDSL.span(ts, AstDSL.intLiteral(1), SpanUnit.D);

    UnresolvedPlan result =
        buildAST(
            "SELECT date_histogram('field'='ts', 'interval'='1d'), COUNT(*) FROM events "
                + "GROUP BY date_histogram('field'='ts', 'interval'='1d')");

    assertEquals(
        AstDSL.project(
            AstDSL.agg(
                AstDSL.relation("events"),
                ImmutableList.of(
                    AstDSL.alias("COUNT(*)", AstDSL.aggregate("COUNT", AllFields.of()))),
                emptyList(),
                ImmutableList.of(AstDSL.alias(bucket.toString(), bucket)),
                emptyList()),
            AstDSL.alias("date_histogram('field'='ts', 'interval'='1d')", bucket),
            AstDSL.alias("COUNT(*)", AstDSL.aggregate("COUNT", AllFields.of()))),
        result);
  }

  @Test
  void via_sql_rejects_positional_invocation() {
    assertThrows(
        SemanticCheckException.class,
        () ->
            buildAST(
                "SELECT date_histogram(ts, '1d') FROM events GROUP BY date_histogram(ts, '1d')"));
  }

  /** Builds a Function("=", [stringLiteral(key), value]) — same shape ANTLR produces for 'k'=v. */
  private static UnresolvedExpression kv(String key, UnresolvedExpression value) {
    return new Function("=", List.of(AstDSL.stringLiteral(key), value));
  }
}
