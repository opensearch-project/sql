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
class HistogramExpanderTest extends AstBuilderTestBase {

  private final HistogramExpander expander = new HistogramExpander();

  @Test
  void rejects_positional_invocation_with_clear_message() {
    SemanticCheckException ex =
        assertThrows(
            SemanticCheckException.class,
            () -> expander.expand(List.of(AstDSL.qualifiedName("price"), AstDSL.intLiteral(100))));
    assertTrue(ex.getMessage().contains("named arguments"));
    assertTrue(ex.getMessage().contains("histogram"));
  }

  @Test
  void property_bag_with_string_field_coerces_to_qualified_name() {
    UnresolvedExpression result =
        expander.expand(
            List.of(
                kv("field", AstDSL.stringLiteral("age")), kv("interval", AstDSL.intLiteral(10))));

    assertEquals(
        new Span(AstDSL.qualifiedName("age"), AstDSL.intLiteral(10), SpanUnit.NONE), result);
  }

  @Test
  void property_bag_with_qualified_name_field_passes_through_unchanged() {
    QualifiedName age = AstDSL.qualifiedName("age");
    UnresolvedExpression result =
        expander.expand(List.of(kv("field", age), kv("interval", AstDSL.intLiteral(10))));

    assertEquals(new Span(age, AstDSL.intLiteral(10), SpanUnit.NONE), result);
  }

  @Test
  void property_bag_rejects_alias() {
    assertThrows(
        SemanticCheckException.class,
        () ->
            expander.expand(
                List.of(
                    kv("field", AstDSL.stringLiteral("age")),
                    kv("interval", AstDSL.intLiteral(10)),
                    kv("alias", AstDSL.stringLiteral("my_label")))));
  }

  @Test
  void property_bag_rejects_nested() {
    assertThrows(
        SemanticCheckException.class,
        () ->
            expander.expand(
                List.of(
                    kv("field", AstDSL.stringLiteral("age")),
                    kv("interval", AstDSL.intLiteral(10)),
                    kv("nested", AstDSL.stringLiteral("path")))));
  }

  @Test
  void property_bag_rejects_reverse_nested() {
    assertThrows(
        SemanticCheckException.class,
        () ->
            expander.expand(
                List.of(
                    kv("field", AstDSL.stringLiteral("age")),
                    kv("interval", AstDSL.intLiteral(10)),
                    kv("reverse_nested", AstDSL.stringLiteral("path")))));
  }

  @Test
  void property_bag_rejects_children() {
    assertThrows(
        SemanticCheckException.class,
        () ->
            expander.expand(
                List.of(
                    kv("field", AstDSL.stringLiteral("age")),
                    kv("interval", AstDSL.intLiteral(10)),
                    kv("children", AstDSL.stringLiteral("ignored")))));
  }

  @Test
  void property_bag_rejects_format() {
    assertThrows(
        SemanticCheckException.class,
        () ->
            expander.expand(
                List.of(
                    kv("field", AstDSL.stringLiteral("age")),
                    kv("interval", AstDSL.intLiteral(10)),
                    kv("format", AstDSL.stringLiteral("yyyy")))));
  }

  @Test
  void property_bag_rejects_time_zone() {
    assertThrows(
        SemanticCheckException.class,
        () ->
            expander.expand(
                List.of(
                    kv("field", AstDSL.stringLiteral("age")),
                    kv("interval", AstDSL.intLiteral(10)),
                    kv("time_zone", AstDSL.stringLiteral("+05:30")))));
  }

  @Test
  void property_bag_rejects_min_doc_count() {
    assertThrows(
        SemanticCheckException.class,
        () ->
            expander.expand(
                List.of(
                    kv("field", AstDSL.stringLiteral("age")),
                    kv("interval", AstDSL.intLiteral(10)),
                    kv("min_doc_count", AstDSL.intLiteral(5)))));
  }

  @Test
  void property_bag_rejects_order() {
    assertThrows(
        SemanticCheckException.class,
        () ->
            expander.expand(
                List.of(
                    kv("field", AstDSL.stringLiteral("age")),
                    kv("interval", AstDSL.intLiteral(10)),
                    kv("order", AstDSL.stringLiteral("count_desc")))));
  }

  @Test
  void property_bag_rejects_extended_bounds() {
    assertThrows(
        SemanticCheckException.class,
        () ->
            expander.expand(
                List.of(
                    kv("field", AstDSL.stringLiteral("age")),
                    kv("interval", AstDSL.intLiteral(10)),
                    kv("extended_bounds", AstDSL.stringLiteral("0:100")))));
  }

  @Test
  void property_bag_rejects_unknown_param() {
    SemanticCheckException ex =
        assertThrows(
            SemanticCheckException.class,
            () ->
                expander.expand(
                    List.of(
                        kv("field", AstDSL.stringLiteral("age")),
                        kv("interval", AstDSL.intLiteral(10)),
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
                    kv("field", AstDSL.stringLiteral("age")),
                    kv("field", AstDSL.stringLiteral("size")),
                    kv("interval", AstDSL.intLiteral(10)))));
  }

  @Test
  void property_bag_rejects_missing_field() {
    assertThrows(
        SemanticCheckException.class,
        () -> expander.expand(List.of(kv("interval", AstDSL.intLiteral(10)))));
  }

  @Test
  void property_bag_rejects_missing_interval() {
    assertThrows(
        SemanticCheckException.class,
        () -> expander.expand(List.of(kv("field", AstDSL.stringLiteral("age")))));
  }

  @Test
  void property_bag_offset_shifts_bucket_boundaries() {
    UnresolvedExpression result =
        expander.expand(
            List.of(
                kv("field", AstDSL.stringLiteral("age")),
                kv("interval", AstDSL.intLiteral(10)),
                kv("offset", AstDSL.intLiteral(3))));

    QualifiedName age = AstDSL.qualifiedName("age");
    Function shiftedField = new Function("-", List.of(age, AstDSL.intLiteral(3)));
    Span bucket = new Span(shiftedField, AstDSL.intLiteral(10), SpanUnit.NONE);
    Function expected = new Function("+", List.of(bucket, AstDSL.intLiteral(3)));
    assertEquals(expected, result);
  }

  @Test
  void property_bag_missing_wraps_field_with_coalesce() {
    UnresolvedExpression result =
        expander.expand(
            List.of(
                kv("field", AstDSL.stringLiteral("age")),
                kv("interval", AstDSL.intLiteral(10)),
                kv("missing", AstDSL.intLiteral(0))));

    Function coalesced =
        new Function("coalesce", List.of(AstDSL.qualifiedName("age"), AstDSL.intLiteral(0)));
    assertEquals(new Span(coalesced, AstDSL.intLiteral(10), SpanUnit.NONE), result);
  }

  @Test
  void property_bag_offset_and_missing_compose_in_correct_order() {
    UnresolvedExpression result =
        expander.expand(
            List.of(
                kv("field", AstDSL.stringLiteral("age")),
                kv("interval", AstDSL.intLiteral(10)),
                kv("offset", AstDSL.intLiteral(3)),
                kv("missing", AstDSL.intLiteral(0))));

    QualifiedName age = AstDSL.qualifiedName("age");
    Function coalesced = new Function("coalesce", List.of(age, AstDSL.intLiteral(0)));
    Function shifted = new Function("-", List.of(coalesced, AstDSL.intLiteral(3)));
    Span bucket = new Span(shifted, AstDSL.intLiteral(10), SpanUnit.NONE);
    Function expected = new Function("+", List.of(bucket, AstDSL.intLiteral(3)));
    assertEquals(expected, result);
  }

  @Test
  void via_sql_lowers_to_span() {
    QualifiedName age = AstDSL.qualifiedName("age");
    Span bucket = AstDSL.span(age, AstDSL.intLiteral(10), SpanUnit.NONE);

    UnresolvedPlan result =
        buildAST(
            "SELECT histogram('field'='age', 'interval'=10), COUNT(*) FROM accounts "
                + "GROUP BY histogram('field'='age', 'interval'=10)");

    assertEquals(
        AstDSL.project(
            AstDSL.agg(
                AstDSL.relation("accounts"),
                ImmutableList.of(
                    AstDSL.alias("COUNT(*)", AstDSL.aggregate("COUNT", AllFields.of()))),
                emptyList(),
                ImmutableList.of(AstDSL.alias(bucket.toString(), bucket)),
                emptyList()),
            AstDSL.alias("histogram('field'='age', 'interval'=10)", bucket),
            AstDSL.alias("COUNT(*)", AstDSL.aggregate("COUNT", AllFields.of()))),
        result);
  }

  @Test
  void via_sql_rejects_positional_invocation() {
    assertThrows(
        SemanticCheckException.class,
        () -> buildAST("SELECT histogram(price, 100) FROM orders GROUP BY histogram(price, 100)"));
  }

  /** Builds a Function("=", [stringLiteral(key), value]) — same shape ANTLR produces for 'k'=v. */
  private static UnresolvedExpression kv(String key, UnresolvedExpression value) {
    return new Function("=", List.of(AstDSL.stringLiteral(key), value));
  }
}
