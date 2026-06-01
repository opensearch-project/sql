/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql.parser;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ast.dsl.AstDSL;
import org.opensearch.sql.ast.expression.AggregateFunction;
import org.opensearch.sql.ast.expression.Alias;
import org.opensearch.sql.ast.expression.Function;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.expression.UnresolvedExpression;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class CompoundAggregateExpanderTest {

  @Test
  void recognizes_stats_case_insensitively() {
    assertTrue(CompoundAggregateExpander.isCompoundName("STATS"));
    assertTrue(CompoundAggregateExpander.isCompoundName("stats"));
    assertTrue(CompoundAggregateExpander.isCompoundName("Stats"));
  }

  @Test
  void recognizes_extended_stats_case_insensitively() {
    assertTrue(CompoundAggregateExpander.isCompoundName("EXTENDED_STATS"));
    assertTrue(CompoundAggregateExpander.isCompoundName("extended_stats"));
  }

  @Test
  void rejects_non_compound_names() {
    assertFalse(CompoundAggregateExpander.isCompoundName("SUM"));
    assertFalse(CompoundAggregateExpander.isCompoundName("AVG"));
    assertFalse(CompoundAggregateExpander.isCompoundName(""));
    assertFalse(CompoundAggregateExpander.isCompoundName(null));
  }

  @Test
  void stats_expands_to_five_primitives_in_canonical_order() {
    QualifiedName price = AstDSL.qualifiedName("price");

    List<UnresolvedExpression> primitives =
        CompoundAggregateExpander.expandPrimitives("STATS", price, null);

    assertEquals(
        ImmutableList.of(
            new AggregateFunction("count", price),
            new AggregateFunction("sum", price),
            new AggregateFunction("avg", price),
            new AggregateFunction("min", price),
            new AggregateFunction("max", price)),
        primitives);
  }

  @Test
  void extended_stats_expands_to_eight_primitives_in_canonical_order() {
    QualifiedName price = AstDSL.qualifiedName("price");

    List<UnresolvedExpression> primitives =
        CompoundAggregateExpander.expandPrimitives("EXTENDED_STATS", price, null);

    assertEquals(
        ImmutableList.of(
            new AggregateFunction("count", price),
            new AggregateFunction("sum", price),
            new AggregateFunction("avg", price),
            new AggregateFunction("min", price),
            new AggregateFunction("max", price),
            new AggregateFunction("sum", new Function("*", List.of(price, price))),
            new AggregateFunction("var_pop", price),
            new AggregateFunction("stddev_pop", price)),
        primitives);
  }

  @Test
  void aliased_without_display_prefix_yields_internal_names_only() {
    QualifiedName price = AstDSL.qualifiedName("price");

    List<UnresolvedExpression> aliased =
        CompoundAggregateExpander.expandAliased("EXTENDED_STATS", price, "price", null, null);

    assertEquals(
        ImmutableList.of(
            "count(price)",
            "sum(price)",
            "avg(price)",
            "min(price)",
            "max(price)",
            "sumOfSquares(price)",
            "variance(price)",
            "stdDeviation(price)"),
        aliased.stream().map(a -> ((Alias) a).getName()).toList());
    aliased.forEach(a -> assertNull(((Alias) a).getAlias()));
  }

  @Test
  void aliased_with_display_prefix_emits_dot_separated_display_names() {
    QualifiedName price = AstDSL.qualifiedName("price");

    List<UnresolvedExpression> aliased =
        CompoundAggregateExpander.expandAliased("EXTENDED_STATS", price, "price", "x", null);

    assertEquals(
        ImmutableList.of(
            "count(price)",
            "sum(price)",
            "avg(price)",
            "min(price)",
            "max(price)",
            "sumOfSquares(price)",
            "variance(price)",
            "stdDeviation(price)"),
        aliased.stream().map(a -> ((Alias) a).getName()).toList());
    assertEquals(
        ImmutableList.of(
            "x.count",
            "x.sum",
            "x.avg",
            "x.min",
            "x.max",
            "x.sumOfSquares",
            "x.variance",
            "x.stdDeviation"),
        aliased.stream().map(a -> ((Alias) a).getAlias()).toList());
  }

  @Test
  void source_text_display_prefix_emits_call_dot_suffix_format() {
    // No-AS scenario: displayPrefix is the source text of the call.
    QualifiedName price = AstDSL.qualifiedName("price");

    List<UnresolvedExpression> aliased =
        CompoundAggregateExpander.expandAliased("STATS", price, "price", "STATS(price)", null);

    assertEquals(
        ImmutableList.of(
            "STATS(price).count",
            "STATS(price).sum",
            "STATS(price).avg",
            "STATS(price).min",
            "STATS(price).max"),
        aliased.stream().map(a -> ((Alias) a).getAlias()).toList());
  }

  @Test
  void condition_propagates_to_each_primitive_when_provided() {
    QualifiedName price = AstDSL.qualifiedName("price");
    UnresolvedExpression condition = AstDSL.function(">", price, AstDSL.intLiteral(0));

    List<UnresolvedExpression> primitives =
        CompoundAggregateExpander.expandPrimitives("EXTENDED_STATS", price, condition);

    primitives.forEach(expr -> assertEquals(condition, ((AggregateFunction) expr).condition()));
  }

  @Test
  void condition_is_null_on_each_primitive_when_not_provided() {
    QualifiedName price = AstDSL.qualifiedName("price");

    List<UnresolvedExpression> primitives =
        CompoundAggregateExpander.expandPrimitives("STATS", price, null);

    primitives.forEach(expr -> assertNull(((AggregateFunction) expr).condition()));
  }

  @Test
  void expand_primitives_rejects_unknown_compound_name() {
    QualifiedName price = AstDSL.qualifiedName("price");
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> CompoundAggregateExpander.expandPrimitives("PERCENTILES", price, null));
    assertTrue(ex.getMessage().contains("PERCENTILES"));
  }

  @Test
  void expand_primitives_rejects_null_compound_name() {
    QualifiedName price = AstDSL.qualifiedName("price");
    assertThrows(
        IllegalArgumentException.class,
        () -> CompoundAggregateExpander.expandPrimitives(null, price, null));
  }

  @Test
  void is_compound_aggregate_alias_true_only_for_alias_wrapping_compound_aggregate() {
    QualifiedName price = AstDSL.qualifiedName("price");
    Alias compound = new Alias("STATS(price)", new AggregateFunction("STATS", price));
    Alias nonCompound = new Alias("AVG(price)", new AggregateFunction("AVG", price));
    Alias nonAggregate = new Alias("price", price);

    assertTrue(CompoundAggregateExpander.isCompoundAggregateAlias(compound));
    assertFalse(CompoundAggregateExpander.isCompoundAggregateAlias(nonCompound));
    assertFalse(CompoundAggregateExpander.isCompoundAggregateAlias(nonAggregate));
    assertFalse(CompoundAggregateExpander.isCompoundAggregateAlias(price));
  }

  @Test
  void is_compound_aggregate_true_only_for_compound_aggregate_function() {
    QualifiedName price = AstDSL.qualifiedName("price");
    AggregateFunction compound = new AggregateFunction("EXTENDED_STATS", price);
    AggregateFunction nonCompound = new AggregateFunction("SUM", price);

    assertTrue(CompoundAggregateExpander.isCompoundAggregate(compound));
    assertFalse(CompoundAggregateExpander.isCompoundAggregate(nonCompound));
    assertFalse(CompoundAggregateExpander.isCompoundAggregate(price));
  }
}
