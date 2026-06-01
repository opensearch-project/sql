/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql.parser;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;
import org.opensearch.sql.ast.expression.AggregateFunction;
import org.opensearch.sql.ast.expression.Alias;
import org.opensearch.sql.ast.expression.Function;
import org.opensearch.sql.ast.expression.UnresolvedExpression;

/**
 * Parse-time expansion utility for fixed-shape compound aggregate calls. A compound aggregate
 * registered in {@link #COMPOUND_AGGREGATES} expands into a fixed list of primitive aggregates,
 * emitted as bare expressions ({@link #expandPrimitives}) or {@link Alias}-wrapped columns ({@link
 * #expandAliased}).
 *
 * <p>To register a new compound aggregate, add an entry to {@link #COMPOUND_AGGREGATES}.
 *
 * <p>Aggregates whose output column count depends on call arguments do not fit this registry.
 */
public final class CompoundAggregateExpander {

  /**
   * Per-output-column descriptor for a compound aggregate's expansion.
   *
   * @param func primitive aggregate function name to emit
   * @param suffix column-name suffix used in the generated alias
   * @param operandTransform applied to the operand before passing to the primitive; defaults to
   *     identity. {@code sum_of_squares} uses {@link #SQUARE}.
   */
  record Component(
      String func, String suffix, UnaryOperator<UnresolvedExpression> operandTransform) {
    Component(String func, String suffix) {
      this(func, suffix, UnaryOperator.identity());
    }
  }

  private static final UnaryOperator<UnresolvedExpression> SQUARE =
      field -> new Function("*", List.of(field, field));

  private static final List<Component> STATS_COMPONENTS =
      List.of(
          new Component("count", "count"),
          new Component("sum", "sum"),
          new Component("avg", "avg"),
          new Component("min", "min"),
          new Component("max", "max"));

  private static final List<Component> EXTENDED_STATS_COMPONENTS =
      Stream.concat(
              STATS_COMPONENTS.stream(),
              Stream.of(
                  new Component("sum", "sumOfSquares", SQUARE),
                  new Component("var_pop", "variance"),
                  new Component("stddev_pop", "stdDeviation")))
          .toList();

  private static final Map<String, List<Component>> COMPOUND_AGGREGATES =
      Map.ofEntries(
          Map.entry("STATS", STATS_COMPONENTS),
          Map.entry("EXTENDED_STATS", EXTENDED_STATS_COMPONENTS));

  private CompoundAggregateExpander() {}

  /** Returns true if {@code functionName} (case-insensitive) is a registered compound aggregate. */
  public static boolean isCompoundName(String functionName) {
    return functionName != null
        && COMPOUND_AGGREGATES.containsKey(functionName.toUpperCase(Locale.ROOT));
  }

  /**
   * Returns true if {@code item} is an {@link Alias} wrapping an {@link AggregateFunction} with a
   * compound function name — the shape SELECT-list parsing produces.
   */
  public static boolean isCompoundAggregateAlias(UnresolvedExpression item) {
    return item instanceof Alias alias
        && alias.getDelegated() instanceof AggregateFunction agg
        && isCompoundName(agg.getFuncName());
  }

  /**
   * Returns true if {@code expr} is an un-aliased {@link AggregateFunction} with a compound
   * function name — the shape the aggregator-collector visitor sees.
   */
  public static boolean isCompoundAggregate(UnresolvedExpression expr) {
    return expr instanceof AggregateFunction agg && isCompoundName(agg.getFuncName());
  }

  /**
   * Expands a compound name into primitive {@link AggregateFunction} expressions in registered
   * order. Each primitive is built over {@code field} pre-transformed by its component's {@link
   * Component#operandTransform()}. {@code condition}, if non-null, is propagated to every
   * primitive.
   *
   * <p>For named columns, use {@link #expandAliased}.
   */
  public static List<UnresolvedExpression> expandPrimitives(
      String compoundName, UnresolvedExpression field, UnresolvedExpression condition) {
    List<Component> components = resolveComponents(compoundName);
    List<UnresolvedExpression> primitives = new ArrayList<>(components.size());
    for (Component comp : components) {
      UnresolvedExpression operand = comp.operandTransform().apply(field);
      AggregateFunction primitive = new AggregateFunction(comp.func, operand);
      if (condition != null) {
        primitive.condition(condition);
      }
      primitives.add(primitive);
    }
    return primitives;
  }

  /**
   * Expands a compound call into primitives wrapped in {@link Alias}es. Each output Alias has:
   *
   * <ul>
   *   <li>{@code name} — internal lookup key {@code <suffix>(<fieldText>)} (e.g. {@code
   *       count(price)}). Used by aggregator dedup against explicit primitive aggregates the user
   *       might also have written.
   *   <li>{@code alias} — user-visible display name in V1 format {@code <displayPrefix>.<suffix>}
   *       (e.g. {@code STATS(price).count}, {@code p.sumOfSquares}). Set only when {@code
   *       displayPrefix} is non-null.
   * </ul>
   *
   * <p>Pass {@code displayPrefix == null} to produce internal-name-only Aliases for the
   * aggregator-collection visitor, which has no display concerns.
   *
   * @param compoundName compound aggregate name (case-insensitive); must be registered
   * @param field operand expression
   * @param fieldText source-text of the operand, used in the internal name
   * @param displayPrefix the part before the dot in V1's display format — typically the user's
   *     {@code AS} alias if given, otherwise the source text of the entire compound call (e.g.
   *     {@code STATS(price)}). Pass {@code null} to skip the display alias entirely.
   * @param condition propagated to every primitive, or {@code null} for no filter
   */
  public static List<UnresolvedExpression> expandAliased(
      String compoundName,
      UnresolvedExpression field,
      String fieldText,
      String displayPrefix,
      UnresolvedExpression condition) {
    List<Component> components = resolveComponents(compoundName);
    List<UnresolvedExpression> primitives = expandPrimitives(compoundName, field, condition);
    List<UnresolvedExpression> aliasedColumns = new ArrayList<>(primitives.size());
    for (int i = 0; i < primitives.size(); i++) {
      Component comp = components.get(i);
      String internalName = comp.suffix() + "(" + fieldText + ")";
      String displayName = displayPrefix != null ? displayPrefix + "." + comp.suffix() : null;
      aliasedColumns.add(new Alias(internalName, primitives.get(i), displayName));
    }
    return aliasedColumns;
  }

  private static List<Component> resolveComponents(String compoundName) {
    if (compoundName == null) {
      throw new IllegalArgumentException("compoundName is null");
    }
    List<Component> components = COMPOUND_AGGREGATES.get(compoundName.toUpperCase(Locale.ROOT));
    if (components == null) {
      throw new IllegalArgumentException("Not a compound aggregate: " + compoundName);
    }
    return components;
  }
}
