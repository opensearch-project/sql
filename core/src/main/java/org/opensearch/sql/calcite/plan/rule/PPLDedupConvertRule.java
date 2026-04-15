/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.plan.rule;

import static org.opensearch.sql.calcite.utils.PlanUtils.ROW_NUMBER_COLUMN_FOR_DEDUP;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexWindowBounds;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.immutables.value.Value;
import org.opensearch.sql.calcite.plan.rel.LogicalDedup;

/**
 * Planner rule that converts a logical dedup into equivalent composite of logical operators, e.g.
 *
 * <pre>
 * | dedup 2 a, b keepempty=true
 *
 * becomes:
 *
 * LogicalDedup(dedupeFields=[a, b], allowedDuplication=2, keepempty=true)
 *
 * which is then converted to:
 *
 * LogicalProject(...)
 * +- LogicalFilter(condition=[OR(IS NULL(a), IS NULL(b), <=(_row_number_dedup_, 2))])
 *    +- LogicalProject(..., _row_number_dedup_=[ROW_NUMBER() OVER (PARTITION BY a, b ORDER BY a, b)])
 * </pre>
 */
@Value.Enclosing
public class PPLDedupConvertRule extends RelRule<PPLDedupConvertRule.Config> {
  /** Creates a PPLDedupConvertRule. */
  protected PPLDedupConvertRule(Config config) {
    super(config);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final LogicalDedup dedup = call.rel(0);
    RelBuilder relBuilder = call.builder();
    relBuilder.push(dedup.getInput());
    if (dedup.getKeepEmpty()) {
      buildDedupOrNull(relBuilder, dedup.getDedupeFields(), dedup.getAllowedDuplication());
    } else {
      buildDedupNotNull(relBuilder, dedup.getDedupeFields(), dedup.getAllowedDuplication());
    }
    call.transformTo(relBuilder.build());
  }

  public static void buildDedupOrNull(
      RelBuilder relBuilder, List<RexNode> dedupeFields, Integer allowedDuplication) {
    /*
     * | dedup 2 a, b keepempty=true
     * LogicalSort(...)  -- re-sort to restore input order
     * +- LogicalProject(...)
     *    +- LogicalFilter(condition=[OR(IS NULL(a), IS NULL(b), <=(_row_number_dedup_, 1))])
     *       +- LogicalProject(..., _row_number_dedup_=[ROW_NUMBER() OVER (PARTITION BY a, b)])
     *           +- ... (input with Sort stripped)
     */
    RelCollation inputCollation = stripInputSort(relBuilder);
    List<RexNode> orderKeys = collationToOrderKeys(relBuilder, inputCollation);
    RexNode rowNumber =
        relBuilder
            .aggregateCall(SqlStdOperatorTable.ROW_NUMBER)
            .over()
            .partitionBy(dedupeFields)
            .orderBy(orderKeys)
            .rowsTo(RexWindowBounds.CURRENT_ROW)
            .as(ROW_NUMBER_COLUMN_FOR_DEDUP);
    relBuilder.projectPlus(rowNumber);
    RexNode _row_number_dedup_ = relBuilder.field(ROW_NUMBER_COLUMN_FOR_DEDUP);
    // Filter (isnull('a) OR isnull('b) OR '_row_number_dedup_ <= n)
    relBuilder.filter(
        relBuilder.or(
            relBuilder.or(
                dedupeFields.stream().map(relBuilder::isNull).collect(Collectors.toList())),
            relBuilder.lessThanOrEqual(
                _row_number_dedup_, relBuilder.literal(allowedDuplication))));
    // DropColumns('_row_number_dedup_)
    relBuilder.projectExcept(_row_number_dedup_);
    // Re-sort to restore the input order that was stripped before the window
    restoreInputOrder(relBuilder, inputCollation);
  }

  public static void buildDedupNotNull(
      RelBuilder relBuilder, List<RexNode> dedupeFields, Integer allowedDuplication) {
    /*
     * | dedup 2 a, b keepempty=false
     * LogicalSort(...)  -- re-sort to restore input order
     * +- LogicalProject(...)
     *    +- LogicalFilter(condition=[<=(_row_number_dedup_, n)]))
     *       +- LogicalProject(..., _row_number_dedup_=[ROW_NUMBER() OVER (PARTITION BY a, b)])
     *           +- LogicalFilter(condition=[AND(IS NOT NULL(a), IS NOT NULL(b))])
     *              +- ... (input with Sort stripped)
     */
    // Strip input sort before building the window to prevent Calcite from incorrectly
    // propagating collation metadata through the window and eliminating the post-dedup sort
    RelCollation inputCollation = stripInputSort(relBuilder);
    List<RexNode> orderKeys = collationToOrderKeys(relBuilder, inputCollation);
    // Filter (isnotnull('a) AND isnotnull('b))
    String rowNumberAlias = ROW_NUMBER_COLUMN_FOR_DEDUP;
    relBuilder.filter(
        relBuilder.and(
            dedupeFields.stream().map(relBuilder::isNotNull).collect(Collectors.toList())));
    RexNode rowNumber =
        relBuilder
            .aggregateCall(SqlStdOperatorTable.ROW_NUMBER)
            .over()
            .partitionBy(dedupeFields)
            .orderBy(orderKeys)
            .rowsTo(RexWindowBounds.CURRENT_ROW)
            .as(rowNumberAlias);
    relBuilder.projectPlus(rowNumber);
    RexNode rowNumberField = relBuilder.field(rowNumberAlias);
    // Filter ('_row_number_dedup_ <= n)
    relBuilder.filter(
        relBuilder.lessThanOrEqual(rowNumberField, relBuilder.literal(allowedDuplication)));
    // DropColumns('_row_number_dedup_)
    relBuilder.projectExcept(rowNumberField);
    // Re-sort to restore the input order that was stripped before the window
    restoreInputOrder(relBuilder, inputCollation);
  }

  /**
   * Strip the Sort node from the input on the RelBuilder stack, returning its collation. This is
   * necessary because EnumerableWindow re-partitions data by PARTITION BY key, which can destroy
   * input sort order. Calcite's metadata system (RelMdCollation) incorrectly propagates the input's
   * collation through the Window, causing the optimizer to eliminate a post-dedup Sort as
   * "redundant". By stripping the Sort before the window and re-adding it after, we break this
   * incorrect metadata chain.
   *
   * @return the collation of the stripped Sort, or null if no Sort was found
   */
  private static RelCollation stripInputSort(RelBuilder relBuilder) {
    RelNode input = relBuilder.peek();
    RelCollation collation = findCollation(input);
    if (collation != null && !collation.getFieldCollations().isEmpty()) {
      RelNode stripped = removeSortFromTree(input);
      if (stripped != input) {
        relBuilder.clear();
        relBuilder.push(stripped);
      }
    }
    return collation;
  }

  /**
   * Remove the first Sort node found in the tree, replacing it with its input. Only traverses
   * through single-input operators (Filter, Project) that preserve order.
   */
  private static RelNode removeSortFromTree(RelNode node) {
    if (node instanceof org.apache.calcite.rel.core.Sort) {
      org.apache.calcite.rel.core.Sort sort = (org.apache.calcite.rel.core.Sort) node;
      if (sort.getCollation() != null
          && !sort.getCollation().getFieldCollations().isEmpty()
          && sort.fetch == null
          && sort.offset == null) {
        return sort.getInput();
      }
    }
    if (node.getInputs().size() == 1) {
      RelNode child = node.getInput(0);
      RelNode newChild = removeSortFromTree(child);
      if (newChild != child) {
        return node.copy(node.getTraitSet(), List.of(newChild));
      }
    }
    return node;
  }

  /**
   * Convert a RelCollation to a list of RexNode order keys using the RelBuilder's field references.
   */
  private static List<RexNode> collationToOrderKeys(RelBuilder relBuilder, RelCollation collation) {
    if (collation == null || collation.getFieldCollations().isEmpty()) {
      return List.of();
    }
    List<RexNode> orderKeys = new ArrayList<>();
    for (RelFieldCollation fieldCollation : collation.getFieldCollations()) {
      RexNode fieldRef = relBuilder.field(fieldCollation.getFieldIndex());
      if (fieldCollation.direction.isDescending()) {
        fieldRef = relBuilder.desc(fieldRef);
      }
      if (fieldCollation.nullDirection == RelFieldCollation.NullDirection.LAST) {
        fieldRef = relBuilder.nullsLast(fieldRef);
      } else if (fieldCollation.nullDirection == RelFieldCollation.NullDirection.FIRST) {
        fieldRef = relBuilder.nullsFirst(fieldRef);
      }
      orderKeys.add(fieldRef);
    }
    return orderKeys;
  }

  /**
   * Re-apply a sort after dedup to restore the input order that may have been disrupted by the
   * window operator. EnumerableWindow can re-partition data by the PARTITION BY key, destroying any
   * upstream sort order. This explicit re-sort ensures the final output preserves the original
   * order.
   */
  private static void restoreInputOrder(RelBuilder relBuilder, RelCollation inputCollation) {
    if (inputCollation != null && !inputCollation.getFieldCollations().isEmpty()) {
      List<RexNode> sortKeys = collationToOrderKeys(relBuilder, inputCollation);
      relBuilder.sort(sortKeys);
    }
  }

  /**
   * Walk down the plan tree to find the first Sort node with collation. Traverses through Filter
   * and Project nodes which preserve input ordering.
   */
  private static RelCollation findCollation(RelNode node) {
    while (node != null) {
      if (node instanceof org.apache.calcite.rel.core.Sort) {
        org.apache.calcite.rel.core.Sort sort = (org.apache.calcite.rel.core.Sort) node;
        if (sort.getCollation() != null && !sort.getCollation().getFieldCollations().isEmpty()) {
          return sort.getCollation();
        }
      }
      if (node.getInputs().isEmpty()) {
        break;
      }
      // Only traverse through single-input operators that preserve order
      if (node.getInputs().size() == 1) {
        node = node.getInput(0);
      } else {
        break;
      }
    }
    return null;
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends OpenSearchRuleConfig {
    Config DEDUP_CONVERTER =
        ImmutablePPLDedupConvertRule.Config.builder()
            .build()
            .withOperandSupplier(b0 -> b0.operand(LogicalDedup.class).anyInputs());

    @Override
    default PPLDedupConvertRule toRule() {
      return new PPLDedupConvertRule(this);
    }
  }

  public static final PPLDedupConvertRule DEDUP_CONVERT_RULE =
      PPLDedupConvertRule.Config.DEDUP_CONVERTER.toRule();
}
