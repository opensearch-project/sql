/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.plan.rule;

import static org.opensearch.sql.calcite.utils.PlanUtils.ROW_NUMBER_COLUMN_FOR_DEDUP;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
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
     * LogicalProject(...)
     * +- LogicalFilter(condition=[OR(IS NULL(a), IS NULL(b), <=(_row_number_dedup_, 1))])
     *    +- LogicalProject(..., _row_number_dedup_=[ROW_NUMBER() OVER (PARTITION BY a, b ORDER BY a, b)])
     *        +- ...
     */
    RexNode rowNumber =
        relBuilder
            .aggregateCall(SqlStdOperatorTable.ROW_NUMBER)
            .over()
            .partitionBy(dedupeFields)
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
  }

  public static void buildDedupNotNull(
      RelBuilder relBuilder, List<RexNode> dedupeFields, Integer allowedDuplication) {
    /*
     * | dedup 2 a, b keepempty=false
     * LogicalProject(...)
     * +- LogicalFilter(condition=[<=(_row_number_dedup_, n)]))
     *    +- LogicalProject(..., _row_number_dedup_=[ROW_NUMBER() OVER (PARTITION BY a, b ORDER BY a, b)])
     *        +- LogicalFilter(condition=[AND(IS NOT NULL(a), IS NOT NULL(b))])
     *           +- ...
     */
    // Filter (isnotnull('a) AND isnotnull('b))
    String rowNumberAlias = ROW_NUMBER_COLUMN_FOR_DEDUP;
    relBuilder.filter(
        relBuilder.and(
            dedupeFields.stream().map(relBuilder::isNotNull).collect(Collectors.toList())));
    // Window [row_number() windowspecdefinition('a, 'b, 'a ASC NULLS FIRST, 'b ASC NULLS FIRST,
    // specifiedwindowoundedpreceding$(), currentrow$())) AS _row_number_dedup_], ['a, 'b], ['a ASC
    // NULLS FIRST, 'b ASC NULLS FIRST]
    RexNode rowNumber =
        relBuilder
            .aggregateCall(SqlStdOperatorTable.ROW_NUMBER)
            .over()
            .partitionBy(dedupeFields)
            .rowsTo(RexWindowBounds.CURRENT_ROW)
            .as(rowNumberAlias);
    relBuilder.projectPlus(rowNumber);
    RexNode rowNumberField = relBuilder.field(rowNumberAlias);
    // Filter ('_row_number_dedup_ <= n)
    relBuilder.filter(
        relBuilder.lessThanOrEqual(rowNumberField, relBuilder.literal(allowedDuplication)));
    // DropColumns('_row_number_dedup_)
    relBuilder.projectExcept(rowNumberField);
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
