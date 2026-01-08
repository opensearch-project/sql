/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.plan.rule;

import static org.opensearch.sql.calcite.utils.PlanUtils.ROW_NUMBER_COLUMN_FOR_DEDUP;

import java.util.List;
import java.util.function.Predicate;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexWindow;
import org.apache.calcite.rex.RexWindowBounds;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Pair;
import org.immutables.value.Value;
import org.opensearch.sql.calcite.plan.rel.LogicalDedup;
import org.opensearch.sql.calcite.utils.PlanUtils;

/**
 * Planner rule that converts a logical dedup into equivalent composite of logical operators, e.g.
 *
 * <pre>
 * | dedup 2 a, b keepempty=true
 *
 * becomes:
 * LogicalProject(...)
 * +- LogicalFilter(condition=[OR(IS NULL(a), IS NULL(b), <=(_row_number_dedup_, 1))])
 *    +- LogicalProject(..., _row_number_dedup_=[ROW_NUMBER() OVER (PARTITION BY a, b ORDER BY a, b)])
 *
 * which is simplified to:
 *
 * LogicalDedup(dedupeFields=[a, b], allowedDuplication=2, keepempty=true)
 * </pre>
 */
@Value.Enclosing
public class PPLSimplifyDedupRule extends RelRule<PPLSimplifyDedupRule.Config> {
  /** Creates a PPLDedupConvertRule. */
  protected PPLSimplifyDedupRule(Config config) {
    super(config);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final LogicalProject finalProject = call.rel(0);
    final LogicalFilter numOfDedupFilter = call.rel(1);
    final LogicalProject projectWithWindow = call.rel(2);
    final LogicalFilter bucketNonNullFilter = call.rel(3);
    apply(call, finalProject, numOfDedupFilter, projectWithWindow, bucketNonNullFilter);
  }

  protected void apply(
      RelOptRuleCall call,
      LogicalProject finalProject,
      LogicalFilter numOfDedupFilter,
      LogicalProject projectWithWindow,
      LogicalFilter bucketNonNullFilter) {
    List<RexWindow> windows = PlanUtils.getRexWindowFromProject(projectWithWindow);
    if (windows.size() != 1) {
      return;
    }

    List<RexNode> dedupColumns = windows.get(0).partitionKeys;
    if (dedupColumns.stream()
        .filter(rex -> rex.isA(SqlKind.INPUT_REF))
        .anyMatch(
            rex ->
                rex.getType().getSqlTypeName() == SqlTypeName.MAP
                    || rex.getType().getSqlTypeName() == SqlTypeName.ARRAY)) {
      return;
    }

    // must be row_number <= number.
    // Since we cannot push down dedup with keepEmpty=true, we don't simplify that pattern
    assert numOfDedupFilter.getCondition().isA(SqlKind.LESS_THAN_OR_EQUAL);
    RexLiteral literal =
        (RexLiteral) ((RexCall) numOfDedupFilter.getCondition()).getOperands().getLast();
    Integer dedupNumber = literal.getValueAs(Integer.class);

    RelBuilder relBuilder = call.builder();
    relBuilder.push(bucketNonNullFilter.getInput());
    List<Pair<RexNode, String>> targetProjections =
        projectWithWindow.getNamedProjects().stream()
            .filter(p -> !p.getKey().isA(SqlKind.ROW_NUMBER))
            .toList();
    relBuilder.project(
        targetProjections.stream().map(Pair::getKey).toList(),
        targetProjections.stream().map(Pair::getValue).toList());

    LogicalDedup dedup =
        LogicalDedup.create(relBuilder.build(), dedupColumns, dedupNumber, false, false);
    relBuilder.push(dedup);
    relBuilder.project(finalProject.getProjects(), finalProject.getRowType().getFieldNames());

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
            relBuilder.or(dedupeFields.stream().map(relBuilder::isNull).toList()),
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
    relBuilder.filter(relBuilder.and(dedupeFields.stream().map(relBuilder::isNotNull).toList()));
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
    Config DEFAULT =
        ImmutablePPLSimplifyDedupRule.Config.builder()
            .build()
            .withOperandSupplier(
                b0 ->
                    b0.operand(LogicalProject.class)
                        .predicate(Predicate.not(PlanUtils::containsRowNumberDedup))
                        .oneInput(
                            b1 ->
                                b1.operand(LogicalFilter.class)
                                    .predicate(Config::validDedupNumberChecker)
                                    .oneInput(
                                        b2 ->
                                            b2.operand(LogicalProject.class)
                                                .predicate(PlanUtils::containsRowNumberDedup)
                                                .oneInput(
                                                    b3 ->
                                                        b3.operand(LogicalFilter.class)
                                                            .predicate(
                                                                PlanUtils
                                                                    ::mayBeFilterFromBucketNonNull)
                                                            .anyInputs()))));

    @Override
    default PPLSimplifyDedupRule toRule() {
      return new PPLSimplifyDedupRule(this);
    }

    private static boolean validDedupNumberChecker(LogicalFilter filter) {
      return filter.getCondition().isA(SqlKind.LESS_THAN_OR_EQUAL)
          && PlanUtils.containsRowNumberDedup(filter);
    }

    /**
     * Check if the condition is null or less than. Should be useful if we can push down Dedup with
     * keepEmpty=true in the future.
     */
    private static boolean isNullOrLessThan(RexNode node) {
      if (node.isA(SqlKind.LESS_THAN_OR_EQUAL)) return true;
      if (!node.isA(SqlKind.OR)) return false;
      boolean hasLessThan = false;
      for (RexNode operand : ((RexCall) node).getOperands()) {
        if (operand.isA(SqlKind.LESS_THAN_OR_EQUAL)) {
          if (hasLessThan) return false; // only one less than
          hasLessThan = true;
        } else if (!operand.isA(SqlKind.IS_NULL)) {
          return false; // only null if not less_than
        }
      }
      return hasLessThan; // should be one less than
    }
  }

  public static final PPLSimplifyDedupRule DEDUP_SIMPLIFY_RULE =
      PPLSimplifyDedupRule.Config.DEFAULT.toRule();
}
