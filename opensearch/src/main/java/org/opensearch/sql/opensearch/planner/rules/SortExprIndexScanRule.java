/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner.rules;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import org.apache.calcite.adapter.enumerable.EnumerableLimitSort;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.immutables.value.Value;
import org.opensearch.sql.calcite.plan.rule.OpenSearchRuleConfig;
import org.opensearch.sql.calcite.utils.PlanUtils;
import org.opensearch.sql.opensearch.storage.scan.AbstractCalciteIndexScan;
import org.opensearch.sql.opensearch.storage.scan.CalciteLogicalIndexScan;
import org.opensearch.sql.opensearch.storage.scan.context.SortExprDigest;
import org.opensearch.sql.opensearch.util.OpenSearchRelOptUtil;

/**
 * Rule to match sort-project-scan RelNode pattern and push down sort expressions to OpenSearch.
 * This rule identifies sort operations with complex expressions and attempts to push them down to
 * the OpenSearch level for better performance.
 */
@Value.Enclosing
public class SortExprIndexScanRule extends InterruptibleRelRule<SortExprIndexScanRule.Config> {

  protected SortExprIndexScanRule(SortExprIndexScanRule.Config config) {
    super(config);
  }

  @Override
  protected void onMatchImpl(RelOptRuleCall call) {
    final Sort sort = call.rel(0);
    // EnumerableLimitSort carries fetch semantics; this rule doesn't preserve it on physical
    // scans because limit pushdown path is logical-only.
    if (sort instanceof EnumerableLimitSort) {
      return;
    }
    final Project project = call.rel(1);
    final AbstractCalciteIndexScan scan = call.rel(2);

    if (sort.getConvention() != project.getConvention()
        || project.getConvention() != scan.getConvention()) {
      return;
    }

    // Only match sort - project - scan when any sort key references an expression
    if (!PlanUtils.sortReferencesExpr(sort, project)) {
      return;
    }

    boolean allSimpleExprs = true;
    Map<Integer, Optional<Pair<Integer, Boolean>>> orderEquivInfoMap = new HashMap<>();

    for (RelFieldCollation relFieldCollation : sort.getCollation().getFieldCollations()) {
      Optional<Pair<Integer, Boolean>> orderEquivInfo =
          OpenSearchRelOptUtil.getOrderEquivalentInputInfo(
              project.getProjects().get(relFieldCollation.getFieldIndex()));
      orderEquivInfoMap.put(relFieldCollation.getFieldIndex(), orderEquivInfo);
      if (allSimpleExprs && orderEquivInfo.isEmpty()) {
        allSimpleExprs = false;
      }
    }

    if (allSimpleExprs) {
      return;
    }

    boolean scanProvidesRequiredCollation =
        OpenSearchRelOptUtil.canScanProvideSortCollation(
            scan, project, sort.collation, orderEquivInfoMap);
    if (scan.isTopKPushed() && !scanProvidesRequiredCollation) {
      return;
    }

    // Extract sort expressions with collation information from the sort node
    List<SortExprDigest> sortExprDigests =
        extractSortExpressionInfos(sort, project, scan, orderEquivInfoMap);

    // Check if any sort expressions can be pushed down
    if (sortExprDigests.isEmpty() || !canPushDownSortExpressionInfos(sortExprDigests)) {
      return;
    }

    AbstractCalciteIndexScan newScan;
    // If the scan's sort info already satisfies new sort, just pushdown limit if there is
    if (scan.isTopKPushed() && scanProvidesRequiredCollation) {
      newScan = scan.copy();
    } else {
      // Attempt to push down sort expressions
      newScan = scan.pushdownSortExpr(sortExprDigests);
    }

    // EnumerableSort won't have limit or offset
    Integer limitValue = LimitIndexScanRule.extractLimitValue(sort.fetch);
    Integer offsetValue = LimitIndexScanRule.extractOffsetValue(sort.offset);
    if (newScan instanceof CalciteLogicalIndexScan && limitValue != null && offsetValue != null) {
      newScan =
          (CalciteLogicalIndexScan)
              ((CalciteLogicalIndexScan) newScan)
                  .pushDownLimit((LogicalSort) sort, limitValue, offsetValue);
    }

    if (newScan != null) {
      Project newProject =
          project.copy(sort.getTraitSet(), newScan, project.getProjects(), project.getRowType());
      call.transformTo(newProject);
      PlanUtils.tryPruneRelNodes(call);
    }
  }

  /**
   * Extract sort expressions with collation information from the sort node, mapping them through
   * the project if necessary.
   *
   * @param sort The sort node
   * @param project The project node
   * @param scan The scan node to get stable field references
   * @param orderEquivInfoMap Order equivalence info to determine if output expression collation can
   *     be optimized to field collation
   * @return List of SortExprDigest with stable field references or complex expressions
   */
  private List<SortExprDigest> extractSortExpressionInfos(
      Sort sort,
      Project project,
      AbstractCalciteIndexScan scan,
      Map<Integer, Optional<Pair<Integer, Boolean>>> orderEquivInfoMap) {
    List<SortExprDigest> sortExprDigests = new ArrayList<>();

    List<RexNode> sortKeys = sort.getSortExps();
    List<RelFieldCollation> collations = sort.getCollation().getFieldCollations();

    for (int i = 0; i < sortKeys.size(); i++) {
      RexNode sortKey = sortKeys.get(i);
      RelFieldCollation collation = collations.get(i);

      SortExprDigest info = mapThroughProject(sortKey, project, scan, collation, orderEquivInfoMap);

      if (info != null) {
        sortExprDigests.add(info);
      }
    }

    return sortExprDigests;
  }

  /**
   * Map a sort key through the project to create a SortExprDigest. For simple field references,
   * stores the field name for stability. For complex expressions, stores the RexNode.
   *
   * @param sortKey The sort key (usually a RexInputRef)
   * @param project The project node
   * @param scan The scan node to get field names from
   * @param collation The collation information
   * @param orderEquivInfoMap Order equivalence info to determine if output expression collation can
   *     be optimized to field collation
   * @return SortExprDigest with stable field reference or complex expression
   */
  private SortExprDigest mapThroughProject(
      RexNode sortKey,
      Project project,
      AbstractCalciteIndexScan scan,
      RelFieldCollation collation,
      Map<Integer, Optional<Pair<Integer, Boolean>>> orderEquivInfoMap) {
    assert sortKey instanceof RexInputRef : "sort key should be always RexInputRef";

    RexInputRef inputRef = (RexInputRef) sortKey;
    RexNode projectExpression = project.getProjects().get(inputRef.getIndex());
    // Get the field name from the scan's row type
    List<String> scanFieldNames = scan.getRowType().getFieldNames();

    // If the project expression is a simple RexInputRef pointing to a scan field,
    // or it can be optimized to sort by field,
    // store the field name for stability
    Optional<Pair<Integer, Boolean>> orderEquivalentInfo =
        orderEquivInfoMap.get(collation.getFieldIndex());
    if (orderEquivalentInfo.isPresent()) {
      Direction equivalentDirection =
          orderEquivalentInfo.get().getRight()
              ? collation.getDirection().reverse()
              : collation.getDirection();
      // Create SortExprDigest with field name (stable reference)
      return new SortExprDigest(
          scanFieldNames.get(orderEquivalentInfo.get().getLeft()),
          equivalentDirection,
          collation.nullDirection);
    }

    // For complex expressions, store the RexNode
    return new SortExprDigest(projectExpression, collation.getDirection(), collation.nullDirection);
  }

  /**
   * Check if sort expressions can be pushed down to OpenSearch. Rejects literals and expressions
   * that only contain literals. Only supports number and string types for sort scripts.
   *
   * @param sortExprDigests List of sort expression infos to check
   * @return true if expressions can be pushed down, false otherwise
   */
  private boolean canPushDownSortExpressionInfos(List<SortExprDigest> sortExprDigests) {
    for (SortExprDigest info : sortExprDigests) {
      RexNode expr = info.getExpression();
      if (expr == null && StringUtils.isEmpty(info.getFieldName())) {
        return false;
      } else if (info.isSimpleFieldReference()) {
        continue;
      }
      // Reject literals or constant expression - they don't provide meaningful sorting
      if (expr instanceof RexLiteral
          || RexUtil.isConstant(expr)
          || !isSupportedSortScriptType(expr.getType().getSqlTypeName())) {
        return false;
      }
    }
    return true;
  }

  /**
   * Check if the SQL type is supported for OpenSearch sort scripts. Only number and string types
   * are supported for sort script.
   *
   * @param sqlTypeName The SQL type name to check
   * @return true if the type is supported for sort scripts, false otherwise
   */
  private boolean isSupportedSortScriptType(SqlTypeName sqlTypeName) {
    return SqlTypeName.CHAR_TYPES.contains(sqlTypeName)
        || SqlTypeName.APPROX_TYPES.contains(sqlTypeName)
        || SqlTypeName.INT_TYPES.contains(sqlTypeName);
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends OpenSearchRuleConfig {
    SortExprIndexScanRule.Config DEFAULT =
        ImmutableSortExprIndexScanRule.Config.builder()
            .build()
            .withOperandSupplier(
                b0 ->
                    b0.operand(Sort.class)
                        // Pure limit pushdown should be covered by SortProjectTransposeRule and
                        // OpenSearchLimitIndexScanRule
                        .predicate(sort -> !sort.collation.getFieldCollations().isEmpty())
                        .oneInput(
                            b1 ->
                                b1.operand(Project.class)
                                    .predicate(Predicate.not(Project::containsOver))
                                    .oneInput(
                                        b2 ->
                                            b2.operand(AbstractCalciteIndexScan.class)
                                                .predicate(
                                                    AbstractCalciteIndexScan::noAggregatePushed)
                                                .noInputs())));

    @Override
    default SortExprIndexScanRule toRule() {
      return new SortExprIndexScanRule(this);
    }
  }
}
