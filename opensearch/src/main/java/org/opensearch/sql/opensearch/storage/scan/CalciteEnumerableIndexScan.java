/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan;

import java.util.List;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.tree.Blocks;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.opensearch.sql.calcite.plan.OpenSearchRules;
import org.opensearch.sql.opensearch.request.OpenSearchRequestBuilder;
import org.opensearch.sql.opensearch.storage.OpenSearchIndex;

/** The physical relational operator representing a scan of an OpenSearchIndex type. */
public class CalciteEnumerableIndexScan extends AbstractCalciteIndexScan implements EnumerableRel {
  private static final Logger LOG = LogManager.getLogger(CalciteEnumerableIndexScan.class);

  /**
   * Creates an CalciteOpenSearchIndexScan.
   *
   * @param cluster Cluster
   * @param table Table
   * @param osIndex OpenSearch index
   */
  public CalciteEnumerableIndexScan(
      RelOptCluster cluster,
      List<RelHint> hints,
      RelOptTable table,
      OpenSearchIndex osIndex,
      RelDataType schema,
      PushDownContext pushDownContext) {
    super(
        cluster,
        cluster.traitSetOf(EnumerableConvention.INSTANCE),
        hints,
        table,
        osIndex,
        schema,
        pushDownContext);
  }

  @Override
  public void register(RelOptPlanner planner) {
    for (RelOptRule rule : OpenSearchRules.OPEN_SEARCH_OPT_RULES) {
      planner.addRule(rule);
    }

    // remove this rule otherwise opensearch can't correctly interpret approx_count_distinct()
    // it is converted to cardinality aggregation in OpenSearch
    planner.removeRule(CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES);
  }

  @Override
  public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    /* In Calcite enumerable operators, row of single column will be optimized to a scalar value.
     * See {@link PhysTypeImpl}.
     * Since we need to combine this operator with their original ones,
     * let's follow this convention to apply the optimization here and ensure `scan` method
     * returns the correct data format for single column rows.
     * See {@link OpenSearchIndexEnumerator}
     */
    PhysType physType =
        PhysTypeImpl.of(implementor.getTypeFactory(), getRowType(), pref.preferArray());

    Expression scanOperator = implementor.stash(this, CalciteEnumerableIndexScan.class);
    return implementor.result(physType, Blocks.toBlock(Expressions.call(scanOperator, "scan")));
  }

  public Enumerable<@Nullable Object> scan() {
    OpenSearchRequestBuilder requestBuilder = osIndex.createRequestBuilder();
    pushDownContext.forEach(action -> action.apply(requestBuilder));
    return new AbstractEnumerable<>() {
      @Override
      public Enumerator<Object> enumerator() {
        return new OpenSearchIndexEnumerator(
            osIndex.getClient(),
            getFieldPath(),
            requestBuilder.getMaxResponseSize(),
            osIndex.buildRequest(requestBuilder));
      }
    };
  }

  private List<String> getFieldPath() {
    return getRowType().getFieldNames().stream()
        .map(f -> osIndex.getAliasMapping().getOrDefault(f, f))
        .toList();
  }
}
