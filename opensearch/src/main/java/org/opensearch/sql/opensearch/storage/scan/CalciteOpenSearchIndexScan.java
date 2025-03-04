/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan;

import static java.util.Objects.requireNonNull;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;
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
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.sql.calcite.plan.OpenSearchTableScan;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.planner.physical.OpenSearchIndexRules;
import org.opensearch.sql.opensearch.request.OpenSearchRequestBuilder;
import org.opensearch.sql.opensearch.request.PredicateAnalyzer;
import org.opensearch.sql.opensearch.storage.OpenSearchIndex;

/** Relational expression representing a scan of an OpenSearchIndex type. */
public class CalciteOpenSearchIndexScan extends OpenSearchTableScan {
  private static final Logger LOG = LogManager.getLogger(CalciteOpenSearchIndexScan.class);

  private final OpenSearchIndex osIndex;
  // The schema of this scan operator, it's initialized with the row type of the table, but may be
  // changed by push down operations.
  private final RelDataType schema;
  // This context maintains all the push down actions, which will be applied to the requestBuilder
  // when it begins to scan data from OpenSearch.
  // Because OpenSearchRequestBuilder doesn't support deep copy while we want to keep the
  // requestBuilder independent among different plans produced in the optimization process,
  // so we cannot apply these actions right away.
  private final PushDownContext pushDownContext;

  /**
   * Creates an CalciteOpenSearchIndexScan.
   *
   * @param cluster Cluster
   * @param table Table
   * @param index OpenSearch index
   */
  public CalciteOpenSearchIndexScan(
      RelOptCluster cluster, RelOptTable table, OpenSearchIndex index) {
    this(cluster, table, index, table.getRowType(), new PushDownContext());
  }

  private CalciteOpenSearchIndexScan(
      RelOptCluster cluster,
      RelOptTable table,
      OpenSearchIndex index,
      RelDataType schema,
      PushDownContext pushDownContext) {
    super(cluster, table);
    this.osIndex = requireNonNull(index, "OpenSearch index");
    this.schema = schema;
    this.pushDownContext = pushDownContext;
  }

  public CalciteOpenSearchIndexScan copy() {
    return new CalciteOpenSearchIndexScan(
        getCluster(), table, osIndex, this.schema, pushDownContext.clone());
  }

  public CalciteOpenSearchIndexScan copyWithNewSchema(RelDataType schema) {
    // Do shallow copy for requestBuilder, thus requestBuilder among different plans produced in the
    // optimization process won't affect each other.
    return new CalciteOpenSearchIndexScan(
        getCluster(), table, osIndex, schema, pushDownContext.clone());
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    assert inputs.isEmpty();
    return new CalciteOpenSearchIndexScan(getCluster(), table, osIndex);
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw)
        .itemIf("PushDownContext", pushDownContext, !pushDownContext.isEmpty());
  }

  @Override
  public void register(RelOptPlanner planner) {
    super.register(planner);
    if (osIndex.getSettings().getSettingValue(Settings.Key.CALCITE_PUSHDOWN_ENABLED)) {
      for (RelOptRule rule : OpenSearchIndexRules.OPEN_SEARCH_INDEX_SCAN_RULES) {
        planner.addRule(rule);
      }
    }
  }

  @Override
  public RelDataType deriveRowType() {
    return this.schema;
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

    Expression scanOperator = implementor.stash(this, CalciteOpenSearchIndexScan.class);
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
            List.copyOf(getRowType().getFieldNames()),
            requestBuilder.getMaxResponseSize(),
            osIndex.buildRequest(requestBuilder));
      }
    };
  }

  public CalciteOpenSearchIndexScan pushDownFilter(Filter filter) {
    try {
      CalciteOpenSearchIndexScan newScan = this.copyWithNewSchema(filter.getRowType());
      List<String> schema = this.getRowType().getFieldNames();
      Map<String, OpenSearchDataType> typeMapping = this.osIndex.getFieldOpenSearchTypes();
      QueryBuilder filterBuilder =
          PredicateAnalyzer.analyze(filter.getCondition(), schema, typeMapping);
      newScan.pushDownContext.add(
          PushDownAction.of(
              PushDownType.FILTER,
              filter.getCondition(),
              requestBuilder -> requestBuilder.pushDownFilter(filterBuilder)));

      // TODO: handle the case where condition contains a score function
      return newScan;
    } catch (Exception e) {
      LOG.warn("Cannot analyze the filter condition {}", filter.getCondition(), e);
    }
    return null;
  }

  /**
   * When pushing down a project, we need to create a new CalciteOpenSearchIndexScan with the
   * updated schema since we cannot override getRowType() which is defined to be final.
   */
  public CalciteOpenSearchIndexScan pushDownProject(List<Integer> selectedColumns) {
    final RelDataTypeFactory.Builder builder = getCluster().getTypeFactory().builder();
    final List<RelDataTypeField> fieldList = this.getRowType().getFieldList();
    for (int project : selectedColumns) {
      builder.add(fieldList.get(project));
    }
    RelDataType newSchema = builder.build();
    CalciteOpenSearchIndexScan newScan = this.copyWithNewSchema(newSchema);
    newScan.pushDownContext.add(
        PushDownAction.of(
            PushDownType.PROJECT,
            newSchema.getFieldNames(),
            requestBuilder ->
                requestBuilder.pushDownProjectStream(newSchema.getFieldNames().stream())));
    return newScan;
  }

  // TODO: should we consider equivalent among PushDownContexts with different push down sequence?
  static class PushDownContext extends ArrayDeque<PushDownAction> {
    @Override
    public PushDownContext clone() {
      return (PushDownContext) super.clone();
    }
  }

  private enum PushDownType {
    FILTER,
    PROJECT,
    // AGGREGATION,
    // SORT,
    // LIMIT,
    // HIGHLIGHT,
    // NESTED
  }

  private static class PushDownAction {
    private final PushDownType type;
    private final Object digest;
    private final AbstractAction action;

    public PushDownAction(PushDownType type, Object digest, AbstractAction action) {
      this.type = type;
      this.digest = digest;
      this.action = action;
    }
    static PushDownAction of(PushDownType type, Object digest, AbstractAction action) {
      return new PushDownAction(type, digest, action);
    }

    public String toString() {
      return type + ":" + digest;
    }

    void apply(OpenSearchRequestBuilder requestBuilder) {
      action.apply(requestBuilder);
    }
  }

  private interface AbstractAction {
    void apply(OpenSearchRequestBuilder requestBuilder);
  }
}
