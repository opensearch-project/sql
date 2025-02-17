/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan;

import static java.util.Objects.requireNonNull;

import java.util.List;
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
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.sql.calcite.plan.OpenSearchTableScan;
import org.opensearch.sql.opensearch.planner.physical.OpenSearchIndexRules;
import org.opensearch.sql.opensearch.request.OpenSearchRequestBuilder;
import org.opensearch.sql.opensearch.request.PredicateAnalyzer;
import org.opensearch.sql.opensearch.request.PredicateAnalyzer.ExpressionNotAnalyzableException;
import org.opensearch.sql.opensearch.storage.OpenSearchIndex;

/** Relational expression representing a scan of an OpenSearchIndex type. */
public class CalciteOpenSearchIndexScan extends OpenSearchTableScan {
  private static final Logger LOG = LogManager.getLogger(CalciteOpenSearchIndexScan.class);

  private final OpenSearchIndex osIndex;
  private final OpenSearchRequestBuilder requestBuilder;
  private final RelDataType schema;

  /**
   * Creates an CalciteOpenSearchIndexScan.
   *
   * @param cluster Cluster
   * @param table Table
   * @param index OpenSearch index
   */
  public CalciteOpenSearchIndexScan(
      RelOptCluster cluster, RelOptTable table, OpenSearchIndex index) {
    this(cluster, table, index, index.createRequestBuilder(), table.getRowType());
  }

  public CalciteOpenSearchIndexScan(
      RelOptCluster cluster,
      RelOptTable table,
      OpenSearchIndex index,
      OpenSearchRequestBuilder requestBuilder,
      RelDataType schema) {
    super(cluster, table);
    this.osIndex = requireNonNull(index, "OpenSearch index");
    this.requestBuilder = requestBuilder;
    this.schema = schema;
  }

  public CalciteOpenSearchIndexScan copyWithNewSchema(RelDataType schema) {
    // TODO: need to do deep-copy on requestBuilder in case non-idempotent push down.
    return new CalciteOpenSearchIndexScan(getCluster(), table, osIndex, requestBuilder, schema);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    assert inputs.isEmpty();
    return new CalciteOpenSearchIndexScan(getCluster(), table, osIndex);
  }

  @Override
  public void register(RelOptPlanner planner) {
    super.register(planner);
    for (RelOptRule rule : OpenSearchIndexRules.OPEN_SEARCH_INDEX_SCAN_RULES) {
      planner.addRule(rule);
    }
  }

  @Override
  public RelDataType deriveRowType() {
    return this.schema;
  }

  @Override
  public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    // Avoid optimizing the java row type since the scan will always return an array.
    PhysType physType =
        PhysTypeImpl.of(implementor.getTypeFactory(), getRowType(), pref.preferArray(), false);

    Expression scanOperator = implementor.stash(this, CalciteOpenSearchIndexScan.class);
    return implementor.result(physType, Blocks.toBlock(Expressions.call(scanOperator, "scan")));
  }

  public Enumerable<@Nullable Object> scan() {
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

  public boolean pushDownFilter(Filter filter) {
    try {
      List<String> schema = this.getRowType().getFieldNames();
      QueryBuilder filterBuilder = PredicateAnalyzer.analyze(filter.getCondition(), schema);
      requestBuilder.pushDownFilter(filterBuilder);
      // TODO: handle the case where condition contains a score function
      return true;
    } catch (ExpressionNotAnalyzableException e) {
      LOG.warn("Cannot analyze the filter condition {}", filter.getCondition(), e);
    }
    return false;
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
    newScan.requestBuilder.pushDownProjectStream(newSchema.getFieldNames().stream());
    return newScan;
  }
}
