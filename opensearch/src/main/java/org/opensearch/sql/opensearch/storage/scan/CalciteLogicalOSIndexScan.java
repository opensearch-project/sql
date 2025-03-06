/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Getter;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.planner.physical.EnumerableIndexScanRule;
import org.opensearch.sql.opensearch.planner.physical.OpenSearchIndexRules;
import org.opensearch.sql.opensearch.request.AggregateAnalyzer;
import org.opensearch.sql.opensearch.request.PredicateAnalyzer;
import org.opensearch.sql.opensearch.response.agg.OpenSearchAggregationResponseParser;
import org.opensearch.sql.opensearch.storage.OpenSearchIndex;

@Getter
public class CalciteLogicalOSIndexScan extends CalciteOSIndexScan {
  private static final Logger LOG = LogManager.getLogger(CalciteLogicalOSIndexScan.class);

  public CalciteLogicalOSIndexScan(
      RelOptCluster cluster, RelOptTable table, OpenSearchIndex osIndex) {
    this(
        cluster,
        cluster.traitSetOf(Convention.NONE),
        ImmutableList.of(),
        table,
        osIndex,
        table.getRowType(),
        new PushDownContext());
  }

  protected CalciteLogicalOSIndexScan(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      List<RelHint> hints,
      RelOptTable table,
      OpenSearchIndex osIndex,
      RelDataType schema,
      PushDownContext pushDownContext) {
    super(cluster, traitSet, hints, table, osIndex, schema, pushDownContext);
  }

  public CalciteLogicalOSIndexScan copyWithNewSchema(RelDataType schema) {
    // Do shallow copy for requestBuilder, thus requestBuilder among different plans produced in the
    // optimization process won't affect each other.
    return new CalciteLogicalOSIndexScan(
        getCluster(), traitSet, hints, table, osIndex, schema, pushDownContext.clone());
  }

  @Override
  public void register(RelOptPlanner planner) {
    super.register(planner);
    planner.addRule(EnumerableIndexScanRule.DEFAULT_CONFIG.toRule());
    if (osIndex.getSettings().getSettingValue(Settings.Key.CALCITE_PUSHDOWN_ENABLED)) {
      for (RelOptRule rule : OpenSearchIndexRules.OPEN_SEARCH_INDEX_SCAN_RULES) {
        planner.addRule(rule);
      }
    }
  }

  public CalciteLogicalOSIndexScan pushDownFilter(Filter filter) {
    try {
      CalciteLogicalOSIndexScan newScan = this.copyWithNewSchema(filter.getRowType());
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
   * When pushing down a project, we need to create a new CalciteLogicalOSIndexScan with the updated
   * schema since we cannot override getRowType() which is defined to be final.
   */
  public CalciteLogicalOSIndexScan pushDownProject(List<Integer> selectedColumns) {
    final RelDataTypeFactory.Builder builder = getCluster().getTypeFactory().builder();
    final List<RelDataTypeField> fieldList = this.getRowType().getFieldList();
    for (int project : selectedColumns) {
      builder.add(fieldList.get(project));
    }
    RelDataType newSchema = builder.build();
    CalciteLogicalOSIndexScan newScan = this.copyWithNewSchema(newSchema);
    newScan.pushDownContext.add(
        PushDownAction.of(
            PushDownType.PROJECT,
            newSchema.getFieldNames(),
            requestBuilder ->
                requestBuilder.pushDownProjectStream(newSchema.getFieldNames().stream())));
    return newScan;
  }

  public CalciteLogicalOSIndexScan pushDownAggregate(Aggregate aggregate) {
    try {
      CalciteLogicalOSIndexScan newScan = this.copyWithNewSchema(aggregate.getRowType());
      List<String> schema = this.getRowType().getFieldNames();
      Map<String, OpenSearchDataType> typeMapping = this.osIndex.getFieldOpenSearchTypes();
      List<String> outputFields = aggregate.getRowType().getFieldNames();
      final Pair<List<AggregationBuilder>, OpenSearchAggregationResponseParser> aggregationBuilder =
          AggregateAnalyzer.analyze(aggregate, schema, typeMapping, outputFields);
      Map<String, OpenSearchDataType> extendedTypeMapping =
          aggregate.getRowType().getFieldList().stream()
              .collect(
                  Collectors.toMap(
                      RelDataTypeField::getName,
                      field ->
                          OpenSearchDataType.of(
                              OpenSearchTypeFactory.convertRelDataTypeToExprType(
                                  field.getType()))));
      newScan.pushDownContext.add(
          PushDownAction.of(
              PushDownType.AGGREGATION,
              aggregate,
              requestBuilder -> {
                requestBuilder.pushDownAggregation(aggregationBuilder);
                requestBuilder.pushTypeMapping(extendedTypeMapping);
              }));
      return newScan;
    } catch (Exception e) {
      LOG.debug("Cannot analyze the aggregate {}", aggregate, e);
    }
    return null;
  }
}
