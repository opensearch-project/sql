/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.Getter;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.data.type.OpenSearchTextType;
import org.opensearch.sql.opensearch.planner.physical.EnumerableIndexScanRule;
import org.opensearch.sql.opensearch.planner.physical.OpenSearchIndexRules;
import org.opensearch.sql.opensearch.request.AggregateAnalyzer;
import org.opensearch.sql.opensearch.request.PredicateAnalyzer;
import org.opensearch.sql.opensearch.request.PredicateAnalyzer.QueryExpression;
import org.opensearch.sql.opensearch.response.agg.OpenSearchAggregationResponseParser;
import org.opensearch.sql.opensearch.storage.OpenSearchIndex;

/** The logical relational operator representing a scan of an OpenSearchIndex type. */
@Getter
public class CalciteLogicalIndexScan extends AbstractCalciteIndexScan {
  private static final Logger LOG = LogManager.getLogger(CalciteLogicalIndexScan.class);

  public CalciteLogicalIndexScan(
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

  protected CalciteLogicalIndexScan(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      List<RelHint> hints,
      RelOptTable table,
      OpenSearchIndex osIndex,
      RelDataType schema,
      PushDownContext pushDownContext) {
    super(cluster, traitSet, hints, table, osIndex, schema, pushDownContext);
  }

  @Override
  protected AbstractCalciteIndexScan buildScan(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      List<RelHint> hints,
      RelOptTable table,
      OpenSearchIndex osIndex,
      RelDataType schema,
      PushDownContext pushDownContext) {
    return new CalciteLogicalIndexScan(
        cluster, traitSet, hints, table, osIndex, schema, pushDownContext);
  }

  public CalciteLogicalIndexScan copyWithNewSchema(RelDataType schema) {
    // Do shallow copy for requestBuilder, thus requestBuilder among different plans produced in the
    // optimization process won't affect each other.
    return new CalciteLogicalIndexScan(
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

  public AbstractRelNode pushDownFilter(Filter filter) {
    try {
      RelDataType rowType = filter.getRowType();
      CalciteLogicalIndexScan newScan = this.copyWithNewSchema(filter.getRowType());
      List<String> schema = this.getRowType().getFieldNames();
      Map<String, ExprType> fieldTypes =
          this.osIndex.getFieldTypes().entrySet().stream()
              .filter(entry -> schema.contains(entry.getKey()))
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
      QueryExpression queryExpression =
          PredicateAnalyzer.analyzeExpression(
              filter.getCondition(), schema, fieldTypes, rowType, getCluster());
      // TODO: handle the case where condition contains a score function
      newScan.pushDownContext.add(
          new PushDownAction(
              QueryExpression.containsScript(queryExpression)
                  ? PushDownType.SCRIPT
                  : PushDownType.FILTER,
              queryExpression.isPartial()
                  ? constructCondition(
                      queryExpression.getAnalyzedNodes(), getCluster().getRexBuilder())
                  : filter.getCondition(),
              requestBuilder -> requestBuilder.pushDownFilter(queryExpression.builder())));

      // If the query expression is partial, we need to replace the input of the filter with the
      // partial pushed scan and the filter condition with non-pushed-down conditions.
      if (queryExpression.isPartial()) {
        // Only CompoundQueryExpression could be partial.
        List<RexNode> conditions = queryExpression.getUnAnalyzableNodes();
        RexNode newCondition = constructCondition(conditions, getCluster().getRexBuilder());
        return filter.copy(filter.getTraitSet(), newScan, newCondition);
      }
      return newScan;
    } catch (Exception e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Cannot pushdown the filter condition.", e);
      }
    }
    return null;
  }

  private static RexNode constructCondition(List<RexNode> conditions, RexBuilder rexBuilder) {
    return conditions.size() > 1
        ? rexBuilder.makeCall(SqlStdOperatorTable.AND, conditions)
        : conditions.get(0);
  }

  public CalciteLogicalIndexScan pushDownCollapse(Project finalOutput, String fieldName) {
    ExprType fieldType = osIndex.getFieldTypes().get(fieldName);
    if (fieldType == null) {
      // the fieldName must be one of index fields
      if (LOG.isDebugEnabled()) {
        LOG.debug("Cannot pushdown the dedup '{}' due to it is not a index field", fieldName);
      }
      return null;
    }
    ExprType originalExprType = fieldType.getOriginalExprType();
    String originalFieldName = originalExprType.getOriginalPath().orElse(fieldName);
    if (!ExprCoreType.numberTypes().contains(originalExprType)
        && !originalExprType.legacyTypeName().equals("KEYWORD")
        && !originalExprType.legacyTypeName().equals("TEXT")) {
      if (LOG.isDebugEnabled()) {
        LOG.debug(
            "Cannot pushdown the dedup '{}' due to only keyword and number type are accepted, but"
                + " its type is {}",
            originalFieldName,
            originalExprType.legacyTypeName());
      }
      return null;
    }
    // For text, use its subfield if exists.
    String field = OpenSearchTextType.toKeywordSubField(originalFieldName, fieldType);
    if (field == null) {
      LOG.debug("Cannot pushdown the dedup due to no keyword subfield for {}.", fieldName);
      return null;
    }
    CalciteLogicalIndexScan newScan = this.copyWithNewSchema(finalOutput.getRowType());
    newScan.pushDownContext.add(
        new PushDownAction(
            PushDownType.COLLAPSE,
            fieldName,
            requestBuilder -> requestBuilder.pushDownCollapse(field)));
    return newScan;
  }

  /**
   * When pushing down a project, we need to create a new CalciteLogicalIndexScan with the updated
   * schema since we cannot override getRowType() which is defined to be final.
   */
  public CalciteLogicalIndexScan pushDownProject(List<Integer> selectedColumns) {
    final RelDataTypeFactory.Builder builder = getCluster().getTypeFactory().builder();
    final List<RelDataTypeField> fieldList = this.getRowType().getFieldList();
    for (int project : selectedColumns) {
      builder.add(fieldList.get(project));
    }
    RelDataType newSchema = builder.build();

    // Projection may alter indicies in the collations.
    // E.g. When sorting age
    // `Project(age) - TableScan(schema=[name, age], collation=[$1 ASC])` should become
    // `TableScan(schema=[age], collation=[$0 ASC])` after pushing down project.
    RelTraitSet traitSetWithReIndexedCollations = reIndexCollations(selectedColumns);

    CalciteLogicalIndexScan newScan =
        new CalciteLogicalIndexScan(
            getCluster(),
            traitSetWithReIndexedCollations,
            hints,
            table,
            osIndex,
            newSchema,
            pushDownContext.clone());

    Map<String, String> aliasMapping = this.osIndex.getAliasMapping();
    // For alias types, we need to push down its original path instead of the alias name.
    List<String> projectedFields =
        newSchema.getFieldNames().stream()
            .map(fieldName -> aliasMapping.getOrDefault(fieldName, fieldName))
            .collect(Collectors.toList());
    newScan.pushDownContext.add(
        new PushDownAction(
            PushDownType.PROJECT,
            newSchema.getFieldNames(),
            requestBuilder -> requestBuilder.pushDownProjectStream(projectedFields.stream())));
    return newScan;
  }

  private RelTraitSet reIndexCollations(List<Integer> selectedColumns) {
    RelTraitSet newTraitSet;
    RelCollation relCollation = getTraitSet().getCollation();
    if (!Objects.isNull(relCollation) && !relCollation.getFieldCollations().isEmpty()) {
      List<RelFieldCollation> newCollations =
          relCollation.getFieldCollations().stream()
              .filter(collation -> selectedColumns.contains(collation.getFieldIndex()))
              .map(
                  collation ->
                      collation.withFieldIndex(selectedColumns.indexOf(collation.getFieldIndex())))
              .collect(Collectors.toList());
      newTraitSet = getTraitSet().plus(RelCollations.of(newCollations));
    } else {
      newTraitSet = getTraitSet();
    }
    return newTraitSet;
  }

  public CalciteLogicalIndexScan pushDownAggregate(Aggregate aggregate, Project project) {
    try {
      CalciteLogicalIndexScan newScan =
          new CalciteLogicalIndexScan(
              getCluster(),
              traitSet,
              hints,
              table,
              osIndex,
              aggregate.getRowType(),
              // Aggregation will eliminate all collations.
              cloneWithoutSort(pushDownContext));
      Map<String, ExprType> fieldTypes = this.osIndex.getFieldTypes();
      List<String> outputFields = aggregate.getRowType().getFieldNames();
      final Pair<List<AggregationBuilder>, OpenSearchAggregationResponseParser> aggregationBuilder =
          AggregateAnalyzer.analyze(
              aggregate, project, getRowType(), fieldTypes, outputFields, getCluster());
      Map<String, OpenSearchDataType> extendedTypeMapping =
          aggregate.getRowType().getFieldList().stream()
              .collect(
                  Collectors.toMap(
                      RelDataTypeField::getName,
                      field ->
                          OpenSearchDataType.of(
                              OpenSearchTypeFactory.convertRelDataTypeToExprType(
                                  field.getType()))));
      AggPushDownAction action = new AggPushDownAction(aggregationBuilder, extendedTypeMapping);
      newScan.pushDownContext.add(new PushDownAction(PushDownType.AGGREGATION, aggregate, action));
      return newScan;
    } catch (Exception e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Cannot pushdown the aggregate {}", aggregate, e);
      }
    }
    return null;
  }

  public AbstractRelNode pushDownLimit(LogicalSort sort, Integer limit, Integer offset) {
    try {
      if (pushDownContext.isAggregatePushed()) {
        // Push down the limit into the aggregation bucket
        boolean pushed =
            pushDownContext.getAggPushDownAction().pushDownLimitIntoBucketSize(limit + offset);
        if (!pushed && offset > 0) return null;
        CalciteLogicalIndexScan newScan = this.copyWithNewSchema(getRowType());
        newScan.pushDownContext.add(
            PushDownAction.of(
                PushDownType.LIMIT, new LimitDigest(limit, offset), requestBuilder -> {}));
        return offset > 0 ? sort.copy(sort.getTraitSet(), List.of(newScan)) : newScan;
      } else {
        CalciteLogicalIndexScan newScan = this.copyWithNewSchema(getRowType());
        newScan.pushDownContext.add(
            new PushDownAction(
                PushDownType.LIMIT,
                new LimitDigest(limit, offset),
                requestBuilder -> requestBuilder.pushDownLimit(limit, offset)));
        return newScan;
      }
    } catch (Exception e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Cannot pushdown limit {} with offset {}", limit, offset, e);
      }
    }
    return null;
  }
}
