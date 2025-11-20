/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan;

import static org.opensearch.sql.opensearch.storage.serde.ScriptParameterHelper.MISSING_MAX;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
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
import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.opensearch.search.sort.ScriptSortBuilder.ScriptSortType;
import org.opensearch.search.sort.SortBuilder;
import org.opensearch.search.sort.SortBuilders;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.sql.ast.expression.Argument;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.data.type.OpenSearchTextType;
import org.opensearch.sql.opensearch.planner.rules.EnumerableIndexScanRule;
import org.opensearch.sql.opensearch.planner.rules.OpenSearchIndexRules;
import org.opensearch.sql.opensearch.request.AggregateAnalyzer;
import org.opensearch.sql.opensearch.request.OpenSearchRequestBuilder.PushDownUnSupportedException;
import org.opensearch.sql.opensearch.request.PredicateAnalyzer;
import org.opensearch.sql.opensearch.request.PredicateAnalyzer.QueryExpression;
import org.opensearch.sql.opensearch.response.agg.OpenSearchAggregationResponseParser;
import org.opensearch.sql.opensearch.storage.OpenSearchIndex;
import org.opensearch.sql.opensearch.storage.scan.context.AbstractAction;
import org.opensearch.sql.opensearch.storage.scan.context.AggPushDownAction;
import org.opensearch.sql.opensearch.storage.scan.context.AggregationBuilderAction;
import org.opensearch.sql.opensearch.storage.scan.context.FilterDigest;
import org.opensearch.sql.opensearch.storage.scan.context.LimitDigest;
import org.opensearch.sql.opensearch.storage.scan.context.OSRequestBuilderAction;
import org.opensearch.sql.opensearch.storage.scan.context.PushDownContext;
import org.opensearch.sql.opensearch.storage.scan.context.PushDownType;
import org.opensearch.sql.opensearch.storage.scan.context.RareTopDigest;
import org.opensearch.sql.opensearch.storage.scan.context.SortExprDigest;

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
        new PushDownContext(osIndex));
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

  @Override
  public CalciteLogicalIndexScan copy() {
    return new CalciteLogicalIndexScan(
        getCluster(), traitSet, hints, table, osIndex, schema, pushDownContext.clone());
  }

  public CalciteLogicalIndexScan copyWithNewSchema(RelDataType schema) {
    // Do shallow copy for requestBuilder, thus requestBuilder among different plans produced in the
    // optimization process won't affect each other.
    return new CalciteLogicalIndexScan(
        getCluster(), traitSet, hints, table, osIndex, schema, pushDownContext.clone());
  }

  public CalciteLogicalIndexScan copyWithNewTraitSet(RelTraitSet traitSet) {
    return new CalciteLogicalIndexScan(
        getCluster(), traitSet, hints, table, osIndex, schema, pushDownContext.clone());
  }

  @Override
  public void register(RelOptPlanner planner) {
    super.register(planner);
    planner.addRule(EnumerableIndexScanRule.DEFAULT_CONFIG.toRule());
    if ((Boolean) osIndex.getSettings().getSettingValue(Settings.Key.CALCITE_PUSHDOWN_ENABLED)) {
      // When pushdown is enabled, use normal rules (they handle everything including relevance
      // functions)
      for (RelOptRule rule : OpenSearchIndexRules.OPEN_SEARCH_INDEX_SCAN_RULES) {
        planner.addRule(rule);
      }
    } else {
      planner.addRule(OpenSearchIndexRules.RELEVANCE_FUNCTION_PUSHDOWN);
    }
  }

  public AbstractRelNode pushDownFilter(Filter filter) {
    try {
      RelDataType rowType = this.getRowType();
      List<String> schema = this.getRowType().getFieldNames();
      Map<String, ExprType> fieldTypes =
          this.osIndex.getAllFieldTypes().entrySet().stream()
              .filter(entry -> schema.contains(entry.getKey()))
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
      QueryExpression queryExpression =
          PredicateAnalyzer.analyzeExpression(
              filter.getCondition(), schema, fieldTypes, rowType, getCluster());
      // TODO: handle the case where condition contains a score function
      CalciteLogicalIndexScan newScan = this.copy();
      newScan.pushDownContext.add(
          queryExpression.getScriptCount() > 0 ? PushDownType.SCRIPT : PushDownType.FILTER,
          new FilterDigest(
              queryExpression.getScriptCount(),
              queryExpression.isPartial()
                  ? constructCondition(
                      queryExpression.getAnalyzedNodes(), getCluster().getRexBuilder())
                  : filter.getCondition()),
          (OSRequestBuilderAction)
              requestBuilder -> requestBuilder.pushDownFilterForCalcite(queryExpression.builder()));

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
        PushDownType.COLLAPSE,
        fieldName,
        (OSRequestBuilderAction) requestBuilder -> requestBuilder.pushDownCollapse(field));
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

    // To prevent circular pushdown for some edge cases where plan has pattern(see TPCH Q1):
    // `Project($1, $0) -> Sort(sort0=[1]) -> Project($1, $0) -> Aggregate(group={0},...)...`
    // We don't support pushing down the duplicated project here otherwise it will cause dead-loop.
    // `ProjectMergeRule` will help merge the duplicated projects.
    if (this.getPushDownContext().containsDigest(newSchema.getFieldNames())) return null;

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

    AbstractAction<?> action;
    if (pushDownContext.isAggregatePushed()) {
      // For aggregate, we do nothing on query builder but only change the schema of the scan.
      action = (AggregationBuilderAction) aggAction -> {};
    } else {
      Map<String, String> aliasMapping = this.osIndex.getAliasMapping();
      // For alias types, we need to push down its original path instead of the alias name.
      List<String> projectedFields =
          newSchema.getFieldNames().stream()
              .map(fieldName -> aliasMapping.getOrDefault(fieldName, fieldName))
              .toList();
      action =
          (OSRequestBuilderAction)
              requestBuilder -> requestBuilder.pushDownProjectStream(projectedFields.stream());
    }
    newScan.pushDownContext.add(PushDownType.PROJECT, newSchema.getFieldNames(), action);
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

  public CalciteLogicalIndexScan pushDownSortAggregateMeasure(Sort sort) {
    try {
      if (!pushDownContext.isAggregatePushed()) return null;
      List<AggregationBuilder> aggregationBuilders =
          pushDownContext.getAggPushDownAction().getBuilderAndParser().getLeft();
      if (aggregationBuilders.size() != 1) {
        return null;
      }
      if (!(aggregationBuilders.getFirst() instanceof CompositeAggregationBuilder)) {
        return null;
      }
      List<String> collationNames = getCollationNames(sort.getCollation().getFieldCollations());
      if (!isAnyCollationNameInAggregators(collationNames)) {
        return null;
      }
      CalciteLogicalIndexScan newScan = copyWithNewTraitSet(sort.getTraitSet());
      AbstractAction<?> newAction =
          (AggregationBuilderAction)
              aggAction ->
                  aggAction.rePushDownSortAggMeasure(
                      sort.getCollation().getFieldCollations(), rowType.getFieldNames());
      Object digest = sort.getCollation().getFieldCollations();
      newScan.pushDownContext.add(PushDownType.SORT_AGG_METRICS, digest, newAction);
      return newScan;
    } catch (Exception e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Cannot pushdown the sort aggregate {}", sort, e);
      }
    }
    return null;
  }

  public CalciteLogicalIndexScan pushDownRareTop(Project project, RareTopDigest digest) {
    try {
      CalciteLogicalIndexScan newScan = copyWithNewSchema(project.getRowType());
      AbstractAction<?> newAction =
          (AggregationBuilderAction) aggAction -> aggAction.rePushDownRareTop(digest);
      newScan.pushDownContext.add(PushDownType.RARE_TOP, digest, newAction);
      return newScan;
    } catch (Exception e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Cannot pushdown {}", digest, e);
      }
      return null;
    }
  }

  public AbstractRelNode pushDownAggregate(Aggregate aggregate, Project project) {
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
              pushDownContext.cloneWithoutSort());
      List<String> schema = this.getRowType().getFieldNames();
      Map<String, ExprType> fieldTypes =
          this.osIndex.getAllFieldTypes().entrySet().stream()
              .filter(entry -> schema.contains(entry.getKey()))
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
      List<String> outputFields = aggregate.getRowType().getFieldNames();
      int bucketSize = osIndex.getBucketSize();
      boolean bucketNullable =
          Boolean.parseBoolean(
              aggregate.getHints().stream()
                  .filter(hits -> hits.hintName.equals("stats_args"))
                  .map(hint -> hint.kvOptions.getOrDefault(Argument.BUCKET_NULLABLE, "true"))
                  .findFirst()
                  .orElseGet(() -> "true"));
      AggregateAnalyzer.AggregateBuilderHelper helper =
          new AggregateAnalyzer.AggregateBuilderHelper(
              getRowType(), fieldTypes, getCluster(), bucketNullable, bucketSize);
      final Pair<List<AggregationBuilder>, OpenSearchAggregationResponseParser> builderAndParser =
          AggregateAnalyzer.analyze(aggregate, project, outputFields, helper);
      Map<String, OpenSearchDataType> extendedTypeMapping =
          aggregate.getRowType().getFieldList().stream()
              .collect(
                  Collectors.toMap(
                      RelDataTypeField::getName,
                      field ->
                          OpenSearchDataType.of(
                              OpenSearchTypeFactory.convertRelDataTypeToExprType(
                                  field.getType()))));
      AggPushDownAction action =
          new AggPushDownAction(
              builderAndParser,
              extendedTypeMapping,
              outputFields.subList(0, aggregate.getGroupSet().cardinality()));
      newScan.pushDownContext.add(PushDownType.AGGREGATION, aggregate, action);
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
        // Push down the limit into the aggregation bucket in advance to detect whether the limit
        // can update the aggregation builder
        boolean updated =
            pushDownContext.getAggPushDownAction().pushDownLimitIntoBucketSize(limit + offset);
        if (!updated && offset > 0) return null;
        CalciteLogicalIndexScan newScan = this.copyWithNewSchema(getRowType());
        // Simplify the action if it doesn't update the aggregation builder, otherwise keep the
        // original action
        // It won't change the aggregation builder by do this action again since it's idempotent
        AggregationBuilderAction action =
            updated
                ? aggAction -> aggAction.pushDownLimitIntoBucketSize(limit + offset)
                : aggAction -> {};
        newScan.pushDownContext.add(PushDownType.LIMIT, new LimitDigest(limit, offset), action);
        return offset > 0 ? sort.copy(sort.getTraitSet(), List.of(newScan)) : newScan;
      } else {
        CalciteLogicalIndexScan newScan = this.copyWithNewSchema(getRowType());
        newScan.pushDownContext.add(
            PushDownType.LIMIT,
            new LimitDigest(limit, offset),
            (OSRequestBuilderAction) requestBuilder -> requestBuilder.pushDownLimit(limit, offset));
        return newScan;
      }
    } catch (Exception e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Cannot pushdown limit {} with offset {}", limit, offset, e);
      }
    }
    return null;
  }

  /**
   * Push down sort expressions to OpenSearch level. Supports mixed RexCall and field sort
   * expressions.
   *
   * @param sortExprDigests List of SortExprDigest with expressions and collation information
   * @return CalciteLogicalIndexScan with sort expressions pushed down, or null if pushdown fails
   */
  public CalciteLogicalIndexScan pushdownSortExpr(List<SortExprDigest> sortExprDigests) {
    try {
      if (sortExprDigests == null || sortExprDigests.isEmpty()) {
        return null;
      }

      CalciteLogicalIndexScan newScan =
          new CalciteLogicalIndexScan(
              getCluster(),
              traitSet,
              hints,
              table,
              osIndex,
              getRowType(),
              pushDownContext.cloneWithoutSort());

      List<Supplier<SortBuilder<?>>> sortBuilderSuppliers = new ArrayList<>();
      for (SortExprDigest digest : sortExprDigests) {
        SortOrder order =
            Direction.DESCENDING.equals(digest.getDirection()) ? SortOrder.DESC : SortOrder.ASC;

        if (digest.isSimpleFieldReference()) {
          String missing =
              switch (digest.getNullDirection()) {
                case FIRST -> "_first";
                case LAST -> "_last";
                default -> null;
              };
          sortBuilderSuppliers.add(
              () -> SortBuilders.fieldSort(digest.getFieldName()).order(order).missing(missing));
          continue;
        }
        RexNode sortExpr = digest.getExpression();
        assert sortExpr instanceof RexCall : "sort expression should be RexCall";
        Map<String, Object> missingValueParams =
            new LinkedHashMap<>() {
              {
                put(MISSING_MAX, digest.isMissingMax());
              }
            };
        // Complex expression - use ScriptQueryExpression to generate script for sort
        PredicateAnalyzer.ScriptQueryExpression scriptExpr =
            new PredicateAnalyzer.ScriptQueryExpression(
                digest.getExpression(),
                rowType,
                osIndex.getAllFieldTypes(),
                getCluster(),
                missingValueParams);
        // Determine the correct ScriptSortType based on the expression's return type
        ScriptSortType sortType = getScriptSortType(sortExpr.getType());

        sortBuilderSuppliers.add(
            () -> SortBuilders.scriptSort(scriptExpr.getScript(), sortType).order(order));
      }

      // Create action to push down sort expressions to OpenSearch
      OSRequestBuilderAction action =
          requestBuilder -> requestBuilder.pushDownSortSuppliers(sortBuilderSuppliers);

      newScan.pushDownContext.add(PushDownType.SORT_EXPR, sortExprDigests, action);
      return newScan;
    } catch (Exception e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Cannot pushdown sort expressions: {}", sortExprDigests, e);
      }
    }
    return null;
  }

  /**
   * Determine the appropriate ScriptSortType based on the expression's return type.
   *
   * @param relDataType the return type of the expression
   * @return the appropriate ScriptSortType
   */
  private ScriptSortType getScriptSortType(RelDataType relDataType) {
    if (SqlTypeName.CHAR_TYPES.contains(relDataType.getSqlTypeName())) {
      return ScriptSortType.STRING;
    } else if (SqlTypeName.INT_TYPES.contains(relDataType.getSqlTypeName())
        || SqlTypeName.APPROX_TYPES.contains(relDataType.getSqlTypeName())) {
      return ScriptSortType.NUMBER;
    } else {
      throw new PushDownUnSupportedException(
          "Unsupported type for sort expression pushdown: " + relDataType);
    }
  }
}
