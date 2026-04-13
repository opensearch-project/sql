/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.Getter;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.Strong;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
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
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.sql.ast.tree.HighlightConfig;
import org.opensearch.sql.calcite.plan.HighlightPushDown;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.calcite.utils.PPLHintUtils;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.HighlightExpression;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.data.type.OpenSearchTextType;
import org.opensearch.sql.opensearch.planner.rules.OpenSearchIndexRules;
import org.opensearch.sql.opensearch.request.AggregateAnalyzer;
import org.opensearch.sql.opensearch.request.PredicateAnalyzer;
import org.opensearch.sql.opensearch.request.PredicateAnalyzer.QueryExpression;
import org.opensearch.sql.opensearch.response.agg.OpenSearchAggregationResponseParser;
import org.opensearch.sql.opensearch.storage.OpenSearchIndex;
import org.opensearch.sql.opensearch.storage.scan.context.AbstractAction;
import org.opensearch.sql.opensearch.storage.scan.context.AggSpec;
import org.opensearch.sql.opensearch.storage.scan.context.FilterDigest;
import org.opensearch.sql.opensearch.storage.scan.context.LimitDigest;
import org.opensearch.sql.opensearch.storage.scan.context.OSRequestBuilderAction;
import org.opensearch.sql.opensearch.storage.scan.context.ProjectDigest;
import org.opensearch.sql.opensearch.storage.scan.context.PushDownContext;
import org.opensearch.sql.opensearch.storage.scan.context.PushDownOperation;
import org.opensearch.sql.opensearch.storage.scan.context.PushDownType;
import org.opensearch.sql.opensearch.storage.scan.context.RareTopDigest;
import org.opensearch.sql.utils.Utils;

/** The logical relational operator representing a scan of an OpenSearchIndex type. */
@Getter
public class CalciteLogicalIndexScan extends AbstractCalciteIndexScan implements HighlightPushDown {
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

  public RelNode pushDownHighlight(HighlightConfig highlightConfig) {
    RelDataTypeFactory.Builder schemaBuilder = getCluster().getTypeFactory().builder();
    schemaBuilder.addAll(getRowType().getFieldList());
    schemaBuilder.add(
        HighlightExpression.HIGHLIGHT_FIELD,
        getCluster().getTypeFactory().createSqlType(SqlTypeName.ANY));
    CalciteLogicalIndexScan newScan = copyWithNewSchema(schemaBuilder.build());
    newScan
        .getPushDownContext()
        .add(
            PushDownType.HIGHLIGHT,
            highlightConfig.fieldNames(),
            (OSRequestBuilderAction)
                requestBuilder -> applyHighlightPushDown(requestBuilder, highlightConfig));
    return newScan;
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
    for (RelOptRule rule : OpenSearchIndexRules.OPEN_SEARCH_NON_PUSHDOWN_RULES) {
      planner.addRule(rule);
    }
    if ((Boolean) osIndex.getSettings().getSettingValue(Settings.Key.CALCITE_PUSHDOWN_ENABLED)) {
      for (RelOptRule rule : OpenSearchIndexRules.OPEN_SEARCH_PUSHDOWN_RULES) {
        planner.addRule(rule);
      }
    }
  }

  public AbstractRelNode pushDownFilter(Filter filter) {
    try {
      RelDataType rowType = this.getRowType();
      List<String> schema = buildSchema();
      Map<String, ExprType> fieldTypes = this.osIndex.getAllFieldTypes();
      QueryExpression queryExpression =
          PredicateAnalyzer.analyzeExpression(
              filter.getCondition(), schema, fieldTypes, rowType, getCluster());
      // TODO: handle the case where condition contains a score function
      CalciteLogicalIndexScan newScan = this.copy();
      RexNode digestCondition =
          queryExpression.isPartial()
              ? constructCondition(queryExpression.getAnalyzedNodes(), getCluster().getRexBuilder())
              : filter.getCondition();
      // Infer NOT NULL constraints from the actually-pushed condition (digestCondition),
      // not the original filter condition — in partial pushdown, the original may contain
      // predicates that stay in Calcite and don't affect the OpenSearch-side aggregation.
      Set<Integer> notNullIndices = extractNotNullFieldIndices(digestCondition);
      newScan.pushDownContext.add(
          queryExpression.getScriptCount() > 0 ? PushDownType.SCRIPT : PushDownType.FILTER,
          new FilterDigest(queryExpression.getScriptCount(), digestCondition, notNullIndices),
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

  /**
   * Build schema for the current scan. Schema is the combination of all index fields and nested
   * fields in index fields.
   *
   * @return All current outputs fields plus nested paths.
   */
  private List<String> buildSchema() {
    List<String> schema = new ArrayList<>(this.getRowType().getFieldNames());
    // Add nested paths to schema if it has
    List<String> nestedPaths =
        schema.stream()
            .map(field -> Utils.resolveNestedPath(field, this.osIndex.getAllFieldTypes()))
            .filter(Objects::nonNull)
            .distinct()
            .toList();
    schema.addAll(nestedPaths);
    return schema;
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
        PushDownType.AGGREGATION,
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
      action = (OSRequestBuilderAction) requestBuilder -> {};
    } else {
      action =
          (OSRequestBuilderAction)
              requestBuilder ->
                  requestBuilder.pushDownProjectStream(
                      newSchema.getFieldNames().stream()
                          .filter(f -> !HighlightExpression.HIGHLIGHT_FIELD.equals(f)));
    }
    newScan.pushDownContext.add(
        PushDownType.PROJECT,
        new ProjectDigest(newSchema.getFieldNames(), selectedColumns),
        action);
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
      AggSpec aggSpec = pushDownContext.getAggSpec();
      if (aggSpec == null || !aggSpec.isCompositeAggregation()) {
        return null;
      }
      List<String> collationNames = getCollationNames(sort.getCollation().getFieldCollations());
      if (!isAnyCollationNameInAggregators(collationNames)) {
        return null;
      }
      CalciteLogicalIndexScan newScan = copyWithNewTraitSet(sort.getTraitSet());
      newScan.pushDownContext.setAggSpec(
          aggSpec.withSortMeasure(
              sort.getCollation().getFieldCollations(), rowType.getFieldNames()));
      AbstractAction<?> action =
          (OSRequestBuilderAction) requestAction -> requestAction.resetRequestTotal();
      Object digest = sort.getCollation().getFieldCollations();
      newScan.pushDownContext.add(PushDownType.SORT_AGG_METRICS, digest, action);
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
      newScan.pushDownContext.setAggSpec(pushDownContext.getAggSpec().withRareTop(digest));
      AbstractAction<?> action =
          (OSRequestBuilderAction) requestAction -> requestAction.resetRequestTotal();
      newScan.pushDownContext.add(PushDownType.RARE_TOP, digest, action);
      return newScan;
    } catch (Exception e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Cannot pushdown {}", digest, e);
      }
      return null;
    }
  }

  public AbstractRelNode pushDownAggregate(Aggregate aggregate, @Nullable Project project) {
    try {
      CalciteLogicalIndexScan newScan =
          new CalciteLogicalIndexScan(
              getCluster(),
              traitSet,
              hints,
              table,
              osIndex,
              aggregate.getRowType(),
              pushDownContext.cloneForAggregate(aggregate, project));
      List<String> schema = buildSchema();
      Map<String, ExprType> fieldTypes =
          this.osIndex.getAllFieldTypes().entrySet().stream()
              .filter(entry -> schema.contains(entry.getKey()))
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
      List<String> outputFields = aggregate.getRowType().getFieldNames();
      List<String> bucketNames = outputFields.subList(0, aggregate.getGroupSet().cardinality());
      if (bucketNames.stream()
          .map(b -> fieldTypes.get(b))
          .filter(Objects::nonNull)
          .anyMatch(expr -> expr.getOriginalType() == ExprCoreType.ARRAY)) {
        // TODO https://github.com/opensearch-project/sql/issues/5006
        if (LOG.isDebugEnabled()) {
          LOG.debug("Cannot pushdown the aggregate due to bucket contains array (nested) type");
        }
        return null;
      }
      int queryBucketSize = osIndex.getQueryBucketSize();
      boolean bucketNullable = !PPLHintUtils.ignoreNullBucket(aggregate);
      Set<Integer> nonNullableGroupIndices =
          extractNonNullableGroupIndices(newScan.pushDownContext, aggregate, project);
      AggregateAnalyzer.AggregateBuilderHelper helper =
          new AggregateAnalyzer.AggregateBuilderHelper(
              getRowType(),
              fieldTypes,
              getCluster(),
              bucketNullable,
              queryBucketSize,
              nonNullableGroupIndices);
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
      AggSpec aggSpec = AggSpec.create(extendedTypeMapping, bucketNames, builderAndParser);
      // AggPushDownAction is lazily materialized by AggSpec.buildAction() and then this action
      // will materialize agg request builder.
      // The AGGREGATION pushdown operation in PushDownContext remains a no-op marker here.
      newScan.pushDownContext.setAggSpec(aggSpec);
      newScan.pushDownContext.add(
          PushDownType.AGGREGATION, aggregate, (OSRequestBuilderAction) requestBuilder -> {});
      return newScan;
    } catch (Exception e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Cannot pushdown the aggregate {}", aggregate, e);
      }
    }
    return null;
  }

  /**
   * Extract group-by field indices that are constrained by IS_NOT_NULL in the pushed filter. This
   * allows setting missingBucket=false per-field for those group-by fields, preventing null buckets
   * in aggregation results when the user explicitly filters with isnotnull().
   */
  private static Set<Integer> extractNonNullableGroupIndices(
      PushDownContext context, Aggregate aggregate, @Nullable Project project) {
    Set<Integer> allNotNullIndices = new HashSet<>();
    for (PushDownOperation op : context) {
      // FilterDigest is attached to both FILTER and SCRIPT push-down operations;
      // the type only reflects whether a script is involved, not whether NOT NULL metadata exists.
      if ((op.type() == PushDownType.FILTER || op.type() == PushDownType.SCRIPT)
          && op.digest() instanceof FilterDigest fd) {
        allNotNullIndices.addAll(fd.notNullFieldIndices());
      }
    }
    if (allNotNullIndices.isEmpty()) {
      return Collections.emptySet();
    }
    // Map scan-level not-null indices to aggregate group indices, resolving through project
    List<Integer> groupList = aggregate.getGroupSet().asList();
    Set<Integer> result = new HashSet<>();
    for (int groupIndex : groupList) {
      int scanIndex = resolveGroupToScanIndex(groupIndex, project);
      if (scanIndex >= 0 && allNotNullIndices.contains(scanIndex)) {
        result.add(groupIndex);
      }
    }
    return result;
  }

  /**
   * Extract scan-level field indices that are implicitly constrained to be NOT NULL by the filter
   * condition. Uses Calcite's {@link Strong#isNotTrue} to detect fields where the condition would
   * not be true if the field were null — this covers explicit IS_NOT_NULL as well as implicit
   * constraints like {@code x <> ''} (which is null if x is null).
   */
  private static Set<Integer> extractNotNullFieldIndices(RexNode condition) {
    Set<Integer> allRefs = new HashSet<>();
    condition.accept(
        new RexVisitorImpl<Void>(true) {
          @Override
          public Void visitInputRef(RexInputRef inputRef) {
            allRefs.add(inputRef.getIndex());
            return null;
          }
        });
    Set<Integer> result = new HashSet<>();
    for (int idx : allRefs) {
      if (Strong.isNotTrue(condition, ImmutableBitSet.of(idx))) {
        result.add(idx);
      }
    }
    return result;
  }

  /** Resolve a group-by index to a scan-level field index through an optional project. */
  private static int resolveGroupToScanIndex(int groupIndex, @Nullable Project project) {
    if (project == null) {
      return groupIndex;
    }
    RexNode projected = project.getProjects().get(groupIndex);
    if (projected instanceof RexInputRef scanRef) {
      return scanRef.getIndex();
    }
    return -1;
  }

  public AbstractRelNode pushDownLimit(LogicalSort sort, Integer limit, Integer offset) {
    try {
      if (pushDownContext.isAggregatePushed()) {
        int totalSize = limit + offset;
        AggSpec aggSpec = pushDownContext.getAggSpec();
        boolean canReduceEstimatedRowsCount =
            !pushDownContext.isLimitPushed()
                || pushDownContext.getQueue().reversed().stream()
                    .takeWhile(op -> op.type() != PushDownType.AGGREGATION)
                    .filter(op -> op.type() == PushDownType.LIMIT)
                    .findFirst()
                    .map(op -> (LimitDigest) op.digest())
                    .map(d -> totalSize < d.offset() + d.limit())
                    .orElse(true);
        boolean canUpdate =
            canReduceEstimatedRowsCount || aggSpec.canPushDownLimitIntoBucketSize(totalSize);
        if (!canUpdate && offset > 0) return null;
        CalciteLogicalIndexScan newScan = this.copyWithNewSchema(getRowType());
        if (canUpdate) {
          newScan.pushDownContext.setAggSpec(aggSpec.withLimit(limit + offset));
        }
        AbstractAction action;
        if (newScan.pushDownContext.getAggSpec().isCompositeAggregation()) {
          action =
              (OSRequestBuilderAction)
                  requestBuilder -> requestBuilder.pushDownLimitToRequestTotal(limit, offset);
        } else {
          action = (OSRequestBuilderAction) requestBuilder -> {};
        }
        newScan.pushDownContext.add(PushDownType.LIMIT, new LimitDigest(limit, offset), action);
        return offset > 0 ? sort.copy(sort.getTraitSet(), List.of(newScan)) : newScan;
      } else {
        LimitDigest digest = new LimitDigest(limit, offset);
        if (pushDownContext.containsDigestOnTop(digest)) {
          // avoid stack overflow
          return null;
        }
        CalciteLogicalIndexScan newScan = this.copyWithNewSchema(getRowType());
        newScan.pushDownContext.add(
            PushDownType.LIMIT,
            digest,
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
}
