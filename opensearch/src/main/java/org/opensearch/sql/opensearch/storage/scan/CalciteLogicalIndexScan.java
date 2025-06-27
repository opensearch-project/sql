/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Getter;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.sort.ScoreSortBuilder;
import org.opensearch.search.sort.SortBuilder;
import org.opensearch.search.sort.SortBuilders;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.data.type.OpenSearchTextType;
import org.opensearch.sql.opensearch.planner.physical.EnumerableIndexScanRule;
import org.opensearch.sql.opensearch.planner.physical.OpenSearchIndexRules;
import org.opensearch.sql.opensearch.request.AggregateAnalyzer;
import org.opensearch.sql.opensearch.request.PredicateAnalyzer;
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

  public CalciteLogicalIndexScan pushDownFilter(Filter filter) {
    try {
      CalciteLogicalIndexScan newScan = this.copyWithNewSchema(filter.getRowType());
      List<String> schema = this.getRowType().getFieldNames();
      Map<String, ExprType> filedTypes = this.osIndex.getFieldTypes();
      QueryBuilder filterBuilder =
          PredicateAnalyzer.analyze(filter.getCondition(), schema, filedTypes);
      newScan.pushDownContext.add(
          PushDownAction.of(
              PushDownType.FILTER,
              filter.getCondition(),
              requestBuilder -> requestBuilder.pushDownFilter(filterBuilder)));

      // TODO: handle the case where condition contains a score function
      return newScan;
    } catch (Exception e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Cannot pushdown the filter condition.", e);
      } else {
        LOG.info("Cannot pushdown the filter condition.");
      }
    }
    return null;
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

    // Projection may alter the index of the collations.
    // E.g. For sort age
    // `Sort($1)\n TableScan(name, age)` may become
    // `Sort($0)\n Project(age)\n  TableScan(name, age)` after projection.
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
            .toList();

    newScan.pushDownContext.add(
        PushDownAction.of(
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

  public CalciteLogicalIndexScan pushDownAggregate(Aggregate aggregate) {
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
      List<String> schema = this.getRowType().getFieldNames();
      Map<String, ExprType> fieldTypes = this.osIndex.getFieldTypes();
      List<String> outputFields = aggregate.getRowType().getFieldNames();
      final Pair<List<AggregationBuilder>, OpenSearchAggregationResponseParser> aggregationBuilder =
          AggregateAnalyzer.analyze(aggregate, schema, fieldTypes, outputFields);
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
      if (LOG.isDebugEnabled()) {
        LOG.debug("Cannot pushdown the aggregate {}", aggregate, e);
      } else {
        LOG.info("Cannot pushdown the aggregate {}, ", aggregate);
      }
    }
    return null;
  }

  public CalciteLogicalIndexScan pushDownLimit(Integer limit, Integer offset) {
    try {
      CalciteLogicalIndexScan newScan = this.copyWithNewSchema(getRowType());
      newScan.pushDownContext.add(
          PushDownAction.of(
              PushDownType.LIMIT,
              limit,
              requestBuilder -> requestBuilder.pushDownLimit(limit, offset)));
      return newScan;
    } catch (Exception e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Cannot pushdown limit {} with offset {}", limit, offset, e);
      } else {
        LOG.info("Cannot pushdown limit {} with offset {}", limit, offset);
      }
    }
    return null;
  }

  public CalciteLogicalIndexScan pushDownSort(List<RelFieldCollation> collations) {
    try {
      List<String> collationNames = getCollationNames(collations);
      if (getPushDownContext().isAggregatePushed() && hasAggregatorInSortBy(collationNames)) {
        // If aggregation is pushed down, we cannot push down sorts where its by fields contain
        // aggregators.
        return null;
      }

      // Merge with existing sort if any
      RelCollation existingCollation = getTraitSet().getCollation();
      List<RelFieldCollation> existingFieldCollations =
          existingCollation == null ? List.of() : existingCollation.getFieldCollations();
      List<RelFieldCollation> mergedCollations =
          mergeCollations(existingFieldCollations, collations);

      // Propagate the sort to the new scan
      RelTraitSet traitsWithCollations = getTraitSet().plus(RelCollations.of(mergedCollations));
      CalciteLogicalIndexScan newScan =
          new CalciteLogicalIndexScan(
              getCluster(),
              traitsWithCollations,
              hints,
              table,
              osIndex,
              getRowType(),
              cloneWithoutSort(pushDownContext));

      List<SortBuilder<?>> builders = new ArrayList<>();
      for (RelFieldCollation collation : mergedCollations) {
        int index = collation.getFieldIndex();
        String fieldName = this.getRowType().getFieldNames().get(index);
        RelFieldCollation.Direction direction = collation.getDirection();
        RelFieldCollation.NullDirection nullDirection = collation.nullDirection;
        // Default sort order is ASCENDING
        SortOrder order =
            RelFieldCollation.Direction.DESCENDING.equals(direction)
                ? SortOrder.DESC
                : SortOrder.ASC;
        // TODO: support script sort and distance sort
        SortBuilder<?> sortBuilder;
        if (ScoreSortBuilder.NAME.equals(fieldName)) {
          sortBuilder = SortBuilders.scoreSort();
        } else {
          String missing =
              switch (nullDirection) {
                case FIRST -> "_first";
                case LAST -> "_last";
                default -> null;
              };
          // Keyword field is optimized for sorting in OpenSearch
          String fieldNameKeyword =
              OpenSearchTextType.convertTextToKeyword(
                  fieldName, osIndex.getFieldTypes().get(fieldName));
          sortBuilder = SortBuilders.fieldSort(fieldNameKeyword).missing(missing);
        }
        builders.add(sortBuilder.order(order));
      }
      newScan.pushDownContext.add(
          PushDownAction.of(
              PushDownType.SORT,
              builders.toString(),
              requestBuilder -> requestBuilder.pushDownSort(builders)));
      return newScan;
    } catch (Exception e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Cannot pushdown the sort {}", getCollationNames(collations), e);
      } else {
        LOG.info("Cannot pushdown the sort {}, ", getCollationNames(collations));
      }
    }
    return null;
  }

  private List<String> getCollationNames(List<RelFieldCollation> collations) {
    return collations.stream()
        .map(collation -> getRowType().getFieldNames().get(collation.getFieldIndex()))
        .toList();
  }

  /**
   * Check if the sort by collations contains any aggregators that are pushed down. E.g. In `stats
   * avg(age) as avg_age by state | sort avg_age`, the sort clause has `avg_age` which is an
   * aggregator. The function will return true in this case.
   *
   * @param collations List of collation names to check against aggregators.
   * @return True if any collation name matches an aggregator output, false otherwise.
   */
  private boolean hasAggregatorInSortBy(List<String> collations) {
    Stream<LogicalAggregate> aggregates =
        pushDownContext.stream()
            .filter(action -> action.type() == PushDownType.AGGREGATION)
            .map(action -> ((LogicalAggregate) action.digest()));
    return aggregates
        .map(aggregate -> isAnyCollationNameInAggregateOutput(aggregate, collations))
        .reduce(false, Boolean::logicalOr);
  }

  private static boolean isAnyCollationNameInAggregateOutput(
      LogicalAggregate aggregate, List<String> collations) {
    List<String> fieldNames = aggregate.getRowType().getFieldNames();
    // The output fields of the aggregate are in the format of
    // [...grouping fields, ...aggregator fields], so we set an offset to skip
    // the grouping fields.
    int groupOffset = aggregate.getGroupSet().cardinality();
    List<String> fieldsWithoutGrouping = fieldNames.subList(groupOffset, fieldNames.size());
    return collations.stream()
        .map(fieldsWithoutGrouping::contains)
        .reduce(false, Boolean::logicalOr);
  }

  /**
   * Merges existing and new collations, ensuring that the last occurrence of each field index takes
   * precedence.
   *
   * @param existingCollations Existing collation list.
   * @param newCollations New collation list to be merged.
   * @return Merged list of collations.
   */
  private static List<RelFieldCollation> mergeCollations(
      List<RelFieldCollation> existingCollations, List<RelFieldCollation> newCollations) {
    Map<Integer, RelFieldCollation> mergedCollations = new LinkedHashMap<>();

    for (RelFieldCollation collation : newCollations) {
      mergedCollations.putIfAbsent(collation.getFieldIndex(), collation);
    }

    for (RelFieldCollation collation : existingCollations) {
      // Existing collations will not replace the new one
      // E.g. in `sort age | sort - age`, the first sort (existing one) will be ignored
      mergedCollations.putIfAbsent(collation.getFieldIndex(), collation);
    }
    return new ArrayList<>(mergedCollations.values());
  }

  /**
   * Create a new {@link PushDownContext} without the collation action.
   *
   * @param pushDownContext The original push-down context.
   * @return A new push-down context without the collation action.
   */
  private static PushDownContext cloneWithoutSort(PushDownContext pushDownContext) {
    PushDownContext newContext = new PushDownContext();
    for (PushDownAction action : pushDownContext) {
      if (action.type() != PushDownType.SORT) {
        newContext.add(action);
      }
    }
    return newContext;
  }
}
