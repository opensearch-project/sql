/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.storage.scan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.ast.tree.Sort.NullOrder.NULL_FIRST;
import static org.opensearch.sql.ast.tree.Sort.SortOrder.ASC;
import static org.opensearch.sql.data.model.ExprValueUtils.integerValue;
import static org.opensearch.sql.data.type.ExprCoreType.DOUBLE;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.LONG;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.planner.logical.LogicalPlanDSL.aggregation;
import static org.opensearch.sql.planner.logical.LogicalPlanDSL.filter;
import static org.opensearch.sql.planner.logical.LogicalPlanDSL.highlight;
import static org.opensearch.sql.planner.logical.LogicalPlanDSL.limit;
import static org.opensearch.sql.planner.logical.LogicalPlanDSL.project;
import static org.opensearch.sql.planner.logical.LogicalPlanDSL.relation;
import static org.opensearch.sql.planner.logical.LogicalPlanDSL.sort;
import static org.opensearch.sql.planner.optimizer.rule.read.TableScanPushDown.PUSH_DOWN_AGGREGATION;
import static org.opensearch.sql.planner.optimizer.rule.read.TableScanPushDown.PUSH_DOWN_FILTER;
import static org.opensearch.sql.planner.optimizer.rule.read.TableScanPushDown.PUSH_DOWN_HIGHLIGHT;
import static org.opensearch.sql.planner.optimizer.rule.read.TableScanPushDown.PUSH_DOWN_LIMIT;
import static org.opensearch.sql.planner.optimizer.rule.read.TableScanPushDown.PUSH_DOWN_PROJECT;
import static org.opensearch.sql.planner.optimizer.rule.read.TableScanPushDown.PUSH_DOWN_SORT;

import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Builder;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.opensearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.opensearch.search.sort.SortBuilder;
import org.opensearch.search.sort.SortBuilders;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.tree.Sort.SortOption;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.HighlightExpression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.request.OpenSearchRequestBuilder;
import org.opensearch.sql.opensearch.response.agg.CompositeAggregationParser;
import org.opensearch.sql.opensearch.response.agg.OpenSearchAggregationResponseParser;
import org.opensearch.sql.opensearch.response.agg.SingleValueParser;
import org.opensearch.sql.opensearch.storage.OpenSearchIndexScan;
import org.opensearch.sql.opensearch.storage.script.aggregation.AggregationQueryBuilder;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.optimizer.LogicalPlanOptimizer;
import org.opensearch.sql.planner.optimizer.rule.read.CreateTableScanBuilder;
import org.opensearch.sql.storage.Table;


@ExtendWith(MockitoExtension.class)
class OpenSearchIndexScanOptimizationTest {

  @Mock
  private Table table;

  @Mock
  private OpenSearchIndexScan indexScan;

  private OpenSearchIndexScanBuilder indexScanBuilder;

  @Mock
  private OpenSearchRequestBuilder requestBuilder;

  private Runnable[] verifyPushDownCalls = {};

  @BeforeEach
  void setUp() {
    indexScanBuilder = new OpenSearchIndexScanBuilder(indexScan);
    when(table.createScanBuilder()).thenReturn(indexScanBuilder);
    when(indexScan.getRequestBuilder()).thenReturn(requestBuilder);
  }

  @Test
  void test_project_push_down() {
    assertEqualsAfterOptimization(
        project(
            indexScanAggBuilder(
                withProjectPushedDown(DSL.ref("intV", INTEGER))),
            DSL.named("i", DSL.ref("intV", INTEGER))
        ),
        project(
            relation("schema", table),
            DSL.named("i", DSL.ref("intV", INTEGER)))
    );
  }

  /**
   * SELECT intV as i FROM schema WHERE intV = 1.
   */
  @Test
  void test_filter_push_down() {
    assertEqualsAfterOptimization(
        project(
            indexScanBuilder(
                //withProjectPushedDown(DSL.ref("intV", INTEGER)),
                withFilterPushedDown(QueryBuilders.termQuery("intV", 1))
            ),
            DSL.named("i", DSL.ref("intV", INTEGER))
        ),
        project(
            filter(
                relation("schema", table),
                DSL.equal(DSL.ref("intV", INTEGER), DSL.literal(integerValue(1)))
            ),
            DSL.named("i", DSL.ref("intV", INTEGER))
        )
    );
  }

  /**
   * SELECT avg(intV) FROM schema GROUP BY string_value.
   */
  @Test
  void test_aggregation_push_down() {
    assertEqualsAfterOptimization(
        project(
            indexScanAggBuilder(
                withAggregationPushedDown(
                    aggregate("AVG(intV)")
                        .aggregateBy("intV")
                        .groupBy("longV")
                        .resultTypes(Map.of(
                            "AVG(intV)", DOUBLE,
                            "longV", LONG)))),
            DSL.named("AVG(intV)", DSL.ref("AVG(intV)", DOUBLE))),
        project(
            aggregation(
                relation("schema", table),
                ImmutableList
                    .of(DSL.named("AVG(intV)",
                        DSL.avg(DSL.ref("intV", INTEGER)))),
                ImmutableList.of(DSL.named("longV", DSL.ref("longV", LONG)))),
            DSL.named("AVG(intV)", DSL.ref("AVG(intV)", DOUBLE))
        )
    );
  }

  /*
  @Disabled("This test should be enabled once https://github.com/opensearch-project/sql/issues/912 is fixed")
  @Test
  void aggregation_cant_merge_indexScan_with_project() {
    assertEquals(
        aggregation(
            OpenSearchLogicalIndexScan.builder().relationName("schema")
                .filter(DSL.equal(DSL.ref("intV", INTEGER), DSL.literal(integerValue(1))))
                .projectList(ImmutableSet.of(DSL.ref("intV", INTEGER)))
                .build(),
            ImmutableList
                .of(DSL.named("AVG(intV)",
                    DSL.avg(DSL.ref("intV", INTEGER)))),
            ImmutableList.of(DSL.named("longV",
                DSL.abs(DSL.ref("longV", LONG))))),
        optimize(
            aggregation(
                OpenSearchLogicalIndexScan.builder().relationName("schema")
                    .filter(DSL.equal(DSL.ref("intV", INTEGER), DSL.literal(integerValue(1))))
                    .projectList(
                        ImmutableSet.of(DSL.ref("intV", INTEGER)))
                    .build(),
                ImmutableList
                    .of(DSL.named("AVG(intV)",
                        DSL.avg(DSL.ref("intV", INTEGER)))),
                ImmutableList.of(DSL.named("longV",
                    DSL.abs(DSL.ref("longV", LONG))))))
    );
  }
  */

  /**
   * Sort - Relation --> IndexScan.
   */
  @Test
  void test_sort_push_down() {
    assertEqualsAfterOptimization(
        indexScanBuilder(
            withSortPushedDown(
                SortBuilders.fieldSort("intV").order(SortOrder.ASC).missing("_first"))
        ),
        sort(
            relation("schema", table),
            Pair.of(SortOption.DEFAULT_ASC, DSL.ref("intV", INTEGER))
        )
    );
  }

  @Test
  void test_limit_push_down() {
    assertEqualsAfterOptimization(
        project(
            indexScanBuilder(
                withLimitPushedDown(1, 1)),
            DSL.named("intV", DSL.ref("intV", INTEGER))
        ),
        project(
            limit(
                relation("schema", table),
                1, 1),
            DSL.named("intV", DSL.ref("intV", INTEGER))
        )
    );
  }

  @Test
  void test_highlight_push_down() {
    assertEqualsAfterOptimization(
        project(
            indexScanBuilder(
                withHighlightPushedDown("*", Collections.emptyMap())),
            DSL.named("highlight(*)",
                new HighlightExpression(DSL.literal("*")))
        ),
        project(
            highlight(
                relation("schema", table),
                DSL.literal("*"), Collections.emptyMap()),
                DSL.named("highlight(*)",
                    new HighlightExpression(DSL.literal("*")))
        )
    );
  }

  /**
   * SELECT avg(intV) FROM schema WHERE intV = 1 GROUP BY string_value.
   */
  @Test
  void test_aggregation_filter_push_down() {
    assertEqualsAfterOptimization(
        project(
            indexScanAggBuilder(
                withFilterPushedDown(QueryBuilders.termQuery("intV", 1)),
                withAggregationPushedDown(
                    aggregate("AVG(intV)")
                        .aggregateBy("intV")
                        .groupBy("longV")
                        .resultTypes(Map.of(
                            "AVG(intV)", DOUBLE,
                            "longV", LONG)))),
            DSL.named("AVG(intV)", DSL.ref("AVG(intV)", DOUBLE))
        ),
        project(
            aggregation(
                filter(
                    relation("schema", table),
                    DSL.equal(DSL.ref("intV", INTEGER), DSL.literal(integerValue(1)))
                ),
                ImmutableList
                    .of(DSL.named("AVG(intV)",
                        DSL.avg(DSL.ref("intV", INTEGER)))),
                ImmutableList.of(DSL.named("longV", DSL.ref("longV", LONG)))),
            DSL.named("AVG(intV)", DSL.ref("AVG(intV)", DOUBLE))
        )
    );
  }

  /**
   * Sort - Filter - Relation --> IndexScan.
   */
  @Test
  void test_sort_filter_push_down() {
    assertEqualsAfterOptimization(
        indexScanBuilder(
            withFilterPushedDown(QueryBuilders.termQuery("intV", 1)),
            withSortPushedDown(
                SortBuilders.fieldSort("longV").order(SortOrder.ASC).missing("_first"))
        ),
        sort(
            filter(
                relation("schema", table),
                DSL.equal(DSL.ref("intV", INTEGER), DSL.literal(integerValue(1)))
            ),
            Pair.of(SortOption.DEFAULT_ASC, DSL.ref("longV", LONG))
        )
    );
  }

  /**
   * SELECT avg(intV) FROM schema GROUP BY stringV ORDER BY stringV.
   */
  @Test
  void test_sort_aggregation_push_down() {
    assertEqualsAfterOptimization(
        project(
            indexScanAggBuilder(
                withAggregationPushedDown(
                    aggregate("AVG(intV)")
                        .aggregateBy("intV")
                        .groupBy("stringV")
                        .sortBy(SortOption.DEFAULT_DESC)
                        .resultTypes(Map.of(
                            "AVG(intV)", DOUBLE,
                            "stringV", STRING)))),
            DSL.named("AVG(intV)", DSL.ref("AVG(intV)", DOUBLE))),
        project(
            sort(
                aggregation(
                    relation("schema", table),
                    ImmutableList
                        .of(DSL.named("AVG(intV)", DSL.avg(DSL.ref("intV", INTEGER)))),
                    ImmutableList.of(DSL.named("stringV", DSL.ref("stringV", STRING)))),
                Pair.of(SortOption.DEFAULT_DESC, DSL.ref("stringV", STRING))
            ),
            DSL.named("AVG(intV)", DSL.ref("AVG(intV)", DOUBLE))
        )
    );
  }

  @Test
  void test_limit_sort_filter_push_down() {
    assertEqualsAfterOptimization(
        project(
            indexScanBuilder(
                withFilterPushedDown(QueryBuilders.termQuery("intV", 1)),
                withSortPushedDown(
                    SortBuilders.fieldSort("longV").order(SortOrder.ASC).missing("_first")),
                withLimitPushedDown(1, 1)),
            DSL.named("intV", DSL.ref("intV", INTEGER))
        ),
        project(
            limit(
                sort(
                    filter(
                        relation("schema", table),
                        DSL.equal(DSL.ref("intV", INTEGER), DSL.literal(integerValue(1)))
                    ),
                    Pair.of(SortOption.DEFAULT_ASC, DSL.ref("longV", LONG))
                ), 1, 1
            ),
            DSL.named("intV", DSL.ref("intV", INTEGER))
        )
    );
  }

  /*
   * Project(ListA) -> Project(ListB) -> Relation.
   * -- will be optimized as
   * Project(ListA) -> Project(ListB) -> Relation(project=ListB).
   */
  @Test
  void only_one_project_should_be_push() {
    assertEqualsAfterOptimization(
        project(
            project(
                indexScanBuilder(
                    withProjectPushedDown(
                        DSL.ref("intV", INTEGER),
                        DSL.ref("stringV", STRING))),
                DSL.named("i", DSL.ref("intV", INTEGER)),
                DSL.named("s", DSL.ref("stringV", STRING))
            ),
            DSL.named("i", DSL.ref("intV", INTEGER))
        ),
        project(
            project(
                relation("schema", table),
                DSL.named("i", DSL.ref("intV", INTEGER)),
                DSL.named("s", DSL.ref("stringV", STRING))
            ),
            DSL.named("i", DSL.ref("intV", INTEGER))
        )
    );
  }

  @Test
  void sort_with_expression_cannot_merge_with_relation() {
    assertEqualsAfterOptimization(
        sort(
            indexScanBuilder(),
            Pair.of(SortOption.DEFAULT_ASC, DSL.abs(DSL.ref("intV", INTEGER)))
        ),
        sort(
            relation("schema", table),
            Pair.of(SortOption.DEFAULT_ASC, DSL.abs(DSL.ref("intV", INTEGER)))
        )
    );
  }

  @Test
  void sort_with_expression_cannot_merge_with_aggregation() {
    assertEqualsAfterOptimization(
        sort(
            indexScanAggBuilder(
                withAggregationPushedDown(
                    aggregate("AVG(intV)")
                        .aggregateBy("intV")
                        .groupBy("stringV")
                        .resultTypes(Map.of(
                            "AVG(intV)", DOUBLE,
                            "stringV", STRING)))),
            Pair.of(SortOption.DEFAULT_ASC, DSL.abs(DSL.ref("intV", INTEGER)))
        ),
        sort(
            aggregation(
                relation("schema", table),
                ImmutableList
                    .of(DSL.named("AVG(intV)", DSL.avg(DSL.ref("intV", INTEGER)))),
                ImmutableList.of(DSL.named("stringV", DSL.ref("stringV", STRING)))),
            Pair.of(SortOption.DEFAULT_ASC, DSL.abs(DSL.ref("intV", INTEGER)))
        )
    );
  }

  @Test
  void aggregation_cant_merge_index_scan_with_limit() {
    assertEqualsAfterOptimization(
        project(
            aggregation(
                indexScanBuilder(
                    withLimitPushedDown(10, 0)),
                ImmutableList
                    .of(DSL.named("AVG(intV)",
                        DSL.avg(DSL.ref("intV", INTEGER)))),
                ImmutableList.of(DSL.named("longV",
                    DSL.abs(DSL.ref("longV", LONG))))),
            DSL.named("AVG(intV)", DSL.ref("AVG(intV)", DOUBLE))),
        project(
            aggregation(
                limit(
                    relation("schema", table),
                    10, 0),
                ImmutableList
                    .of(DSL.named("AVG(intV)",
                        DSL.avg(DSL.ref("intV", INTEGER)))),
                ImmutableList.of(DSL.named("longV",
                    DSL.abs(DSL.ref("longV", LONG))))),
            DSL.named("AVG(intV)", DSL.ref("AVG(intV)", DOUBLE))));
  }

  /**
   * Can't Optimize the following query.
   * SELECT avg(intV) FROM schema GROUP BY stringV ORDER BY avg(intV).
   */
  @Test
  void sort_refer_to_aggregator_should_not_merge_with_indexAgg() {
    assertEqualsAfterOptimization(
        project(
            sort(
                indexScanAggBuilder(
                    withAggregationPushedDown(
                        aggregate("AVG(intV)")
                            .aggregateBy("intV")
                            .groupBy("stringV")
                            .resultTypes(Map.of(
                                "AVG(intV)", DOUBLE,
                                "stringV", STRING)))),
                Pair.of(SortOption.DEFAULT_ASC, DSL.ref("AVG(intV)", INTEGER))
            ),
            DSL.named("AVG(intV)", DSL.ref("AVG(intV)", DOUBLE))),
        project(
            sort(
                aggregation(
                    relation("schema", table),
                    ImmutableList
                        .of(DSL.named("AVG(intV)", DSL.avg(DSL.ref("intV", INTEGER)))),
                    ImmutableList.of(DSL.named("stringV", DSL.ref("stringV", STRING)))),
                Pair.of(SortOption.DEFAULT_ASC, DSL.ref("AVG(intV)", INTEGER))
            ),
            DSL.named("AVG(intV)", DSL.ref("AVG(intV)", DOUBLE))
        )
    );
  }

  @Test
  void project_literal_should_not_be_pushed_down() {
    assertEqualsAfterOptimization(
        project(
            indexScanBuilder(),
            DSL.named("i", DSL.literal("str"))
        ),
        optimize(
            project(
                relation("schema", table),
                DSL.named("i", DSL.literal("str"))
            )
        )
    );
  }

  private OpenSearchIndexScanBuilder indexScanBuilder(Runnable... verifyPushDownCalls) {
    this.verifyPushDownCalls = verifyPushDownCalls;
    return new OpenSearchIndexScanBuilder(new OpenSearchIndexScanQueryBuilder(indexScan));
  }

  private OpenSearchIndexScanBuilder indexScanAggBuilder(Runnable... verifyPushDownCalls) {
    this.verifyPushDownCalls = verifyPushDownCalls;
    return new OpenSearchIndexScanBuilder(new OpenSearchIndexScanAggregationBuilder(indexScan));
  }

  private void assertEqualsAfterOptimization(LogicalPlan expected, LogicalPlan actual) {
    assertEquals(expected, optimize(actual));

    // Trigger build to make sure all push down actually happened in scan builder
    indexScanBuilder.build();

    // Verify to make sure all push down methods are called as expected
    if (verifyPushDownCalls.length == 0) {
      reset(indexScan);
    } else {
      Arrays.stream(verifyPushDownCalls).forEach(Runnable::run);
    }
  }

  private Runnable withFilterPushedDown(QueryBuilder filteringCondition) {
    return () -> verify(requestBuilder, times(1)).pushDown(filteringCondition);
  }

  private Runnable withAggregationPushedDown(
      AggregationAssertHelper.AggregationAssertHelperBuilder aggregation) {

    // Assume single term bucket and AVG metric in all tests in this suite
    CompositeAggregationBuilder aggBuilder = AggregationBuilders.composite(
        "composite_buckets",
        Collections.singletonList(
            new TermsValuesSourceBuilder(aggregation.groupBy)
                .field(aggregation.groupBy)
                .order(aggregation.sortBy.getSortOrder() == ASC ? "asc" : "desc")
                .missingOrder(aggregation.sortBy.getNullOrder() == NULL_FIRST ? "first" : "last")
                .missingBucket(true)))
        .subAggregation(
            AggregationBuilders.avg(aggregation.aggregateName)
                .field(aggregation.aggregateBy))
        .size(AggregationQueryBuilder.AGGREGATION_BUCKET_SIZE);

    List<AggregationBuilder> aggBuilders = Collections.singletonList(aggBuilder);
    OpenSearchAggregationResponseParser responseParser =
        new CompositeAggregationParser(
            new SingleValueParser(aggregation.aggregateName));

    return () -> {
      verify(requestBuilder, times(1)).pushDownAggregation(Pair.of(aggBuilders, responseParser));
      verify(requestBuilder, times(1)).pushTypeMapping(aggregation.resultTypes
          .entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
                e -> OpenSearchDataType.of(e.getValue()))));
    };
  }

  private Runnable withSortPushedDown(SortBuilder<?>... sorts) {
    return () -> verify(requestBuilder, times(1)).pushDownSort(Arrays.asList(sorts));
  }

  private Runnable withLimitPushedDown(int size, int offset) {
    return () -> verify(requestBuilder, times(1)).pushDownLimit(size, offset);
  }

  private Runnable withProjectPushedDown(ReferenceExpression... references) {
    return () -> verify(requestBuilder, times(1)).pushDownProjects(
        new HashSet<>(Arrays.asList(references)));
  }

  private Runnable withHighlightPushedDown(String field, Map<String, Literal> arguments) {
    return () -> verify(requestBuilder, times(1)).pushDownHighlight(field, arguments);
  }

  private static AggregationAssertHelper.AggregationAssertHelperBuilder aggregate(String aggName) {
    var aggBuilder = new AggregationAssertHelper.AggregationAssertHelperBuilder();
    aggBuilder.aggregateName = aggName;
    aggBuilder.sortBy = SortOption.DEFAULT_ASC;
    return aggBuilder;
  }

  /** Assertion helper for readability. */
  @Builder
  private static class AggregationAssertHelper {

    String aggregateName;

    String aggregateBy;

    String groupBy;

    SortOption sortBy;

    Map<String, ExprType> resultTypes;
  }

  private LogicalPlan optimize(LogicalPlan plan) {
    LogicalPlanOptimizer optimizer = new LogicalPlanOptimizer(List.of(
        new CreateTableScanBuilder(),
        PUSH_DOWN_FILTER,
        PUSH_DOWN_AGGREGATION,
        PUSH_DOWN_SORT,
        PUSH_DOWN_LIMIT,
        PUSH_DOWN_HIGHLIGHT,
        PUSH_DOWN_PROJECT));
    return optimizer.optimize(plan);
  }
}
