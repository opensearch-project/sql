/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.storage;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.hasEntry;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.data.type.ExprCoreType.DOUBLE;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.expression.DSL.literal;
import static org.opensearch.sql.expression.DSL.named;
import static org.opensearch.sql.expression.DSL.ref;
import static org.opensearch.sql.opensearch.utils.Utils.indexScan;
import static org.opensearch.sql.opensearch.utils.Utils.indexScanAgg;
import static org.opensearch.sql.opensearch.utils.Utils.noProjects;
import static org.opensearch.sql.opensearch.utils.Utils.projects;
import static org.opensearch.sql.planner.logical.LogicalPlanDSL.aggregation;
import static org.opensearch.sql.planner.logical.LogicalPlanDSL.eval;
import static org.opensearch.sql.planner.logical.LogicalPlanDSL.filter;
import static org.opensearch.sql.planner.logical.LogicalPlanDSL.limit;
import static org.opensearch.sql.planner.logical.LogicalPlanDSL.project;
import static org.opensearch.sql.planner.logical.LogicalPlanDSL.relation;
import static org.opensearch.sql.planner.logical.LogicalPlanDSL.remove;
import static org.opensearch.sql.planner.logical.LogicalPlanDSL.rename;
import static org.opensearch.sql.planner.logical.LogicalPlanDSL.sort;

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.search.fetch.subphase.FetchSourceContext;
import org.opensearch.sql.ast.tree.Sort;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.data.model.ExprBooleanValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.expression.aggregation.AvgAggregator;
import org.opensearch.sql.expression.aggregation.NamedAggregator;
import org.opensearch.sql.expression.config.ExpressionConfig;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory;
import org.opensearch.sql.opensearch.mapping.IndexMapping;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.logical.LogicalPlanDSL;
import org.opensearch.sql.planner.physical.AggregationOperator;
import org.opensearch.sql.planner.physical.FilterOperator;
import org.opensearch.sql.planner.physical.LimitOperator;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlanDSL;
import org.opensearch.sql.planner.physical.ProjectOperator;
import org.opensearch.sql.storage.Table;

@ExtendWith(MockitoExtension.class)
class OpenSearchIndexTest {

  private final DSL dsl = new ExpressionConfig().dsl(new ExpressionConfig().functionRepository());

  @Mock
  private OpenSearchClient client;

  @Mock
  private OpenSearchExprValueFactory exprValueFactory;

  @Mock
  private Settings settings;

  @Test
  void getFieldTypes() {
    when(client.getIndexMappings("test"))
        .thenReturn(
            ImmutableMap.of(
                "test",
                new IndexMapping(
                    ImmutableMap.<String, String>builder()
                        .put("name", "keyword")
                        .put("address", "text")
                        .put("age", "integer")
                        .put("account_number", "long")
                        .put("balance1", "float")
                        .put("balance2", "double")
                        .put("gender", "boolean")
                        .put("family", "nested")
                        .put("employer", "object")
                        .put("birthday", "date")
                        .put("id1", "byte")
                        .put("id2", "short")
                        .put("blob", "binary")
                        .build())));

    Table index = new OpenSearchIndex(client, prometheusService, settings, "test");
    Map<String, ExprType> fieldTypes = index.getFieldTypes();
    assertThat(
        fieldTypes,
        allOf(
            aMapWithSize(13),
            hasEntry("name", ExprCoreType.STRING),
            hasEntry("address", (ExprType) OpenSearchDataType.OPENSEARCH_TEXT),
            hasEntry("age", ExprCoreType.INTEGER),
            hasEntry("account_number", ExprCoreType.LONG),
            hasEntry("balance1", ExprCoreType.FLOAT),
            hasEntry("balance2", ExprCoreType.DOUBLE),
            hasEntry("gender", ExprCoreType.BOOLEAN),
            hasEntry("family", ExprCoreType.ARRAY),
            hasEntry("employer", ExprCoreType.STRUCT),
            hasEntry("birthday", ExprCoreType.TIMESTAMP),
            hasEntry("id1", ExprCoreType.BYTE),
            hasEntry("id2", ExprCoreType.SHORT),
            hasEntry("blob", (ExprType) OpenSearchDataType.OPENSEARCH_BINARY)
        ));
  }

  @Test
  void implementRelationOperatorOnly() {
    when(settings.getSettingValue(Settings.Key.QUERY_SIZE_LIMIT)).thenReturn(200);

    String indexName = "test";
    LogicalPlan plan = relation(indexName);
    Table index = new OpenSearchIndex(client, prometheusService, settings, indexName);
    assertEquals(
        new OpenSearchIndexScan(client, settings, indexName, exprValueFactory),
        index.implement(plan));
  }

  @Test
  void implementRelationOperatorWithOptimization() {
    when(settings.getSettingValue(Settings.Key.QUERY_SIZE_LIMIT)).thenReturn(200);

    String indexName = "test";
    LogicalPlan plan = relation(indexName);
    Table index = new OpenSearchIndex(client, prometheusService, settings, indexName);
    assertEquals(
        new OpenSearchIndexScan(client, settings, indexName, exprValueFactory),
        index.implement(index.optimize(plan)));
  }

  @Test
  void implementOtherLogicalOperators() {
    when(settings.getSettingValue(Settings.Key.QUERY_SIZE_LIMIT)).thenReturn(200);

    String indexName = "test";
    NamedExpression include = named("age", ref("age", INTEGER));
    ReferenceExpression exclude = ref("name", STRING);
    ReferenceExpression dedupeField = ref("name", STRING);
    Expression filterExpr = literal(ExprBooleanValue.of(true));
    List<NamedExpression> groupByExprs = Arrays.asList(named("age", ref("age", INTEGER)));
    List<NamedAggregator> aggregators =
        Arrays.asList(named("avg(age)", new AvgAggregator(Arrays.asList(ref("age", INTEGER)),
            DOUBLE)));
    Map<ReferenceExpression, ReferenceExpression> mappings =
        ImmutableMap.of(ref("name", STRING), ref("lastname", STRING));
    Pair<ReferenceExpression, Expression> newEvalField =
        ImmutablePair.of(ref("name1", STRING), ref("name", STRING));
    Integer sortCount = 100;
    Pair<Sort.SortOption, Expression> sortField =
        ImmutablePair.of(Sort.SortOption.DEFAULT_ASC, ref("name1", STRING));

    LogicalPlan plan =
        project(
            LogicalPlanDSL.dedupe(
                sort(
                    eval(
                        remove(
                            rename(
                                relation(indexName),
                                mappings),
                            exclude),
                        newEvalField),
                    sortField),
                dedupeField),
            include);

    Table index = new OpenSearchIndex(client, prometheusService, settings, indexName);
    assertEquals(
        PhysicalPlanDSL.project(
            PhysicalPlanDSL.dedupe(
                PhysicalPlanDSL.sort(
                    PhysicalPlanDSL.eval(
                        PhysicalPlanDSL.remove(
                            PhysicalPlanDSL.rename(
                                new OpenSearchIndexScan(client, settings, indexName,
                                    exprValueFactory),
                                mappings),
                            exclude),
                        newEvalField),
                    sortField),
                dedupeField),
            include),
        index.implement(plan));
  }

  @Test
  void shouldImplLogicalIndexScan() {
    when(settings.getSettingValue(Settings.Key.QUERY_SIZE_LIMIT)).thenReturn(200);

    ReferenceExpression field = ref("name", STRING);
    NamedExpression named = named("n", field);
    Expression filterExpr = dsl.equal(field, literal("John"));

    String indexName = "test";
    OpenSearchIndex index = new OpenSearchIndex(client, prometheusService, settings, indexName);
    PhysicalPlan plan = index.implement(
        project(
            indexScan(
                indexName,
                filterExpr
            ),
            named));

    assertTrue(plan instanceof ProjectOperator);
    assertTrue(((ProjectOperator) plan).getInput() instanceof OpenSearchIndexScan);
  }

  @Test
  void shouldNotPushDownFilterFarFromRelation() {
    when(settings.getSettingValue(Settings.Key.QUERY_SIZE_LIMIT)).thenReturn(200);

    ReferenceExpression field = ref("name", STRING);
    Expression filterExpr = dsl.equal(field, literal("John"));
    List<NamedExpression> groupByExprs = Arrays.asList(named("age", ref("age", INTEGER)));
    List<NamedAggregator> aggregators =
        Arrays.asList(named("avg(age)", new AvgAggregator(Arrays.asList(ref("age", INTEGER)),
            DOUBLE)));

    String indexName = "test";
    OpenSearchIndex index = new OpenSearchIndex(client, prometheusService, settings, indexName);
    PhysicalPlan plan = index.implement(
        filter(
            aggregation(
                relation(indexName),
                aggregators,
                groupByExprs
            ),
            filterExpr));

    assertTrue(plan instanceof FilterOperator);
  }

  @Test
  void shouldImplLogicalIndexScanAgg() {
    when(settings.getSettingValue(Settings.Key.QUERY_SIZE_LIMIT)).thenReturn(200);

    ReferenceExpression field = ref("name", STRING);
    Expression filterExpr = dsl.equal(field, literal("John"));
    List<NamedExpression> groupByExprs = Arrays.asList(named("age", ref("age", INTEGER)));
    List<NamedAggregator> aggregators =
        Arrays.asList(named("avg(age)", new AvgAggregator(Arrays.asList(ref("age", INTEGER)),
            DOUBLE)));

    String indexName = "test";
    OpenSearchIndex index = new OpenSearchIndex(client, prometheusService, settings, indexName);

    // IndexScanAgg without Filter
    PhysicalPlan plan = index.implement(
        filter(
            indexScanAgg(
                indexName,
                aggregators,
                groupByExprs
            ),
            filterExpr));

    assertTrue(plan.getChild().get(0) instanceof OpenSearchIndexScan);

    // IndexScanAgg with Filter
    plan = index.implement(
        indexScanAgg(
            indexName,
            filterExpr,
            aggregators,
            groupByExprs));
    assertTrue(plan instanceof OpenSearchIndexScan);
  }

  @Test
  void shouldNotPushDownAggregationFarFromRelation() {
    when(settings.getSettingValue(Settings.Key.QUERY_SIZE_LIMIT)).thenReturn(200);

    ReferenceExpression field = ref("name", STRING);
    Expression filterExpr = dsl.equal(field, literal("John"));
    List<NamedExpression> groupByExprs = Arrays.asList(named("age", ref("age", INTEGER)));
    List<NamedAggregator> aggregators =
        Arrays.asList(named("avg(age)", new AvgAggregator(Arrays.asList(ref("age", INTEGER)),
            DOUBLE)));

    String indexName = "test";
    OpenSearchIndex index = new OpenSearchIndex(client, prometheusService, settings, indexName);

    PhysicalPlan plan = index.implement(
        aggregation(
            filter(filter(
                relation(indexName),
                filterExpr), filterExpr),
            aggregators,
            groupByExprs));
    assertTrue(plan instanceof AggregationOperator);
  }

  @Test
  void shouldImplIndexScanWithSort() {
    when(settings.getSettingValue(Settings.Key.QUERY_SIZE_LIMIT)).thenReturn(200);

    ReferenceExpression field = ref("name", STRING);
    NamedExpression named = named("n", field);
    Expression sortExpr = ref("name", STRING);

    String indexName = "test";
    OpenSearchIndex index = new OpenSearchIndex(client, prometheusService, settings, indexName);
    PhysicalPlan plan = index.implement(
        project(
            indexScan(
                indexName,
                Pair.of(Sort.SortOption.DEFAULT_ASC, sortExpr)
            ),
            named));

    assertTrue(plan instanceof ProjectOperator);
    assertTrue(((ProjectOperator) plan).getInput() instanceof OpenSearchIndexScan);
  }

  @Test
  void shouldImplIndexScanWithLimit() {
    when(settings.getSettingValue(Settings.Key.QUERY_SIZE_LIMIT)).thenReturn(200);

    ReferenceExpression field = ref("name", STRING);
    NamedExpression named = named("n", field);

    String indexName = "test";
    OpenSearchIndex index = new OpenSearchIndex(client, prometheusService, settings, indexName);
    PhysicalPlan plan = index.implement(
        project(
            indexScan(
                indexName,
                1, 1, noProjects()
            ),
            named));

    assertTrue(plan instanceof ProjectOperator);
    assertTrue(((ProjectOperator) plan).getInput() instanceof OpenSearchIndexScan);
  }

  @Test
  void shouldImplIndexScanWithSortAndLimit() {
    when(settings.getSettingValue(Settings.Key.QUERY_SIZE_LIMIT)).thenReturn(200);

    ReferenceExpression field = ref("name", STRING);
    NamedExpression named = named("n", field);
    Expression sortExpr = ref("name", STRING);

    String indexName = "test";
    OpenSearchIndex index = new OpenSearchIndex(client, prometheusService, settings, indexName);
    PhysicalPlan plan = index.implement(
        project(
            indexScan(
                indexName,
                sortExpr,
                1, 1,
                noProjects()
            ),
            named));

    assertTrue(plan instanceof ProjectOperator);
    assertTrue(((ProjectOperator) plan).getInput() instanceof OpenSearchIndexScan);
  }

  @Test
  void shouldNotPushDownLimitFarFromRelationButUpdateScanSize() {
    when(settings.getSettingValue(Settings.Key.QUERY_SIZE_LIMIT)).thenReturn(200);

    String indexName = "test";
    OpenSearchIndex index = new OpenSearchIndex(client, prometheusService, settings, indexName);
    PhysicalPlan plan = index.implement(index.optimize(
        project(
            limit(
                sort(
                    relation("test"),
                    Pair.of(Sort.SortOption.DEFAULT_ASC,
                        dsl.abs(named("intV", ref("intV", INTEGER))))
                ),
                300, 1
            ),
            named("intV", ref("intV", INTEGER))
        )
    ));

    assertTrue(plan instanceof ProjectOperator);
    assertTrue(((ProjectOperator) plan).getInput() instanceof LimitOperator);
  }

  @Test
  void shouldPushDownProjects() {
    when(settings.getSettingValue(Settings.Key.QUERY_SIZE_LIMIT)).thenReturn(200);

    String indexName = "test";
    OpenSearchIndex index = new OpenSearchIndex(client, prometheusService, settings, indexName);
    PhysicalPlan plan = index.implement(
        project(
            indexScan(
                indexName, projects(ref("intV", INTEGER))
            ),
            named("i", ref("intV", INTEGER))));

    assertTrue(plan instanceof ProjectOperator);
    assertTrue(((ProjectOperator) plan).getInput() instanceof OpenSearchIndexScan);

    final FetchSourceContext fetchSource =
        ((OpenSearchIndexScan) ((ProjectOperator) plan).getInput()).getRequest()
            .getSourceBuilder().fetchSource();
    assertThat(fetchSource.includes(), arrayContaining("intV"));
    assertThat(fetchSource.excludes(), emptyArray());
  }
}
