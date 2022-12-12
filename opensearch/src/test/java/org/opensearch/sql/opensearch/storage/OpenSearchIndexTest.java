/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.storage;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.hasEntry;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.data.type.ExprCoreType.DOUBLE;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.expression.DSL.literal;
import static org.opensearch.sql.expression.DSL.named;
import static org.opensearch.sql.expression.DSL.ref;
import static org.opensearch.sql.opensearch.data.type.OpenSearchDataType.OPENSEARCH_TEXT_KEYWORD;
import static org.opensearch.sql.planner.logical.LogicalPlanDSL.eval;
import static org.opensearch.sql.planner.logical.LogicalPlanDSL.project;
import static org.opensearch.sql.planner.logical.LogicalPlanDSL.relation;
import static org.opensearch.sql.planner.logical.LogicalPlanDSL.remove;
import static org.opensearch.sql.planner.logical.LogicalPlanDSL.rename;
import static org.opensearch.sql.planner.logical.LogicalPlanDSL.sort;

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.ast.tree.Sort;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.data.model.ExprBooleanValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.expression.aggregation.AvgAggregator;
import org.opensearch.sql.expression.aggregation.NamedAggregator;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory;
import org.opensearch.sql.opensearch.mapping.IndexMapping;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.logical.LogicalPlanDSL;
import org.opensearch.sql.planner.physical.PhysicalPlanDSL;
import org.opensearch.sql.storage.Table;

@ExtendWith(MockitoExtension.class)
class OpenSearchIndexTest {

  private final String indexName = "test";

  @Mock
  private OpenSearchClient client;

  @Mock
  private OpenSearchExprValueFactory exprValueFactory;

  @Mock
  private Settings settings;

  @Mock
  private Table table;

  private OpenSearchIndex index;

  @BeforeEach
  void setUp() {
    this.index = new OpenSearchIndex(client, settings, indexName);
  }

  @Test
  void isExist() {
    when(client.exists(indexName)).thenReturn(true);

    assertTrue(index.exists());
  }

  @Test
  void createIndex() {
    Map<String, Object> mappings = ImmutableMap.of(
        "properties",
        ImmutableMap.of(
            "name", "text_keyword",
            "age", "integer"));
    doNothing().when(client).createIndex(indexName, mappings);

    Map<String, ExprType> schema = new HashMap<>();
    schema.put("name", OPENSEARCH_TEXT_KEYWORD);
    schema.put("age", INTEGER);
    index.create(schema);
  }

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

    // Run more than once to confirm caching logic is covered and can work
    for (int i = 0; i < 2; i++) {
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
  }

  @Test
  void implementRelationOperatorOnly() {
    when(settings.getSettingValue(Settings.Key.QUERY_SIZE_LIMIT)).thenReturn(200);
    when(client.getIndexMaxResultWindows("test")).thenReturn(Map.of("test", 10000));

    LogicalPlan plan = index.createScanBuilder();
    Integer maxResultWindow = index.getMaxResultWindow();
    assertEquals(
        new OpenSearchIndexScan(client, settings, indexName, maxResultWindow, exprValueFactory),
        index.implement(plan));
  }

  @Test
  void implementRelationOperatorWithOptimization() {
    when(settings.getSettingValue(Settings.Key.QUERY_SIZE_LIMIT)).thenReturn(200);
    when(client.getIndexMaxResultWindows("test")).thenReturn(Map.of("test", 10000));

    LogicalPlan plan = index.createScanBuilder();
    Integer maxResultWindow = index.getMaxResultWindow();
    assertEquals(
        new OpenSearchIndexScan(client, settings, indexName, maxResultWindow, exprValueFactory),
        index.implement(index.optimize(plan)));
  }

  @Test
  void implementOtherLogicalOperators() {
    when(settings.getSettingValue(Settings.Key.QUERY_SIZE_LIMIT)).thenReturn(200);
    when(client.getIndexMaxResultWindows("test")).thenReturn(Map.of("test", 10000));

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
                                index.createScanBuilder(),
                                mappings),
                            exclude),
                        newEvalField),
                    sortField),
                dedupeField),
            include);

    Integer maxResultWindow = index.getMaxResultWindow();
    assertEquals(
        PhysicalPlanDSL.project(
            PhysicalPlanDSL.dedupe(
                PhysicalPlanDSL.sort(
                    PhysicalPlanDSL.eval(
                        PhysicalPlanDSL.remove(
                            PhysicalPlanDSL.rename(
                                new OpenSearchIndexScan(client, settings, indexName,
                                    maxResultWindow, exprValueFactory),
                                mappings),
                            exclude),
                        newEvalField),
                    sortField),
                dedupeField),
            include),
        index.implement(plan));
  }
}
