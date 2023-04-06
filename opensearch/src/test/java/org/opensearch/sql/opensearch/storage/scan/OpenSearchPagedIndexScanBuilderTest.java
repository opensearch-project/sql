/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.storage.scan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.data.model.ExprValueUtils.integerValue;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.planner.logical.LogicalPlanDSL.filter;
import static org.opensearch.sql.planner.logical.LogicalPlanDSL.highlight;
import static org.opensearch.sql.planner.logical.LogicalPlanDSL.project;
import static org.opensearch.sql.planner.logical.LogicalPlanDSL.relation;
import static org.opensearch.sql.planner.optimizer.rule.read.TableScanPushDown.PUSH_DOWN_FILTER;
import static org.opensearch.sql.planner.optimizer.rule.read.TableScanPushDown.PUSH_DOWN_HIGHLIGHT;
import static org.opensearch.sql.planner.optimizer.rule.read.TableScanPushDown.PUSH_DOWN_PROJECT;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.HighlightExpression;
import org.opensearch.sql.opensearch.request.InitialPageRequestBuilder;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.logical.LogicalRelation;
import org.opensearch.sql.planner.optimizer.LogicalPlanOptimizer;
import org.opensearch.sql.planner.optimizer.rule.CreatePagingTableScanBuilder;
import org.opensearch.sql.storage.Table;

@ExtendWith(MockitoExtension.class)
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
public class OpenSearchPagedIndexScanBuilderTest {

  @Mock
  private Table table;

  @Mock
  private OpenSearchPagedIndexScan indexScan;

  @Mock
  private InitialPageRequestBuilder requestBuilder;

  private OpenSearchPagedIndexScanBuilder scanBuilder;

  @BeforeEach
  void setUp() {
    scanBuilder = new OpenSearchPagedIndexScanBuilder(indexScan);
    when(table.createPagedScanBuilder(anyInt())).thenReturn(scanBuilder);
    when(indexScan.getRequestBuilder()).thenReturn(requestBuilder);

  }

  @Test
  public void push_down_project() {
    var relation = (LogicalRelation) relation("schema", table);
    relation.setPageSize(5);

    assertEquals(
        project(
            new OpenSearchPagedIndexScanBuilder(indexScan),
            DSL.named("i", DSL.ref("intV", INTEGER))),
        optimize(
            project(
                relation,
                DSL.named("i", DSL.ref("intV", INTEGER))
    )));
    scanBuilder.build();
    verify(requestBuilder).pushDownProjects(Set.of(DSL.ref("intV", INTEGER)));
  }

  @Test
  public void push_down_filter() {
    var relation = (LogicalRelation) relation("schema", table);
    relation.setPageSize(5);

    assertEquals(
        project(
            new OpenSearchPagedIndexScanBuilder(indexScan),
            DSL.named("i", DSL.ref("intV", INTEGER))),
        optimize(
            project(
                filter(
                    relation,
                    DSL.equal(DSL.ref("intV", INTEGER), DSL.literal(integerValue(1)))
                ),
                DSL.named("i", DSL.ref("intV", INTEGER))
    )));
    scanBuilder.build();
    verify(requestBuilder).pushDownFilter(QueryBuilders.termQuery("intV", 1));
  }

  @Test
  public void push_down_highlight() {
    var relation = (LogicalRelation) relation("schema", table);
    relation.setPageSize(5);

    assertEquals(
        project(
            new OpenSearchPagedIndexScanBuilder(indexScan),
            DSL.named("highlight(*)",
                new HighlightExpression(DSL.literal("*")))),
        optimize(
            project(
                highlight(
                    relation,
                    DSL.literal("*"), Map.of()),
                    DSL.named("highlight(*)",
                        new HighlightExpression(DSL.literal("*")))
                )
    ));
    scanBuilder.build();
    verify(requestBuilder).pushDownHighlight("*", Map.of());
  }

  private LogicalPlan optimize(LogicalPlan plan) {
    return new LogicalPlanOptimizer(List.of(
            new CreatePagingTableScanBuilder(),
            PUSH_DOWN_FILTER,
            PUSH_DOWN_HIGHLIGHT,
            PUSH_DOWN_PROJECT
        )).optimize(plan);
  }
}
