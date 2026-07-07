/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import static org.apache.calcite.test.Matchers.hasTree;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.opensearch.sql.executor.QueryType.PPL;

import java.util.List;
import java.util.function.UnaryOperator;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.sql.ast.Node;
import org.opensearch.sql.ast.statement.Query;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.CalcitePlanContext.NodeIdMapping;
import org.opensearch.sql.calcite.CalciteRelNodeVisitor;
import org.opensearch.sql.calcite.SysLimit;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.ppl.antlr.PPLSyntaxParser;
import org.opensearch.sql.ppl.parser.AstBuilder;
import org.opensearch.sql.ppl.parser.AstStatementBuilder;

public class CalcitePPLTrackingTest {

  private final Frameworks.ConfigBuilder config;
  private final CalciteRelNodeVisitor planTransformer;
  private final Settings settings;
  private final DataSourceService dataSourceService;
  private final PPLSyntaxParser pplParser = new PPLSyntaxParser();

  public CalcitePPLTrackingTest() {
    this.dataSourceService = mock(DataSourceService.class);
    this.planTransformer = new CalciteRelNodeVisitor(dataSourceService);
    this.settings = mock(Settings.class);
    this.config = Frameworks.newConfigBuilder()
        .defaultSchema(
            CalciteAssert.addSchema(
                Frameworks.createRootSchema(true), CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL))
        .programs();
  }

  @Before
  public void init() {
    doReturn(true).when(settings).getSettingValue(Settings.Key.CALCITE_ENGINE_ENABLED);
    doReturn(true).when(settings).getSettingValue(Settings.Key.CALCITE_SUPPORT_ALL_JOIN_TYPES);
    doReturn(true).when(settings).getSettingValue(Settings.Key.PPL_SYNTAX_LEGACY_PREFERRED);
    doReturn(-1).when(settings).getSettingValue(Settings.Key.PPL_JOIN_SUBSEARCH_MAXOUT);
    doReturn(-1).when(settings).getSettingValue(Settings.Key.PPL_SUBSEARCH_MAXOUT);
    doReturn(false).when(dataSourceService).dataSourceExists(any());
  }

  private CalcitePlanContext createContext() {
    config.context(Contexts.of(RelBuilder.Config.DEFAULT));
    return CalcitePlanContext.create(config.build(), SysLimit.fromSettings(settings), PPL);
  }

  private Node plan(String query) {
    final AstStatementBuilder builder =
        new AstStatementBuilder(
            new AstBuilder(query, settings),
            AstStatementBuilder.StatementBuilderContext.builder().build());
    return builder.visit(pplParser.parse(query));
  }

  private RelNode getRelNode(String ppl, CalcitePlanContext context) {
    Query query = (Query) plan(ppl);
    planTransformer.analyze(query.getPlan(), context);
    return context.relBuilder.build();
  }

  @Test
  public void testTrackingProducesSameLogicalPlanAsNonTracking() {
    String ppl = "source=EMP | eval a = 1 | fields EMPNO, a";

    CalcitePlanContext withoutTracking = createContext();
    RelNode expected = getRelNode(ppl, withoutTracking);

    CalcitePlanContext withTracking = createContext();
    withTracking.setTrackingEnabled(true);
    RelNode actual = getRelNode(ppl, withTracking);

    assertThat(actual, hasTree(expected.explain()));
  }

  @Test
  public void testTrackingDisabledProducesNoMappings() {
    String ppl = "source=EMP | eval a = 1";
    CalcitePlanContext context = createContext();
    context.setTrackingEnabled(false);
    getRelNode(ppl, context);

    assertTrue(context.getNodeIdMappings().isEmpty());
  }

  @Test
  public void testTrackingEvalRecordsMappings() {
    String ppl = "source=EMP | eval a = 1";
    CalcitePlanContext context = createContext();
    context.setTrackingEnabled(true);
    getRelNode(ppl, context);

    List<NodeIdMapping> mappings = context.getNodeIdMappings();
    assertFalse(mappings.isEmpty());

    List<String> astTypes = mappings.stream().map(NodeIdMapping::astNodeType).toList();
    assertTrue("Should contain Relation mapping", astTypes.contains("Relation"));
    assertTrue("Should contain Eval mapping", astTypes.contains("Eval"));
  }

  @Test
  public void testTrackingFilterRecordsMappings() {
    String ppl = "source=EMP | where SAL > 1000";
    CalcitePlanContext context = createContext();
    context.setTrackingEnabled(true);
    getRelNode(ppl, context);

    List<NodeIdMapping> mappings = context.getNodeIdMappings();
    List<String> astTypes = mappings.stream().map(NodeIdMapping::astNodeType).toList();
    assertTrue("Should contain Relation mapping", astTypes.contains("Relation"));
    assertTrue("Should contain Filter mapping", astTypes.contains("Filter"));
  }

  @Test
  public void testTrackingSortRecordsMappings() {
    String ppl = "source=EMP | sort SAL";
    CalcitePlanContext context = createContext();
    context.setTrackingEnabled(true);
    getRelNode(ppl, context);

    List<NodeIdMapping> mappings = context.getNodeIdMappings();
    List<String> astTypes = mappings.stream().map(NodeIdMapping::astNodeType).toList();
    assertTrue("Should contain Relation mapping", astTypes.contains("Relation"));
    assertTrue("Should contain Sort mapping", astTypes.contains("Sort"));
  }

  @Test
  public void testTrackingMultipleCommandsRecordsMappings() {
    String ppl = "source=EMP | where SAL > 1000 | eval bonus = SAL * 0.1 | sort SAL";
    CalcitePlanContext context = createContext();
    context.setTrackingEnabled(true);
    getRelNode(ppl, context);

    List<NodeIdMapping> mappings = context.getNodeIdMappings();
    List<String> astTypes = mappings.stream().map(NodeIdMapping::astNodeType).toList();
    assertTrue("Should contain Relation mapping", astTypes.contains("Relation"));
    assertTrue("Should contain Filter mapping", astTypes.contains("Filter"));
    assertTrue("Should contain Eval mapping", astTypes.contains("Eval"));
    assertTrue("Should contain Sort mapping", astTypes.contains("Sort"));
  }

  @Test
  public void testTrackingMappingsHaveNonEmptyRelNodeIds() {
    String ppl = "source=EMP | eval a = 1";
    CalcitePlanContext context = createContext();
    context.setTrackingEnabled(true);
    getRelNode(ppl, context);

    for (NodeIdMapping mapping : context.getNodeIdMappings()) {
      assertFalse(
          "Mapping for " + mapping.astNodeType() + " should have non-empty RelNode IDs",
          mapping.relNodeIds().isEmpty());
    }
  }

  @Test
  public void testTrackingProjectRecordsMappings() {
    String ppl = "source=EMP | fields EMPNO, ENAME";
    CalcitePlanContext context = createContext();
    context.setTrackingEnabled(true);
    getRelNode(ppl, context);

    List<NodeIdMapping> mappings = context.getNodeIdMappings();
    List<String> astTypes = mappings.stream().map(NodeIdMapping::astNodeType).toList();
    assertTrue("Should contain Relation mapping", astTypes.contains("Relation"));
    assertTrue("Should contain Project mapping", astTypes.contains("Project"));
  }

  @Test
  public void testTrackingAggregationRecordsMappings() {
    String ppl = "source=EMP | stats count() by DEPTNO";
    CalcitePlanContext context = createContext();
    context.setTrackingEnabled(true);
    getRelNode(ppl, context);

    List<NodeIdMapping> mappings = context.getNodeIdMappings();
    List<String> astTypes = mappings.stream().map(NodeIdMapping::astNodeType).toList();
    assertTrue("Should contain Relation mapping", astTypes.contains("Relation"));
    assertTrue("Should contain Aggregation mapping", astTypes.contains("Aggregation"));
  }

  @Test
  public void testTrackingMultipleCommandsProducesSameLogicalPlan() {
    String ppl = "source=EMP | where SAL > 1000 | eval bonus = SAL * 0.1 | sort SAL | head 10";

    CalcitePlanContext withoutTracking = createContext();
    RelNode expected = getRelNode(ppl, withoutTracking);

    CalcitePlanContext withTracking = createContext();
    withTracking.setTrackingEnabled(true);
    RelNode actual = getRelNode(ppl, withTracking);

    assertThat(actual, hasTree(expected.explain()));
  }

  @Test
  public void testVisitChildrenCapturesSubtreeContribution() {
    // visitChildren records a child's SUBTREE contribution (all RelNodes produced
    // by that child and its descendants). For a multi-command pipeline, the mapping
    // for Filter should include RelNodes from its own subtree (Relation + Filter itself).
    String ppl = "source=EMP | where SAL > 1000 | eval bonus = SAL * 0.1";
    CalcitePlanContext context = createContext();
    context.setTrackingEnabled(true);
    getRelNode(ppl, context);

    List<NodeIdMapping> mappings = context.getNodeIdMappings();

    // Relation is a leaf — should produce exactly 1 RelNode (the scan)
    NodeIdMapping relationMapping =
        mappings.stream()
            .filter(m -> m.astNodeType().equals("Relation"))
            .findFirst()
            .orElseThrow();
    assertFalse(
        "Relation (leaf) should produce at least one RelNode",
        relationMapping.relNodeIds().isEmpty());

    // Filter's subtree includes Relation beneath it, so visitChildren should
    // capture more RelNode IDs for Filter than for Relation alone.
    NodeIdMapping filterMapping =
        mappings.stream()
            .filter(m -> m.astNodeType().equals("Filter"))
            .findFirst()
            .orElseThrow();
    assertTrue(
        "Filter subtree should produce more RelNodes than Relation alone",
        filterMapping.relNodeIds().size() > relationMapping.relNodeIds().size());
  }

  @Test
  public void testVisitChildrenRecordsAllChildrenSeparately() {
    // visitChildren iterates over node.getChild() and records each one.
    // For a pipeline with multiple commands, each command gets its own mapping entry.
    String ppl = "source=EMP | where SAL > 1000 | sort SAL | head 5";
    CalcitePlanContext context = createContext();
    context.setTrackingEnabled(true);
    getRelNode(ppl, context);

    List<NodeIdMapping> mappings = context.getNodeIdMappings();
    List<String> astTypes = mappings.stream().map(NodeIdMapping::astNodeType).toList();

    // Each command in the pipeline should have a separate mapping entry
    assertTrue("Should record Relation", astTypes.contains("Relation"));
    assertTrue("Should record Filter", astTypes.contains("Filter"));
    assertTrue("Should record Sort", astTypes.contains("Sort"));
    assertTrue("Should record Head", astTypes.contains("Head"));
  }
}
