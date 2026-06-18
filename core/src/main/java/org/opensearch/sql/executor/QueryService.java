/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Programs;
import org.opensearch.sql.analysis.AnalysisContext;
import org.opensearch.sql.analysis.Analyzer;
import org.opensearch.sql.ast.statement.ExplainMode;
import org.opensearch.sql.ast.tree.HighlightConfig;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.CalciteRelNodeVisitor;
import org.opensearch.sql.calcite.OpenSearchSchema;
import org.opensearch.sql.calcite.SysLimit;
import org.opensearch.sql.calcite.plan.rel.LogicalSystemLimit;
import org.opensearch.sql.calcite.plan.rel.LogicalSystemLimit.SystemLimitType;
import org.opensearch.sql.calcite.utils.CalciteClassLoaderHelper;
import org.opensearch.sql.calcite.utils.CalciteToolsHelper.OpenSearchRelRunners;
import org.opensearch.sql.common.error.ErrorReport;
import org.opensearch.sql.common.error.QueryProcessingStage;
import org.opensearch.sql.common.error.StageErrorHandler;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.common.utils.QueryContext;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.exception.CalciteUnsupportedException;
import org.opensearch.sql.exception.NonFallbackCalciteException;
import org.opensearch.sql.monitor.profile.MetricName;
import org.opensearch.sql.monitor.profile.ProfileContext;
import org.opensearch.sql.monitor.profile.ProfileMetric;
import org.opensearch.sql.monitor.profile.QueryProfile;
import org.opensearch.sql.monitor.profile.QueryProfiling;
import org.opensearch.sql.planner.PlanContext;
import org.opensearch.sql.planner.Planner;
import org.opensearch.sql.planner.logical.LogicalPaginate;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlan;

/** The low level interface of core engine. */
@RequiredArgsConstructor
@AllArgsConstructor
@Log4j2
public class QueryService {
  private final Analyzer analyzer;
  private final ExecutionEngine executionEngine;
  private final Planner planner;
  private DataSourceService dataSourceService;
  private Settings settings;

  @Getter(lazy = true)
  private final CalciteRelNodeVisitor relNodeVisitor = new CalciteRelNodeVisitor(dataSourceService);

  /** Helper: depending on the type of error, either re-raise or propagate to the listener. */
  private void propagateCalciteError(Throwable t, ResponseListener<?> listener)
      throws VirtualMachineError {
    if (t instanceof VirtualMachineError) {
      // throw and fast fail the VM errors such as OOM (same with v2).
      throw (VirtualMachineError) t;
    }
    if (t instanceof Exception) {
      listener.onFailure((Exception) t);
    } else if (t instanceof ExceptionInInitializerError
        && ((ExceptionInInitializerError) t).getException() instanceof Exception) {
      listener.onFailure((Exception) ((ExceptionInInitializerError) t).getException());
    } else {
      // Calcite may throw AssertError during query execution.
      listener.onFailure(new CalciteUnsupportedException(t.getMessage(), t));
    }
  }

  /** Execute the {@link UnresolvedPlan}, using {@link ResponseListener} to get response.<br> */
  public void execute(
      UnresolvedPlan plan,
      QueryType queryType,
      ResponseListener<ExecutionEngine.QueryResponse> listener) {
    execute(plan, queryType, null, listener);
  }

  /** Execute with optional highlight config. */
  public void execute(
      UnresolvedPlan plan,
      QueryType queryType,
      HighlightConfig highlightConfig,
      ResponseListener<ExecutionEngine.QueryResponse> listener) {
    if (shouldUseCalcite(queryType)) {
      executeWithCalcite(plan, queryType, highlightConfig, listener);
    } else {
      executeWithLegacy(plan, queryType, listener, Optional.empty());
    }
  }

  /** Explain the {@link UnresolvedPlan}, using {@link ResponseListener} to get response.<br> */
  public void explain(
      UnresolvedPlan plan,
      QueryType queryType,
      ResponseListener<ExecutionEngine.ExplainResponse> listener,
      ExplainMode mode) {
    explain(plan, queryType, null, listener, mode);
  }

  /** Explain with optional highlight config. */
  public void explain(
      UnresolvedPlan plan,
      QueryType queryType,
      HighlightConfig highlightConfig,
      ResponseListener<ExecutionEngine.ExplainResponse> listener,
      ExplainMode mode) {
    if (shouldUseCalcite(queryType)) {
      explainWithCalcite(plan, queryType, highlightConfig, listener, mode);
    } else {
      explainWithLegacy(plan, queryType, listener, mode, Optional.empty());
    }
  }

  public void executeWithCalcite(
      UnresolvedPlan plan,
      QueryType queryType,
      HighlightConfig highlightConfig,
      ResponseListener<ExecutionEngine.QueryResponse> listener) {
    CalcitePlanContext.run(
        () -> {
          try {
            ProfileContext profileContext =
                QueryProfiling.activate(QueryContext.isProfileEnabled());
            ProfileMetric analyzeMetric = profileContext.getOrCreateMetric(MetricName.ANALYZE);
            long analyzeStart = System.nanoTime();
            CalciteClassLoaderHelper.withCalciteClassLoader(
                () -> {
                  CalcitePlanContext context =
                      CalcitePlanContext.create(
                          buildFrameworkConfig(), SysLimit.fromSettings(settings), queryType);

                  context.setHighlightConfig(highlightConfig);

                  // Wrap analyze with ANALYZING stage tracking
                  RelNode relNode =
                      StageErrorHandler.executeStage(
                          QueryProcessingStage.ANALYZING,
                          () -> analyze(plan, context),
                          "while preparing and validating the query plan");

                  // Wrap plan conversion with PLAN_CONVERSION stage tracking
                  RelNode calcitePlan =
                      StageErrorHandler.executeStage(
                          QueryProcessingStage.PLAN_CONVERSION,
                          () -> convertToCalcitePlan(relNode, context),
                          "while converting the query to an executable plan");

                  analyzeMetric.set(System.nanoTime() - analyzeStart);

                  // Wrap execution with EXECUTING stage tracking
                  StageErrorHandler.executeStageVoid(
                      QueryProcessingStage.EXECUTING,
                      () -> executionEngine.execute(calcitePlan, context, listener),
                      "while running the query");
                },
                QueryService.class);
          } catch (Throwable t) {
            if (isCalciteFallbackAllowed(t) && !(t instanceof NonFallbackCalciteException)) {
              log.warn("Fallback to V2 query engine since got exception", t);
              executeWithLegacy(plan, queryType, listener, Optional.of(t));
            } else {
              propagateCalciteError(t, listener);
            }
          }
        },
        settings);
  }

  public void explainWithCalcite(
      UnresolvedPlan plan,
      QueryType queryType,
      HighlightConfig highlightConfig,
      ResponseListener<ExecutionEngine.ExplainResponse> listener,
      ExplainMode mode) {
    CalcitePlanContext.run(
        () -> {
          try {
            QueryProfiling.noop();
            CalciteClassLoaderHelper.withCalciteClassLoader(
                () -> {
                  CalcitePlanContext context =
                      CalcitePlanContext.create(
                          buildFrameworkConfig(), SysLimit.fromSettings(settings), queryType);
                  context.setHighlightConfig(highlightConfig);
                  context.run(
                      () -> {
                        RelNode relNode = analyze(plan, context);
                        RelNode calcitePlan = convertToCalcitePlan(relNode, context);
                        executionEngine.explain(calcitePlan, mode, context, listener);
                      },
                      settings);
                },
                QueryService.class);
          } catch (Throwable t) {
            if (isCalciteFallbackAllowed(t)) {
              log.warn("Fallback to V2 query engine since got exception", t);
              explainWithLegacy(plan, queryType, listener, mode, Optional.of(t));
            } else {
              propagateCalciteError(t, listener);
            }
          }
        },
        settings);
  }

  public void analyzeWithCalcite(
      String query,
      List<AnalyzeResponse.QuerySegment> querySegments,
      UnresolvedPlan plan,
      QueryType queryType,
      ResponseListener<AnalyzeResponse> listener) {
    // Phase 1: Execute via the exact same path as executeWithCalcite + executionEngine.execute
    // to get identical profile timings. Use a latch to synchronize the async callback.
    // Force profiling on so executeWithCalcite activates QueryProfiling.
    QueryContext.setProfile(true);
    AtomicReference<ExecutionEngine.QueryResponse> queryResponseRef = new AtomicReference<>();
    AtomicReference<QueryProfile> profileRef = new AtomicReference<>();
    AtomicReference<Exception> errorRef = new AtomicReference<>();
    CountDownLatch latch = new CountDownLatch(1);

    executeWithCalcite(
        plan,
        queryType,
        null,
        new ResponseListener<>() {
          @Override
          public void onResponse(ExecutionEngine.QueryResponse response) {
            ProfileMetric formatMetric =
                QueryProfiling.current().getOrCreateMetric(MetricName.FORMAT);
            long formatStart = System.nanoTime();
            int resultSize = response.getResults().size();
            for (var exprValue : response.getResults()) {
              exprValue.tupleValue().entrySet().stream()
                  .map(e -> e.getValue().value())
                  .toArray(Object[]::new);
            }
            formatMetric.set(System.nanoTime() - formatStart);
            profileRef.set(QueryProfiling.current().finish());
            queryResponseRef.set(response);
            latch.countDown();
          }

          @Override
          public void onFailure(Exception e) {
            errorRef.set(e);
            latch.countDown();
          }
        });

    try {
      latch.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      listener.onFailure(new RuntimeException("Interrupted while waiting for query execution", e));
      return;
    }

    if (errorRef.get() != null) {
      listener.onFailure(errorRef.get());
      return;
    }

    ExecutionEngine.QueryResponse queryResponse = queryResponseRef.get();
    QueryProfile profile = profileRef.get();

    // Phase 2: Re-run with tracking to capture logical/physical plans and node mappings.
    // This run benefits from warm caches but we don't report its timings.
    CalcitePlanContext.run(
        () -> {
          try {
            QueryProfiling.noop();
            CalciteClassLoaderHelper.withCalciteClassLoader(
                () -> {
                  CalcitePlanContext context =
                      CalcitePlanContext.create(
                          buildFrameworkConfig(), SysLimit.fromSettings(settings), queryType);
                  context.setTrackingEnabled(true);
                  RelNode relNode = analyze(plan, context);
                  RelNode calcitePlan = convertToCalcitePlan(relNode, context);

                  AtomicReference<String> physicalPlanRef = new AtomicReference<>();
                  AtomicReference<RelNode> physicalRelRef = new AtomicReference<>();
                  try (Hook.Closeable closeable =
                      Hook.PLAN_BEFORE_IMPLEMENTATION.addThread(
                          obj -> {
                            RelRoot relRoot = (RelRoot) obj;
                            physicalRelRef.set(relRoot.rel);
                            physicalPlanRef.set(
                                RelOptUtil.toString(relRoot.rel, SqlExplainLevel.ALL_ATTRIBUTES));
                          })) {
                    try (java.sql.PreparedStatement ignored =
                        OpenSearchRelRunners.run(context, calcitePlan)) {
                    } catch (java.sql.SQLException e) {
                      throw new RuntimeException(e);
                    }
                  }

                  String logicalPlanStr =
                      RelOptUtil.toString(calcitePlan, SqlExplainLevel.ALL_ATTRIBUTES);
                  List<String> logicalPlanNodes =
                      java.util.Arrays.stream(logicalPlanStr.split("\n"))
                          .map(String::trim)
                          .filter(s -> !s.isEmpty())
                          .toList();
                  List<String> physicalPlanNodes =
                      java.util.Arrays.stream(physicalPlanRef.get().split("\n"))
                          .map(String::trim)
                          .filter(s -> !s.isEmpty())
                          .toList();

                  // Build operator tree using phase 2's tracking data + phase 1's profile.
                  List<AnalyzeResponse.OperatorNode> operatorTree =
                      buildOperatorTree(
                          querySegments,
                          logicalPlanNodes,
                          context.getNodeIdMappings(),
                          calcitePlan,
                          physicalRelRef.get(),
                          profile);

                  // Convert QueryResponse results to analyze format.
                  List<AnalyzeResponse.SchemaColumn> schema = new ArrayList<>();
                  if (queryResponse.getSchema() != null) {
                    for (ExecutionEngine.Schema.Column col :
                        queryResponse.getSchema().getColumns()) {
                      schema.add(
                          AnalyzeResponse.SchemaColumn.builder()
                              .name(col.getName())
                              .type(col.getExprType().typeName())
                              .build());
                    }
                  }

                  List<Map<String, Object>> datarows = new ArrayList<>();
                  for (var exprValue : queryResponse.getResults()) {
                    Map<String, Object> row = new java.util.LinkedHashMap<>();
                    exprValue.tupleValue().forEach((k, v) -> row.put(k, v.value()));
                    datarows.add(row);
                  }

                  AnalyzeResponse response =
                      AnalyzeResponse.builder()
                          .query(query)
                          .logicalPlan(logicalPlanNodes)
                          .physicalPlan(physicalPlanNodes)
                          .operator_tree(operatorTree)
                          .recommendations(List.of())
                          .profile(profile)
                          .schema(schema)
                          .datarows(datarows)
                          .total(datarows.size())
                          .size(datarows.size())
                          .build();
                  listener.onResponse(response);
                },
                QueryService.class);
          } catch (Throwable t) {
            if (t instanceof Exception) {
              listener.onFailure((Exception) t);
            } else {
              listener.onFailure(new RuntimeException(t));
            }
          }
        },
        settings);
  }

  private List<AnalyzeResponse.OperatorNode> buildOperatorTree(
      List<AnalyzeResponse.QuerySegment> querySegments,
      List<String> logicalPlanNodes,
      List<CalcitePlanContext.NodeIdMapping> nodeIdMappings,
      RelNode logicalPlan,
      RelNode physicalPlan,
      QueryProfile profile) {
    // Build a map from RelNode id to its logical plan description string.
    Map<Integer, String> idToDescription = new HashMap<>();
    for (String node : logicalPlanNodes) {
      int idIdx = node.lastIndexOf("id = ");
      if (idIdx >= 0) {
        String idStr = node.substring(idIdx + 5).trim();
        try {
          int id = Integer.parseInt(idStr);
          idToDescription.put(id, node);
        } catch (NumberFormatException ignored) {
        }
      }
    }

    // Compute exclusive ids per mapping by subtracting the previous mapping's ids.
    // Mappings are recorded bottom-up: [Relation:[0], Filter:[0,1], Project:[0,1,2]]
    // Exclusive: Relation=[0], Filter=[1], Project=[2]
    List<Set<Integer>> exclusiveIds = new ArrayList<>();
    Set<Integer> previousIds = new HashSet<>();
    for (CalcitePlanContext.NodeIdMapping mapping : nodeIdMappings) {
      Set<Integer> current = new HashSet<>(mapping.relNodeIds());
      Set<Integer> exclusive = new HashSet<>(current);
      exclusive.removeAll(previousIds);
      exclusiveIds.add(exclusive);
      previousIds = current;
    }

    // Determine how many segments from the bottom were pushed into the physical scan.
    // The physical plan's leaf node (the scan) absorbs logical nodes from the bottom up.
    // Physical depth tells us how many separate physical operators exist; everything else
    // was pushed down. We count segments bottom-up until we've covered all pushed logical nodes.
    int physicalDepth = getLinearDepth(physicalPlan);
    int logicalDepth = getLinearDepth(logicalPlan);
    int pushedNodeCount = logicalDepth - physicalDepth;

    // log.info(
    //     "buildOperatorTree: logicalDepth={}, physicalDepth={}, pushedNodeCount={},"
    //         + " segments={}, exclusiveIds={}",
    //     logicalDepth,
    //     physicalDepth,
    //     pushedNodeCount,
    //     querySegments.size(),
    //     exclusiveIds);

    // Walk segments bottom-up (they're already in bottom-up order) and greedily assign
    // them to the pushed group until we've accounted for all pushed logical nodes.
    // The LogicalSystemLimit added by convertToCalcitePlan counts toward the logical depth
    // but has no segment, so we only count nodes that appear in exclusiveIds.
    int pushedLogicalNodes = 0;
    int pushedSegments = 0;
    for (int idx = 0; idx < querySegments.size() && pushedLogicalNodes < pushedNodeCount; idx++) {
      Set<Integer> ids = idx < exclusiveIds.size() ? exclusiveIds.get(idx) : Set.of();
      long planNodeCount = ids.stream().filter(idToDescription::containsKey).count();
      pushedLogicalNodes += planNodeCount;
      pushedSegments++;
    }

    // log.info(
    //     "buildOperatorTree: pushedSegments={}, pushedLogicalNodes={}",
    //     pushedSegments,
    //     pushedLogicalNodes);

    // Compute estimated row counts from the logical plan using RelMetadataQuery.
    // Walk the logical plan bottom-up to get rowcount per node by id.
    org.apache.calcite.rel.metadata.RelMetadataQuery mq =
        logicalPlan.getCluster().getMetadataQuery();
    Map<Integer, Double> idToRowCount = new HashMap<>();
    collectRowCounts(logicalPlan, mq, idToRowCount);

    // Compute exclusive time and rows per physical node from the profile plan tree.
    // The plan tree is top-down; we flatten it bottom-up to match operator tree order.
    List<double[]> physicalTimings = new ArrayList<>();
    if (profile != null && profile.getPlan() != null) {
      List<QueryProfile.PlanNode> planNodes = new ArrayList<>();
      QueryProfile.PlanNode current = profile.getPlan();
      while (current != null) {
        planNodes.add(current);
        current =
            (current.getChildren() != null && !current.getChildren().isEmpty())
                ? current.getChildren().get(0)
                : null;
      }
      // planNodes is top-down; reverse to bottom-up
      java.util.Collections.reverse(planNodes);
      for (int p = 0; p < planNodes.size(); p++) {
        double inclusive = planNodes.get(p).getTimeMillis();
        double childInclusive = (p > 0) ? planNodes.get(p - 1).getTimeMillis() : 0;
        double exclusive = Math.max(0, inclusive - childInclusive);
        long rows = planNodes.get(p).getRows();
        physicalTimings.add(new double[] {exclusive, rows});
      }
    }

    List<AnalyzeResponse.OperatorNode> operators = new ArrayList<>();
    int physicalIdx = 0;

    // Build the pushed-down merged entry (first pushedSegments segments)
    if (pushedSegments > 1) {
      List<AnalyzeResponse.QuerySegment> mergedSegments = querySegments.subList(0, pushedSegments);
      List<String> descriptions = new ArrayList<>();
      for (int idx = 0; idx < pushedSegments; idx++) {
        Set<Integer> ids = idx < exclusiveIds.size() ? exclusiveIds.get(idx) : Set.of();
        ids.stream()
            .sorted()
            .map(idToDescription::get)
            .filter(Objects::nonNull)
            .forEach(descriptions::add);
      }
      String combinedSource =
          mergedSegments.stream()
              .map(AnalyzeResponse.QuerySegment::getSource)
              .reduce((a, b) -> a + " | " + b)
              .orElse("");
      List<String> nodeTypes =
          mergedSegments.stream().map(AnalyzeResponse.QuerySegment::getNodeType).toList();
      // Collect all plan node ids in the pushed group for estimated_rows
      Set<Integer> allPushedPlanIds = new HashSet<>();
      for (int i = 0; i < pushedSegments; i++) {
        Set<Integer> ids = i < exclusiveIds.size() ? exclusiveIds.get(i) : Set.of();
        ids.stream().filter(idToDescription::containsKey).forEach(allPushedPlanIds::add);
      }
      double[] timing =
          physicalIdx < physicalTimings.size() ? physicalTimings.get(physicalIdx) : null;
      physicalIdx++;
      operators.add(
          AnalyzeResponse.OperatorNode.builder()
              .source(combinedSource)
              .node_type(nodeTypes)
              .description(descriptions.isEmpty() ? null : descriptions)
              .is_pushed_down(true)
              .estimated_rows(getEstimatedRows(allPushedPlanIds, idToRowCount))
              .actual_time_ms(timing != null ? String.format("%.2f ms", timing[0]) : null)
              .actual_rows(timing != null ? (long) timing[1] : null)
              .build());
    } else if (pushedSegments == 1) {
      AnalyzeResponse.QuerySegment seg = querySegments.get(0);
      Set<Integer> ids = !exclusiveIds.isEmpty() ? exclusiveIds.get(0) : Set.of();
      Set<Integer> planIds =
          ids.stream()
              .filter(idToDescription::containsKey)
              .collect(java.util.stream.Collectors.toSet());
      List<String> descriptions =
          ids.stream().sorted().map(idToDescription::get).filter(Objects::nonNull).toList();
      double[] timing =
          physicalIdx < physicalTimings.size() ? physicalTimings.get(physicalIdx) : null;
      physicalIdx++;
      operators.add(
          AnalyzeResponse.OperatorNode.builder()
              .source(seg.getSource())
              .node_type(List.of(seg.getNodeType()))
              .description(descriptions.isEmpty() ? null : descriptions)
              .estimated_rows(getEstimatedRows(planIds, idToRowCount))
              .actual_time_ms(timing != null ? String.format("%.2f ms", timing[0]) : null)
              .actual_rows(timing != null ? (long) timing[1] : null)
              .build());
    }

    // Remaining segments map to non-scan physical nodes (physicalDepth - 1 of them).
    // Each physical node corresponds to one logical plan node. Group segments so that each
    // group covers exactly one logical plan node; segments with 0 plan nodes merge into the
    // next group that has one.
    int idx = pushedSegments;
    while (idx < querySegments.size()) {
      List<AnalyzeResponse.QuerySegment> group = new ArrayList<>();
      List<String> descriptions = new ArrayList<>();
      Set<Integer> groupPlanIds = new HashSet<>();
      long logicalNodesInGroup = 0;
      while (idx < querySegments.size() && logicalNodesInGroup < 1) {
        group.add(querySegments.get(idx));
        Set<Integer> ids = idx < exclusiveIds.size() ? exclusiveIds.get(idx) : Set.of();
        ids.stream()
            .sorted()
            .map(idToDescription::get)
            .filter(Objects::nonNull)
            .forEach(descriptions::add);
        ids.stream().filter(idToDescription::containsKey).forEach(groupPlanIds::add);
        logicalNodesInGroup += ids.stream().filter(idToDescription::containsKey).count();
        idx++;
      }
      String combinedSource =
          group.stream()
              .map(AnalyzeResponse.QuerySegment::getSource)
              .reduce((a, b) -> a + " | " + b)
              .orElse("");
      List<String> nodeTypes =
          group.stream().map(AnalyzeResponse.QuerySegment::getNodeType).toList();
      double[] timing =
          physicalIdx < physicalTimings.size() ? physicalTimings.get(physicalIdx) : null;
      physicalIdx++;
      operators.add(
          AnalyzeResponse.OperatorNode.builder()
              .source(combinedSource)
              .node_type(nodeTypes)
              .description(descriptions.isEmpty() ? null : descriptions)
              .estimated_rows(getEstimatedRows(groupPlanIds, idToRowCount))
              .actual_time_ms(timing != null ? String.format("%.2f ms", timing[0]) : null)
              .actual_rows(timing != null ? (long) timing[1] : null)
              .build());
    }

    return operators;
  }

  private static int getLinearDepth(RelNode node) {
    int depth = 0;
    RelNode current = node;
    while (current != null) {
      depth++;
      List<RelNode> inputs = current.getInputs();
      current = inputs.isEmpty() ? null : inputs.get(0);
    }
    return depth;
  }

  private void collectRowCounts(
      RelNode node,
      org.apache.calcite.rel.metadata.RelMetadataQuery mq,
      Map<Integer, Double> idToRowCount) {
    try {
      Double rowCount = mq.getRowCount(node);
      if (rowCount != null) {
        idToRowCount.put(node.getId(), rowCount);
      }
    } catch (Exception ignored) {
    }
    for (RelNode input : node.getInputs()) {
      collectRowCounts(input, mq, idToRowCount);
    }
  }

  private Long getEstimatedRows(Set<Integer> ids, Map<Integer, Double> idToRowCount) {
    return ids.stream()
        .filter(idToRowCount::containsKey)
        .max(Integer::compareTo)
        .map(id -> Math.round(idToRowCount.get(id)))
        .orElse(null);
  }

  public void executeWithLegacy(
      UnresolvedPlan plan,
      QueryType queryType,
      ResponseListener<ExecutionEngine.QueryResponse> listener,
      Optional<Throwable> calciteFailure) {
    try {
      executePlan(analyze(plan, queryType), PlanContext.emptyPlanContext(), listener);
    } catch (Exception e) {
      if (calciteFailure.isPresent()) {
        // This happens if Calcite fell back to V2 due to some issue, and then V2 also failed.
        // Prefer the Calcite error.
        // https://github.com/opensearch-project/sql/issues/5060
        propagateCalciteError(calciteFailure.get(), listener);
      } else {
        listener.onFailure(e);
      }
    }
  }

  /**
   * Explain the query in {@link UnresolvedPlan} using {@link ResponseListener} to get and format
   * explain response.
   *
   * @param plan {@link UnresolvedPlan}
   * @param queryType {@link QueryType}
   * @param listener {@link ResponseListener} for explain response
   * @param calciteFailure Optional failure thrown from calcite
   */
  public void explainWithLegacy(
      UnresolvedPlan plan,
      QueryType queryType,
      ResponseListener<ExecutionEngine.ExplainResponse> listener,
      ExplainMode mode,
      Optional<Throwable> calciteFailure) {
    try {
      if (mode != null && (mode != ExplainMode.STANDARD)) {
        throw new UnsupportedOperationException(
            "Explain mode " + mode.name() + " is not supported in v2 engine");
      }
      executionEngine.explain(plan(analyze(plan, queryType)), listener);
    } catch (Exception e) {
      if (calciteFailure.isPresent()) {
        // This happens if Calcite fell back to V2 due to some issue, and then V2 also failed.
        // Prefer the Calcite error.
        // https://github.com/opensearch-project/sql/issues/5060
        propagateCalciteError(calciteFailure.get(), listener);
      } else {
        listener.onFailure(e);
      }
    }
  }

  /**
   * Execute the {@link LogicalPlan}, with {@link PlanContext} and using {@link ResponseListener} to
   * get response.<br>
   * Todo. Pass split from PlanContext to ExecutionEngine in following PR.
   *
   * @param plan {@link LogicalPlan}
   * @param planContext {@link PlanContext}
   * @param listener {@link ResponseListener}
   */
  public void executePlan(
      LogicalPlan plan,
      PlanContext planContext,
      ResponseListener<ExecutionEngine.QueryResponse> listener) {
    try {
      planContext
          .getSplit()
          .ifPresentOrElse(
              split -> executionEngine.execute(plan(plan), new ExecutionContext(split), listener),
              () ->
                  executionEngine.execute(
                      plan(plan),
                      ExecutionContext.querySizeLimit(
                          // For pagination, querySizeLimit shouldn't take effect.
                          // See {@link PaginationWindowIT::testQuerySizeLimitDoesNotEffectPageSize}
                          plan instanceof LogicalPaginate
                              ? null
                              : SysLimit.fromSettings(settings).querySizeLimit()),
                      listener));
    } catch (Exception e) {
      listener.onFailure(e);
    }
  }

  public RelNode analyze(UnresolvedPlan plan, CalcitePlanContext context) {
    return getRelNodeVisitor().analyze(plan, context);
  }

  /** Analyze {@link UnresolvedPlan}. */
  public LogicalPlan analyze(UnresolvedPlan plan, QueryType queryType) {
    return analyzer.analyze(plan, new AnalysisContext(queryType));
  }

  /** Translate {@link LogicalPlan} to {@link PhysicalPlan}. */
  public PhysicalPlan plan(LogicalPlan plan) {
    return planner.plan(plan);
  }

  private boolean isCalciteUnsupportedError(@Nullable Throwable t) {
    return switch (t) {
      case null -> false;
      case CalciteUnsupportedException calciteUnsupportedException -> true;
      case ErrorReport errorReport when t.getCause() instanceof CalciteUnsupportedException -> true;
      default -> false;
    };
  }

  private boolean isCalciteFallbackAllowed(@Nullable Throwable t) {
    // We always allow fallback the query failed with CalciteUnsupportedException.
    // This is for avoiding breaking changes when enable Calcite by default.
    if (isCalciteUnsupportedError(t)) {
      return true;
    }

    if (settings != null) {
      Boolean fallback_allowed = settings.getSettingValue(Settings.Key.CALCITE_FALLBACK_ALLOWED);
      if (fallback_allowed == null) {
        return false;
      }
      return fallback_allowed;
    }

    return true;
  }

  private boolean isCalciteEnabled(Settings settings) {
    if (settings != null) {
      return settings.getSettingValue(Settings.Key.CALCITE_ENGINE_ENABLED);
    } else {
      return false;
    }
  }

  // TODO https://github.com/opensearch-project/sql/issues/3457
  // Calcite is not available for SQL query now. Maybe release in 3.1.0?
  private boolean shouldUseCalcite(QueryType queryType) {
    return isCalciteEnabled(settings) && queryType == QueryType.PPL;
  }

  private FrameworkConfig buildFrameworkConfig() {
    // Use simple calcite schema since we don't compute tables in advance of the query.
    final SchemaPlus rootSchema = CalciteSchema.createRootSchema(true, false).plus();
    final SchemaPlus opensearchSchema =
        rootSchema.add(
            OpenSearchSchema.OPEN_SEARCH_SCHEMA_NAME, new OpenSearchSchema(dataSourceService));
    Frameworks.ConfigBuilder configBuilder =
        Frameworks.newConfigBuilder()
            .parserConfig(SqlParser.Config.DEFAULT) // TODO check
            .defaultSchema(opensearchSchema)
            .traitDefs((List<RelTraitDef>) null)
            .programs(Programs.standard())
            .typeSystem(OpenSearchTypeSystem.INSTANCE);
    return configBuilder.build();
  }

  /**
   * Convert OpenSearch Plan to Calcite Plan. Although both plans consist of Calcite RelNodes, there
   * are some differences in the topological structures or semantics between them.
   *
   * @param osPlan Logical Plan derived from OpenSearch PPL
   * @param context Calcite context
   */
  private static RelNode convertToCalcitePlan(RelNode osPlan, CalcitePlanContext context) {
    // Explicitly add a limit operator to enforce query size limit
    RelNode calcitePlan =
        LogicalSystemLimit.create(
            SystemLimitType.QUERY_SIZE_LIMIT,
            osPlan,
            context.relBuilder.literal(context.sysLimit.querySizeLimit()));
    /* Calcite only ensures collation of the final result produced from the root sort operator.
     * While we expect that the collation can be preserved through the pipes over PPL, we need to
     * explicitly add a sort operator on top of the original plan
     * to ensure the correct collation of the final result.
     * See logic in ${@link CalcitePrepareImpl}
     * For the redundant sort, we rely on Calcite optimizer to eliminate
     */
    RelCollation collation = calcitePlan.getTraitSet().getCollation();
    if (!(calcitePlan instanceof Sort) && collation != RelCollations.EMPTY) {
      calcitePlan = LogicalSort.create(calcitePlan, collation, null, null);
    }
    return calcitePlan;
  }
}
