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
import org.opensearch.sql.ast.Node;
import org.opensearch.sql.ast.statement.ExplainMode;
import org.opensearch.sql.ast.tree.HighlightConfig;
import org.opensearch.sql.ast.tree.Relation;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.CalciteRelNodeVisitor;
import org.opensearch.sql.calcite.OpenSearchSchema;
import org.opensearch.sql.calcite.SysLimit;
import org.opensearch.sql.calcite.plan.rel.LogicalSystemLimit;
import org.opensearch.sql.calcite.plan.rel.LogicalSystemLimit.SystemLimitType;
import org.opensearch.sql.calcite.utils.CalciteClassLoaderHelper;
import org.opensearch.sql.calcite.utils.CalciteToolsHelper;
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
import org.opensearch.sql.protocol.response.format.Format;

/** The low level interface of core engine. */
@RequiredArgsConstructor
@Log4j2
public class QueryService {
  private final Analyzer analyzer;
  private final ExecutionEngine executionEngine;
  private final Planner planner;
  private DataSourceService dataSourceService;
  private Settings settings;
  private ExecutionDispatcher executionDispatcher = new DirectExecutionDispatcher();

  public QueryService(
      Analyzer analyzer,
      ExecutionEngine executionEngine,
      Planner planner,
      DataSourceService dataSourceService,
      Settings settings) {
    this(
        analyzer,
        executionEngine,
        planner,
        dataSourceService,
        settings,
        new DirectExecutionDispatcher());
  }

  public QueryService(
      Analyzer analyzer,
      ExecutionEngine executionEngine,
      Planner planner,
      DataSourceService dataSourceService,
      Settings settings,
      ExecutionDispatcher executionDispatcher) {
    this.analyzer = analyzer;
    this.executionEngine = executionEngine;
    this.planner = planner;
    this.dataSourceService = dataSourceService;
    this.settings = settings;
    this.executionDispatcher = executionDispatcher;
  }

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
    explain(plan, queryType, highlightConfig, listener, mode, null);
  }

  /** Explain with optional highlight config and format. */
  public void explain(
      UnresolvedPlan plan,
      QueryType queryType,
      HighlightConfig highlightConfig,
      ResponseListener<ExecutionEngine.ExplainResponse> listener,
      ExplainMode mode,
      Format format) {
    if (shouldUseCalcite(queryType)) {
      explainWithCalcite(plan, queryType, highlightConfig, listener, mode, format);
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

                  executeCalcitePlan(calcitePlan, context, listener, analyzeMetric, analyzeStart);
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

  private void executeCalcitePlan(
      RelNode calcitePlan,
      CalcitePlanContext context,
      ResponseListener<ExecutionEngine.QueryResponse> listener,
      ProfileMetric analyzeMetric,
      long analyzeStart) {
    try {
      // Optimize before dispatch so the dispatcher's ScriptDetector
      // sees the post-optimization plan for accurate routing.
      RelNode optimizedPlan = CalciteToolsHelper.optimize(calcitePlan, context);
      analyzeMetric.set(System.nanoTime() - analyzeStart);

      // Wrap execution with EXECUTING stage tracking — dispatch via
      // ExecutionDispatcher which may route to a complex worker pool
      StageErrorHandler.executeStageVoid(
          QueryProcessingStage.EXECUTING,
          () -> executionDispatcher.dispatch(optimizedPlan, context, listener, executionEngine),
          "while running the query");
    } catch (RuntimeException e) {
      ArithmeticException overflow = findArithmeticOverflow(e);
      if (overflow != null) {
        throw new NonFallbackCalciteException(
            "Arithmetic overflow: " + overflow.getMessage(), overflow);
      }
      throw e;
    }
  }

  public void explainWithCalcite(
      UnresolvedPlan plan,
      QueryType queryType,
      HighlightConfig highlightConfig,
      ResponseListener<ExecutionEngine.ExplainResponse> listener,
      ExplainMode mode) {
    explainWithCalcite(plan, queryType, highlightConfig, listener, mode, null);
  }

  public void explainWithCalcite(
      UnresolvedPlan plan,
      QueryType queryType,
      HighlightConfig highlightConfig,
      ResponseListener<ExecutionEngine.ExplainResponse> listener,
      ExplainMode mode,
      Format format) {
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
                        if (format != null) {
                          executionEngine.explain(calcitePlan, mode, format, context, listener);
                        } else {
                          executionEngine.explain(calcitePlan, mode, context, listener);
                        }
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
      QueryType queryType, // boolean disableCache,
      ResponseListener<AnalyzeResponse> listener) {
    if (!shouldUseCalcite(queryType)) {
      listener.onFailure(
          new UnsupportedOperationException(
              "Analyze requires the Calcite engine to be enabled"
                  + " (plugins.calcite.enabled=true) and a PPL query type"));
      return;
    }
    boolean disableCache = true;
    // Phase 1: Execute via the exact same path as executeWithCalcite + executionEngine.execute
    // to get identical profile timings. Use a latch to synchronize the async callback.
    // Force profiling on so executeWithCalcite activates QueryProfiling.
    QueryContext.setProfile(true);

    String[] indexNames = extractIndexNames(plan);
    long cacheHitsBefore = disableCache ? -1 : executionEngine.getRequestCacheHitCount(indexNames);

    if (disableCache) {
      CalcitePlanContext.disableRequestCache.set(true);
    }

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
      CalcitePlanContext.disableRequestCache.remove();
      listener.onFailure(new RuntimeException("Interrupted while waiting for query execution", e));
      return;
    } finally {
      CalcitePlanContext.disableRequestCache.remove();
    }

    if (errorRef.get() != null) {
      listener.onFailure(errorRef.get());
      return;
    }

    long cacheHitsAfter = disableCache ? -1 : executionEngine.getRequestCacheHitCount(indexNames);
    boolean possibleCacheHit =
        !disableCache
            && cacheHitsBefore >= 0
            && cacheHitsAfter >= 0
            && cacheHitsAfter > cacheHitsBefore;

    ExecutionEngine.QueryResponse queryResponse = queryResponseRef.get();
    QueryProfile profile = profileRef.get();

    // If the profile plan tree has branching (any node with >1 child), our linear
    // operator tree logic won't work. Return a response that 'fallsback' on `profile`
    // by only including fields mirroring the `profile` endpoint.
    if (profile != null && profile.getPlan() != null && !isLinearPlanTree(profile)) {
      List<AnalyzeResponse.SchemaColumn> schema = new ArrayList<>();
      if (queryResponse.getSchema() != null) {
        for (ExecutionEngine.Schema.Column col : queryResponse.getSchema().getColumns()) {
          schema.add(
              AnalyzeResponse.SchemaColumn.builder()
                  .name(col.getName())
                  .type(col.getExprType().typeName())
                  .build());
        }
      }
      Object[][] datarows = new Object[queryResponse.getResults().size()][];
      int rowIdx = 0;
      for (var exprValue : queryResponse.getResults()) {
        datarows[rowIdx++] =
            exprValue.tupleValue().entrySet().stream()
                .map(e -> e.getValue().value())
                .toArray(Object[]::new);
      }
      listener.onResponse(
          AnalyzeResponse.builder()
              .query(query)
              .profile(profile)
              .possibleCacheHit(possibleCacheHit)
              .schema(schema)
              .datarows(datarows)
              .total(datarows.length)
              .size(datarows.length)
              .build());
      return;
    }

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

                  Object[][] datarows = new Object[queryResponse.getResults().size()][];
                  int rowIdx = 0;
                  for (var exprValue : queryResponse.getResults()) {
                    datarows[rowIdx++] =
                        exprValue.tupleValue().entrySet().stream()
                            .map(e -> e.getValue().value())
                            .toArray(Object[]::new);
                  }

                  // Extract scan metadata for recommendations #2 and #3.
                  org.apache.calcite.rel.metadata.RelMetadataQuery mq =
                      calcitePlan.getCluster().getMetadataQuery();
                  long totalIndexDocs = getIndexDocCount(plan, context);
                  if (totalIndexDocs <= 0) {
                    totalIndexDocs = getScanBaseRowCount(calcitePlan, mq);
                  }
                  Set<String> dateFieldNames = getDateFieldNames(plan, context);
                  boolean isTimeSeriesIndex =
                      !dateFieldNames.isEmpty()
                          || hasDateField(calcitePlan)
                          || hasDateFieldByName(logicalPlanNodes);
                  boolean hasDateRangeFilter =
                      logicalPlanNodes.stream()
                              .anyMatch(
                                  n ->
                                      n.contains("LogicalFilter")
                                          && dateFieldNames.stream()
                                              .anyMatch(
                                                  f ->
                                                      n.contains(f)
                                                          || n.contains("TIMESTAMP")
                                                          || n.contains("date(")))
                          || physicalPlanNodes.stream()
                              .anyMatch(
                                  n ->
                                      n.contains("FILTER")
                                          && dateFieldNames.stream().anyMatch(n::contains));

                  List<AnalyzeResponse.Recommendation> recommendations =
                      buildRecommendations(
                          operatorTree,
                          profile,
                          totalIndexDocs,
                          isTimeSeriesIndex,
                          hasDateRangeFilter);

                  AnalyzeResponse response =
                      AnalyzeResponse.builder()
                          .query(query)
                          .querySegments(querySegments)
                          .logicalPlan(logicalPlanNodes)
                          .physicalPlan(physicalPlanNodes)
                          .operator_tree(operatorTree)
                          .recommendations(recommendations)
                          .profile(profile)
                          .possibleCacheHit(possibleCacheHit)
                          .schema(schema)
                          .datarows(datarows)
                          .total(datarows.length)
                          .size(datarows.length)
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
    long pushedLogicalNodes = 0;
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

    // Collect non-cumulative costs per logical node for cost attribution.
    Map<Integer, Double> idToCost = new HashMap<>();
    collectNonCumulativeCosts(logicalPlan, mq, idToCost);

    // Compute exclusive time and rows per physical node from the profile plan tree.
    // The plan tree is top-down; we flatten it bottom-up to match operator tree order.
    List<double[]> physicalTimings = new ArrayList<>();
    if (profile != null && profile.getPlan() != null) {
      List<QueryProfile.PlanNode> planNodes = new ArrayList<>();
      QueryProfile.PlanNode current = (QueryProfile.PlanNode) profile.getPlan();
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

    // Collect per-segment plan IDs for cost attribution across all segments.
    List<Set<Integer>> allSegmentPlanIds = new ArrayList<>();
    for (int i = 0; i < querySegments.size(); i++) {
      Set<Integer> ids = i < exclusiveIds.size() ? exclusiveIds.get(i) : Set.of();
      allSegmentPlanIds.add(
          ids.stream()
              .filter(idToDescription::containsKey)
              .collect(java.util.stream.Collectors.toSet()));
    }
    List<Float> allSegmentCosts = computeSegmentCosts(allSegmentPlanIds, idToCost);

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
      List<Float> nodeCosts = allSegmentCosts.subList(0, pushedSegments);
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
              .node_cost(nodeCosts)
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
              .node_cost(List.of(allSegmentCosts.get(0)))
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
      List<Float> groupCosts = new ArrayList<>();
      long logicalNodesInGroup = 0;
      while (idx < querySegments.size() && logicalNodesInGroup < 1) {
        group.add(querySegments.get(idx));
        groupCosts.add(allSegmentCosts.get(idx));
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
              .node_cost(groupCosts)
              .description(descriptions.isEmpty() ? null : descriptions)
              .estimated_rows(getEstimatedRows(groupPlanIds, idToRowCount))
              .actual_time_ms(timing != null ? String.format("%.2f ms", timing[0]) : null)
              .actual_rows(timing != null ? (long) timing[1] : null)
              .build());
    }

    return operators;
  }

  private static boolean isLinearPlanTree(QueryProfile profile) {
    QueryProfile.PlanNode current = (QueryProfile.PlanNode) profile.getPlan();
    while (current != null) {
      if (current.getChildren() != null && current.getChildren().size() > 1) {
        return false;
      }
      current =
          (current.getChildren() != null && !current.getChildren().isEmpty())
              ? current.getChildren().get(0)
              : null;
    }
    return true;
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

  private static RelNode getLeafScanNode(RelNode node) {
    RelNode current = node;
    while (current != null) {
      List<RelNode> inputs = current.getInputs();
      if (inputs.isEmpty()) {
        return current;
      }
      current = inputs.get(0);
    }
    return node;
  }

  private static long getIndexDocCount(UnresolvedPlan plan, CalcitePlanContext context) {
    try {
      String[] indexNames = extractIndexNames(plan);
      if (indexNames.length == 0) {
        return -1;
      }
      org.apache.calcite.schema.SchemaPlus schema = context.config.getDefaultSchema();
      org.apache.calcite.schema.Table calciteTable = schema.getTable(indexNames[0]);
      if (calciteTable instanceof org.opensearch.sql.storage.Table storageTable) {
        return storageTable.getDocCount();
      }
    } catch (Exception ignored) {
    }
    return -1;
  }

  private static long getScanBaseRowCount(
      RelNode plan, org.apache.calcite.rel.metadata.RelMetadataQuery mq) {
    RelNode leaf = getLeafScanNode(plan);
    try {
      Double rowCount = mq.getRowCount(leaf);
      if (rowCount != null) {
        return Math.round(rowCount);
      }
    } catch (Exception ignored) {
    }
    return -1;
  }

  private static boolean hasDateField(RelNode plan) {
    RelNode leaf = getLeafScanNode(plan);
    try {
      org.apache.calcite.rel.type.RelDataType rowType;
      if (leaf instanceof org.apache.calcite.rel.core.TableScan tableScan) {
        rowType = tableScan.getTable().getRowType();
      } else {
        rowType = leaf.getRowType();
      }
      for (org.apache.calcite.rel.type.RelDataTypeField field : rowType.getFieldList()) {
        org.apache.calcite.rel.type.RelDataType fieldType = field.getType();
        org.apache.calcite.sql.type.SqlTypeName typeName = fieldType.getSqlTypeName();
        if (typeName == org.apache.calcite.sql.type.SqlTypeName.TIMESTAMP
            || typeName == org.apache.calcite.sql.type.SqlTypeName.DATE
            || typeName == org.apache.calcite.sql.type.SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
          return true;
        }
        if (fieldType
            instanceof org.opensearch.sql.calcite.type.AbstractExprRelDataType<?> exprType) {
          org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.ExprUDT udt = exprType.getUdt();
          if (udt == org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.ExprUDT.EXPR_TIMESTAMP
              || udt == org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.ExprUDT.EXPR_DATE) {
            return true;
          }
        }
      }
    } catch (Exception ignored) {
    }
    return false;
  }

  /**
   * Checks whether the index backing the query has a date/timestamp field by looking up the full
   * table schema from the Calcite schema registry. Unlike {@link #hasDateField(RelNode)}, this
   * approach is not affected by project pushdown which narrows the scan's row type to only the
   * fields referenced in the query.
   */
  private static boolean hasDateFieldFromSchema(UnresolvedPlan plan, CalcitePlanContext context) {
    try {
      String[] indexNames = extractIndexNames(plan);
      if (indexNames.length == 0) {
        return false;
      }
      org.apache.calcite.schema.SchemaPlus schema = context.config.getDefaultSchema();
      for (String indexName : indexNames) {
        org.apache.calcite.schema.Table table = schema.getTable(indexName);
        if (table == null) {
          continue;
        }
        org.apache.calcite.rel.type.RelDataType fullRowType =
            table.getRowType(org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.TYPE_FACTORY);
        for (org.apache.calcite.rel.type.RelDataTypeField field : fullRowType.getFieldList()) {
          org.apache.calcite.rel.type.RelDataType fieldType = field.getType();
          org.apache.calcite.sql.type.SqlTypeName typeName = fieldType.getSqlTypeName();
          if (typeName == org.apache.calcite.sql.type.SqlTypeName.TIMESTAMP
              || typeName == org.apache.calcite.sql.type.SqlTypeName.DATE
              || typeName
                  == org.apache.calcite.sql.type.SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
            return true;
          }
          if (fieldType
              instanceof org.opensearch.sql.calcite.type.AbstractExprRelDataType<?> exprType) {
            org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.ExprUDT udt = exprType.getUdt();
            if (udt == org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.ExprUDT.EXPR_TIMESTAMP
                || udt
                    == org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.ExprUDT.EXPR_DATE) {
              return true;
            }
          }
        }
      }
    } catch (Exception ignored) {
    }
    return false;
  }

  private static Set<String> getDateFieldNames(UnresolvedPlan plan, CalcitePlanContext context) {
    Set<String> dateFields = new HashSet<>();
    try {
      String[] indexNames = extractIndexNames(plan);
      if (indexNames.length == 0) {
        return dateFields;
      }
      org.apache.calcite.schema.SchemaPlus schema = context.config.getDefaultSchema();
      for (String indexName : indexNames) {
        org.apache.calcite.schema.Table table = schema.getTable(indexName);
        if (table == null) {
          continue;
        }
        org.apache.calcite.rel.type.RelDataType fullRowType =
            table.getRowType(org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.TYPE_FACTORY);
        for (org.apache.calcite.rel.type.RelDataTypeField field : fullRowType.getFieldList()) {
          org.apache.calcite.rel.type.RelDataType fieldType = field.getType();
          org.apache.calcite.sql.type.SqlTypeName typeName = fieldType.getSqlTypeName();
          if (typeName == org.apache.calcite.sql.type.SqlTypeName.TIMESTAMP
              || typeName == org.apache.calcite.sql.type.SqlTypeName.DATE
              || typeName
                  == org.apache.calcite.sql.type.SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
            dateFields.add(field.getName());
          } else if (fieldType
              instanceof org.opensearch.sql.calcite.type.AbstractExprRelDataType<?> exprType) {
            org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.ExprUDT udt = exprType.getUdt();
            if (udt == org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.ExprUDT.EXPR_TIMESTAMP
                || udt
                    == org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.ExprUDT.EXPR_DATE) {
              dateFields.add(field.getName());
            }
          }
        }
      }
    } catch (Exception ignored) {
    }
    return dateFields;
  }

  private static boolean hasDateFieldByName(List<String> logicalPlanNodes) {
    for (String node : logicalPlanNodes) {
      if (node.contains("timestamp")
          || node.contains("@timestamp")
          || node.contains("event_time")
          || node.contains("created_at")
          || node.contains("updated_at")
          || node.contains("date_field")
          || node.contains("EXPR_TIMESTAMP")
          || node.contains("EXPR_DATE")) {
        return true;
      }
    }
    return false;
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

  private void collectNonCumulativeCosts(
      RelNode node,
      org.apache.calcite.rel.metadata.RelMetadataQuery mq,
      Map<Integer, Double> idToCost) {
    try {
      org.apache.calcite.plan.RelOptCost cost = mq.getNonCumulativeCost(node);
      if (cost != null && !cost.isInfinite()) {
        double weight = cost.getCpu() + cost.getIo() * 10.0 + cost.getRows();
        idToCost.put(node.getId(), weight);
      }
    } catch (Exception ignored) {
    }
    for (RelNode input : node.getInputs()) {
      collectNonCumulativeCosts(input, mq, idToCost);
    }
  }

  private Long getEstimatedRows(Set<Integer> ids, Map<Integer, Double> idToRowCount) {
    return ids.stream()
        .filter(idToRowCount::containsKey)
        .max(Integer::compareTo)
        .map(id -> Math.round(idToRowCount.get(id)))
        .orElse(null);
  }

  /**
   * Compute per-segment cost fractions from Calcite's non-cumulative cost. Each segment's cost is
   * the sum of its exclusive RelNode costs, normalized to a percentage of the total across all
   * segments in the operator tree.
   */
  private List<Float> computeSegmentCosts(
      List<Set<Integer>> segmentPlanIds, Map<Integer, Double> idToCost) {
    List<Double> rawCosts = new ArrayList<>();
    for (Set<Integer> ids : segmentPlanIds) {
      double segCost = ids.stream().filter(idToCost::containsKey).mapToDouble(idToCost::get).sum();
      rawCosts.add(segCost);
    }
    double total = rawCosts.stream().mapToDouble(Double::doubleValue).sum();
    if (total <= 0) {
      return rawCosts.stream().map(c -> 0f).toList();
    }
    return rawCosts.stream().map(c -> (float) (c / total * 100.0)).toList();
  }

  private List<AnalyzeResponse.Recommendation> buildRecommendations(
      List<AnalyzeResponse.OperatorNode> operatorTree,
      QueryProfile profile,
      long totalIndexDocs,
      boolean isTimeSeriesIndex,
      boolean hasDateRangeFilter) {
    List<AnalyzeResponse.Recommendation> recommendations = new ArrayList<>();
    if (operatorTree == null || operatorTree.isEmpty() || profile == null) {
      return recommendations;
    }

    QueryProfile.Phase executePhase = profile.getPhases().get("execute");
    if (executePhase == null || executePhase.getTimeMillis() <= 0) {
      return recommendations;
    }
    double executeTime = executePhase.getTimeMillis();

    double maxTime = 0;
    AnalyzeResponse.OperatorNode bottleneck = null;

    for (AnalyzeResponse.OperatorNode node : operatorTree) {
      if (node.getActual_time_ms() == null) {
        continue;
      }
      double time = parseTimeMs(node.getActual_time_ms());
      if (time > maxTime) {
        maxTime = time;
        bottleneck = node;
      }
    }

    int totalNodes = operatorTree.size();
    int pushedDown = 0;
    for (AnalyzeResponse.OperatorNode node : operatorTree) {
      if (Boolean.TRUE.equals(node.getIs_pushed_down())) {
        pushedDown++;
      }
    }
    int inMemory = totalNodes - pushedDown;
    if (totalNodes > 0) {
      recommendations.add(
          AnalyzeResponse.Recommendation.builder()
              .serverity(AnalyzeResponse.RecommendationSeverityLevel.INFO)
              .rule("Pushdown visibility")
              .message(
                  pushedDown
                      + " of "
                      + totalNodes
                      + " stages pushed down; "
                      + inMemory
                      + " ran in-memory")
              .build());
    }

    if (bottleneck != null && maxTime > 0) {
      long pct = Math.round((maxTime / executeTime) * 100);
      String stage =
          (bottleneck.getNode_type() != null && !bottleneck.getNode_type().isEmpty())
              ? String.join(", ", bottleneck.getNode_type())
              : "unknown";
      recommendations.add(
          AnalyzeResponse.Recommendation.builder()
              .serverity(AnalyzeResponse.RecommendationSeverityLevel.INFO)
              .rule("Bottleneck stage")
              .message(pct + "% of time is in the *" + stage + "* stage")
              .affected_node(bottleneck.getSource())
              .suggestion("Consider optimizing the " + stage + " operation")
              .build());
    }

    // In-memory bottleneck: find the non-pushed-down node with the highest self-time
    double maxInMemoryTime = 0;
    AnalyzeResponse.OperatorNode inMemoryBottleneck = null;
    for (AnalyzeResponse.OperatorNode node : operatorTree) {
      if (Boolean.TRUE.equals(node.getIs_pushed_down())) {
        continue;
      }
      if (node.getActual_time_ms() == null) {
        continue;
      }
      double time = parseTimeMs(node.getActual_time_ms());
      if (time > maxInMemoryTime) {
        maxInMemoryTime = time;
        inMemoryBottleneck = node;
      }
    }
    if (inMemoryBottleneck != null
        && maxInMemoryTime > 0
        && inMemoryBottleneck.getActual_rows() != null) {
      long pct = Math.round((maxInMemoryTime / executeTime) * 100);
      String stage =
          (inMemoryBottleneck.getNode_type() != null
                  && !inMemoryBottleneck.getNode_type().isEmpty())
              ? String.join(", ", inMemoryBottleneck.getNode_type())
              : "unknown";
      recommendations.add(
          AnalyzeResponse.Recommendation.builder()
              .serverity(AnalyzeResponse.RecommendationSeverityLevel.WARNING)
              .rule("In-memory bottleneck")
              .message(
                  "Your *"
                      + stage
                      + "* ran in-memory over "
                      + inMemoryBottleneck.getActual_rows()
                      + " rows ("
                      + pct
                      + "% of time)")
              .affected_node(inMemoryBottleneck.getSource())
              .suggestion(
                  "Consider pushing this operation down or reducing input rows with filters")
              .build());
    }

    // Low scan selectivity: scan rows / total index docs > 80%
    log.info(
        "Low scan selectivity check: totalIndexDocs={}, operatorTree.size={}",
        totalIndexDocs,
        operatorTree.size());
    if (totalIndexDocs > 0) {
      AnalyzeResponse.OperatorNode scanNode = operatorTree.get(0);
      log.info(
          "Low scan selectivity: scanNode.actual_rows={}, scanNode.estimated_rows={}",
          scanNode.getActual_rows(),
          scanNode.getEstimated_rows());
      if (scanNode.getActual_rows() != null && scanNode.getActual_rows() > 0) {
        long scannedRows = scanNode.getActual_rows();
        long pct = Math.round((double) scannedRows / totalIndexDocs * 100);
        long resultRows =
            operatorTree.get(operatorTree.size() - 1).getActual_rows() != null
                ? operatorTree.get(operatorTree.size() - 1).getActual_rows()
                : 0;
        log.info(
            "Low scan selectivity: scannedRows={}, pct={}, resultRows={}",
            scannedRows,
            pct,
            resultRows);
        if (pct > 80) {
          recommendations.add(
              AnalyzeResponse.Recommendation.builder()
                  .serverity(AnalyzeResponse.RecommendationSeverityLevel.WARNING)
                  .rule("Low scan selectivity")
                  .message(
                      "Scanned "
                          + scannedRows
                          + " docs ("
                          + pct
                          + "% of index) to return "
                          + resultRows
                          + " rows")
                  .affected_node(scanNode.getSource())
                  .suggestion("Add filters to reduce the number of documents scanned")
                  .build());
        }
      }
    }

    // Missing time filter: time-series index with no date range predicate pushed down
    if (isTimeSeriesIndex && !hasDateRangeFilter) {
      AnalyzeResponse.OperatorNode scanNode = operatorTree.get(0);
      recommendations.add(
          AnalyzeResponse.Recommendation.builder()
              .serverity(AnalyzeResponse.RecommendationSeverityLevel.CRITICAL)
              .rule("Missing time filter")
              .message("No time filter on a time-series index: add one")
              .affected_node(scanNode.getSource())
              .suggestion(
                  "Add a time range filter (e.g. where @timestamp > now() - interval 1 hour)")
              .build());
    }

    return recommendations;
  }

  private static double parseTimeMs(String timeMsStr) {
    String stripped = timeMsStr.replaceAll("[^0-9.]", "");
    try {
      return Double.parseDouble(stripped);
    } catch (NumberFormatException e) {
      return 0;
    }
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

  /**
   * Walk the cause chain to find an {@link ArithmeticException} raised by checked arithmetic. Row-
   * level overflow surfaces wrapped (SQLException -&gt; RuntimeException -&gt; ErrorReport), so a
   * top-level {@code catch (ArithmeticException)} is insufficient.
   */
  private static ArithmeticException findArithmeticOverflow(@Nullable Throwable t) {
    for (Throwable cause = t;
        cause != null && cause != cause.getCause();
        cause = cause.getCause()) {
      if (cause instanceof ArithmeticException arithmeticException) {
        return arithmeticException;
      }
    }
    return null;
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

  private static String[] extractIndexNames(UnresolvedPlan plan) {
    Set<String> names = new HashSet<>();
    collectRelationNames(plan, names);
    return names.toArray(String[]::new);
  }

  private static void collectRelationNames(Node node, Set<String> names) {
    if (node instanceof Relation relation) {
      names.add(relation.getTableQualifiedName().toString());
    }
    if (node.getChild() != null) {
      for (Node child : node.getChild()) {
        collectRelationNames(child, names);
      }
    }
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
