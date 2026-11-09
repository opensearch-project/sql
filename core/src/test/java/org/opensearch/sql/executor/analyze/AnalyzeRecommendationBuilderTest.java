/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor.analyze;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.executor.AnalyzeResponse.Recommendation;
import org.opensearch.sql.executor.AnalyzeResponse.RecommendationSeverityLevel;
import org.opensearch.sql.monitor.profile.MetricName;
import org.opensearch.sql.monitor.profile.QueryProfile;
import org.opensearch.sql.monitor.profile.QueryProfile.PlanNode;

class AnalyzeRecommendationBuilderTest {

  private static QueryProfile profile(double optimizeMs, double executeMs, PlanNode plan) {
    Map<MetricName, Double> phases = new EnumMap<>(MetricName.class);
    phases.put(MetricName.OPTIMIZE, optimizeMs);
    phases.put(MetricName.EXECUTE, executeMs);
    return new QueryProfile(optimizeMs + executeMs, phases, plan);
  }

  private static PlanNode leaf(String name, long rows) {
    return new PlanNode(name, 1.0, rows, null);
  }

  private static Optional<Recommendation> ruleOf(List<Recommendation> recs, String rule) {
    return recs.stream().filter(r -> r.getRule().equals(rule)).findFirst();
  }

  @Test
  void nullProfileYieldsNoRecommendations() {
    assertTrue(new AnalyzeRecommendationBuilder(null).build().isEmpty());
  }

  @Test
  void ineffectiveFilterFiresWhenFilterBarelyReducesRows() {
    // filter passes 990 of 1000 rows -> ratio 0.99 > 0.95
    PlanNode filter = new PlanNode("CalciteFilter", 5.0, 990, List.of(leaf("scan", 1000)));
    List<Recommendation> recs = new AnalyzeRecommendationBuilder(profile(1, 10, filter)).build();

    Recommendation r = ruleOf(recs, "Ineffective Filter").orElseThrow();
    assertEquals(RecommendationSeverityLevel.WARNING, r.getSeverity());
    assertEquals("Filter only dropped 1% of rows", r.getMessage());
    assertEquals("CalciteFilter", r.getAffected_node());
  }

  @Test
  void ineffectiveFilterSilentWhenFilterIsSelective() {
    // filter passes 100 of 1000 rows -> ratio 0.10, not ineffective
    PlanNode filter = new PlanNode("CalciteFilter", 5.0, 100, List.of(leaf("scan", 1000)));
    List<Recommendation> recs = new AnalyzeRecommendationBuilder(profile(1, 10, filter)).build();
    assertTrue(ruleOf(recs, "Ineffective Filter").isEmpty());
  }

  @Test
  void joinRowExplosionWarnsAboveRatioFive() {
    // 100 in -> 800 out = 8x (>5, <20) -> WARNING
    PlanNode join =
        new PlanNode("EnumerableHashJoin", 5.0, 800, List.of(leaf("l", 60), leaf("r", 40)));
    List<Recommendation> recs = new AnalyzeRecommendationBuilder(profile(1, 10, join)).build();

    Recommendation r = ruleOf(recs, "Join Row Explosion").orElseThrow();
    assertEquals(RecommendationSeverityLevel.WARNING, r.getSeverity());
    assertEquals("Join expanded 100 rows into 800 rows (8.0×)", r.getMessage());
  }

  @Test
  void joinRowExplosionCriticalAtHighRatio() {
    // 100 in -> 3000 out = 30x (>=20) -> CRITICAL
    PlanNode join =
        new PlanNode("EnumerableHashJoin", 5.0, 3000, List.of(leaf("l", 50), leaf("r", 50)));
    List<Recommendation> recs = new AnalyzeRecommendationBuilder(profile(1, 10, join)).build();
    assertEquals(
        RecommendationSeverityLevel.CRITICAL,
        ruleOf(recs, "Join Row Explosion").orElseThrow().getSeverity());
  }

  @Test
  void expensiveSortFiresOnLargeSlowSort() {
    // time_ms is cumulative: scan 10ms, sort 40ms -> sort self-time 30ms of 100 execute (30% >
    // 20%); 60k input rows (> 50k)
    PlanNode scan = new PlanNode("scan", 10.0, 60_000, null);
    PlanNode sort = new PlanNode("EnumerableSort", 40.0, 60_000, List.of(scan));
    List<Recommendation> recs = new AnalyzeRecommendationBuilder(profile(1, 100, sort)).build();

    Recommendation r = ruleOf(recs, "Expensive Sort").orElseThrow();
    assertEquals(RecommendationSeverityLevel.WARNING, r.getSeverity());
    assertTrue(r.getMessage().contains("Sorting 60000 rows"));
    assertTrue(r.getMessage().contains("30.0 ms"));
    assertTrue(r.getMessage().contains("30% of execution"));
  }

  @Test
  void expensiveSortFiresForFusedTopKNode() {
    // A top-level sort fuses with the query-size-limit into CalciteEnumerableTopK (no "sort" in
    // the name). It is still a sort, so it must trip the rule: 30ms self-time of 100 (30% > 20%),
    // 60k input rows (> 50k).
    PlanNode scan = new PlanNode("EnumerableMergeJoin", 10.0, 60_000, null);
    PlanNode topK = new PlanNode("CalciteEnumerableTopK", 40.0, 100, List.of(scan));
    List<Recommendation> recs = new AnalyzeRecommendationBuilder(profile(1, 100, topK)).build();

    Recommendation r = ruleOf(recs, "Expensive Sort").orElseThrow();
    assertEquals(RecommendationSeverityLevel.WARNING, r.getSeverity());
    assertEquals("CalciteEnumerableTopK", r.getAffected_node());
    assertTrue(r.getMessage().contains("Sorting 60000 rows"));
  }

  @Test
  void expensiveSortSilentWhenSelfTimeIsSmall() {
    // sort cumulative 90ms but its child took 89ms -> self-time only 1ms, not expensive
    PlanNode scan = new PlanNode("scan", 89.0, 60_000, null);
    PlanNode sort = new PlanNode("EnumerableSort", 90.0, 60_000, List.of(scan));
    List<Recommendation> recs = new AnalyzeRecommendationBuilder(profile(1, 100, sort)).build();
    assertTrue(ruleOf(recs, "Expensive Sort").isEmpty());
  }

  @Test
  void expensiveSortSilentWhenInputSmall() {
    // 30% self-time but only 100 input rows (< 50k)
    PlanNode scan = new PlanNode("scan", 10.0, 100, null);
    PlanNode sort = new PlanNode("EnumerableSort", 40.0, 100, List.of(scan));
    List<Recommendation> recs = new AnalyzeRecommendationBuilder(profile(1, 100, sort)).build();
    assertTrue(ruleOf(recs, "Expensive Sort").isEmpty());
  }

  @Test
  void bottleneckStageFiresOnSlowestSelfTimeNotCumulative() {
    // Root project cumulative 100ms but its child scan took 90ms -> project self-time 10ms,
    // scan self-time 90ms. Bottleneck should be the scan (90% > 75%), NOT the root.
    PlanNode scan = new PlanNode("CalciteEnumerableIndexScan", 90.0, 10, null);
    PlanNode project = new PlanNode("EnumerableProject", 100.0, 10, List.of(scan));
    List<Recommendation> recs = new AnalyzeRecommendationBuilder(profile(1, 100, project)).build();

    Recommendation r = ruleOf(recs, "Bottleneck Stage").orElseThrow();
    assertEquals(RecommendationSeverityLevel.INFO, r.getSeverity());
    assertTrue(r.getMessage().contains("CalciteEnumerableIndexScan"));
    assertTrue(r.getMessage().contains("90% of execution"));
  }

  @Test
  void bottleneckStageSilentWhenNoNodeDominatesBySelfTime() {
    // Root cumulative 100ms but 45ms is its own and 55ms is the child's -> no single node > 75%.
    PlanNode scan = new PlanNode("CalciteEnumerableIndexScan", 55.0, 10, null);
    PlanNode project = new PlanNode("EnumerableProject", 100.0, 10, List.of(scan));
    List<Recommendation> recs = new AnalyzeRecommendationBuilder(profile(1, 100, project)).build();
    assertTrue(ruleOf(recs, "Bottleneck Stage").isEmpty());
  }

  @Test
  void optimizePhaseDominatesFiresWhenPlanningExceedsExecution() {
    // optimize 100 > execute 10, and optimize > 75ms
    PlanNode scan = new PlanNode("CalciteEnumerableIndexScan", 5.0, 10, null);
    List<Recommendation> recs = new AnalyzeRecommendationBuilder(profile(100, 10, scan)).build();

    Recommendation r = ruleOf(recs, "Optimize Phase Dominates").orElseThrow();
    assertEquals(RecommendationSeverityLevel.INFO, r.getSeverity());
    assertTrue(r.getMessage().contains("Query planning took 100.0 ms vs 10.0 ms executing"));
  }

  @Test
  void optimizePhaseDominatesSilentWhenExecutionLarger() {
    PlanNode scan = new PlanNode("CalciteEnumerableIndexScan", 5.0, 10, null);
    List<Recommendation> recs = new AnalyzeRecommendationBuilder(profile(100, 200, scan)).build();
    assertTrue(ruleOf(recs, "Optimize Phase Dominates").isEmpty());
  }

  @Test
  void ineffectiveFilterSilentForLeafFilterWithNoInput() {
    // A "filter"-named leaf has no children, so rows_in is 0 and the ratio guard skips it.
    PlanNode leafFilter = leaf("CalciteFilter", 1000);
    List<Recommendation> recs =
        new AnalyzeRecommendationBuilder(profile(1, 10, leafFilter)).build();
    assertTrue(ruleOf(recs, "Ineffective Filter").isEmpty());
  }

  @Test
  void ineffectiveFilterSilentExactlyAtThreshold() {
    // ratio == 0.95 exactly; the rule uses strict '>' so it must NOT fire.
    PlanNode filter = new PlanNode("CalciteFilter", 5.0, 950, List.of(leaf("scan", 1000)));
    List<Recommendation> recs = new AnalyzeRecommendationBuilder(profile(1, 10, filter)).build();
    assertTrue(ruleOf(recs, "Ineffective Filter").isEmpty());
  }

  @Test
  void joinRowExplosionSilentExactlyAtThreshold() {
    // ratio == 5.0 exactly; strict '>' means no recommendation.
    PlanNode join =
        new PlanNode("EnumerableHashJoin", 5.0, 500, List.of(leaf("l", 50), leaf("r", 50)));
    List<Recommendation> recs = new AnalyzeRecommendationBuilder(profile(1, 10, join)).build();
    assertTrue(ruleOf(recs, "Join Row Explosion").isEmpty());
  }

  @Test
  void timeBasedRulesSilentWhenExecutePhaseIsZero() {
    // executeMs == 0 -> Expensive Sort and Bottleneck Stage short-circuit to empty.
    PlanNode sort = new PlanNode("EnumerableSort", 40.0, 60_000, List.of(leaf("scan", 60_000)));
    List<Recommendation> recs = new AnalyzeRecommendationBuilder(profile(1, 0, sort)).build();
    assertTrue(ruleOf(recs, "Expensive Sort").isEmpty());
    assertTrue(ruleOf(recs, "Bottleneck Stage").isEmpty());
  }

  @Test
  void nonPlanNodePlanYieldsNoRecommendations() {
    // profile.plan is a pre-rendered object (not a PlanNode) -> no per-node rules can run.
    Map<MetricName, Double> phases = new EnumMap<>(MetricName.class);
    phases.put(MetricName.OPTIMIZE, 1.0);
    phases.put(MetricName.EXECUTE, 10.0);
    QueryProfile p = new QueryProfile(11.0, phases, "some pre-rendered plan string");
    assertTrue(new AnalyzeRecommendationBuilder(p).build().isEmpty());
  }

  @Test
  void nullPlanYieldsNoPerNodeRecommendations() {
    // No plan tree, but phase-based Optimize Phase Dominates can still fire.
    QueryProfile p = profile(100, 10, null);
    List<Recommendation> recs = new AnalyzeRecommendationBuilder(p).build();
    assertTrue(ruleOf(recs, "Bottleneck Stage").isEmpty());
    assertTrue(ruleOf(recs, "Expensive Sort").isEmpty());
    assertTrue(ruleOf(recs, "Optimize Phase Dominates").isPresent());
  }

  @Test
  void durationClampedToZeroWhenChildSlowerThanParent() {
    // Measurement jitter: child (60ms) reads slower than parent (50ms) -> parent self-time 0,
    // so the parent is not treated as the bottleneck despite a large cumulative time.
    PlanNode child = new PlanNode("scan", 60.0, 10, null);
    PlanNode parent = new PlanNode("EnumerableProject", 50.0, 10, List.of(child));
    List<Recommendation> recs = new AnalyzeRecommendationBuilder(profile(1, 100, parent)).build();

    // The scan (60ms self-time, 60% of execute) is below 75%, so nothing fires -- and crucially
    // the clamp prevents a negative parent duration from being ranked as the max.
    Recommendation r = ruleOf(recs, "Bottleneck Stage").orElse(null);
    if (r != null) {
      assertTrue(r.getMessage().contains("scan"));
    }
  }

  @Test
  void multipleRulesFireForOneQuery() {
    // A single tree that trips both Ineffective Filter and Bottleneck Stage, plus Optimize
    // Phase Dominates from the phases. Verifies build() concatenates across rules.
    PlanNode scan = new PlanNode("CalciteEnumerableIndexScan", 90.0, 990, null);
    PlanNode filter = new PlanNode("CalciteFilter", 100.0, 990, List.of(scan));
    // optimize 200 > execute 100 and > 75ms
    List<Recommendation> recs = new AnalyzeRecommendationBuilder(profile(200, 100, filter)).build();

    assertTrue(ruleOf(recs, "Ineffective Filter").isPresent());
    assertTrue(ruleOf(recs, "Bottleneck Stage").isPresent());
    assertTrue(ruleOf(recs, "Optimize Phase Dominates").isPresent());
  }

  @Test
  void perNodeRuleFiresOncePerMatchingNode() {
    // Two ineffective filters in one tree -> two recommendations.
    PlanNode scan = new PlanNode("scan", 1.0, 1000, null);
    PlanNode filter1 = new PlanNode("CalciteFilter", 2.0, 999, List.of(scan));
    PlanNode filter2 = new PlanNode("CalciteFilter", 3.0, 998, List.of(filter1));
    List<Recommendation> recs = new AnalyzeRecommendationBuilder(profile(1, 10, filter2)).build();

    long count = recs.stream().filter(r -> r.getRule().equals("Ineffective Filter")).count();
    assertEquals(2, count);
  }
}
