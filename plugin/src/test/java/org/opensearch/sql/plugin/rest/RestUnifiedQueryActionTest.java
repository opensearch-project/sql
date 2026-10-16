/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.rest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.plugin.rest.RestUnifiedQueryAction.FORWARDED_CLUSTER_SETTINGS;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.calcite.rel.RelNode;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.analytics.EngineContextProvider;
import org.opensearch.analytics.exec.QueryPlanExecutor;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.indices.IndicesService;
import org.opensearch.sql.api.UnifiedQueryContext;
import org.opensearch.sql.ast.dsl.AstDSL;
import org.opensearch.sql.ast.expression.ParseMethod;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.ast.tree.Join;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.common.setting.Settings.Key;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.executor.QueryType;
import org.opensearch.transport.client.node.NodeClient;

/**
 * Tests for analytics index routing in RestUnifiedQueryAction. Routing requires both {@code
 * index.pluggable.dataformat.enabled=true} and {@code index.pluggable.dataformat=composite}.
 */
public class RestUnifiedQueryActionTest {

  private ClusterService clusterService;
  private Metadata metadata;
  private org.opensearch.sql.common.setting.Settings pluginSettings;
  private RestUnifiedQueryAction action;

  @Before
  public void setUp() {
    clusterService = mock(ClusterService.class);
    ClusterState clusterState = mock(ClusterState.class);
    metadata = mock(Metadata.class);
    when(clusterService.state()).thenReturn(clusterState);
    when(clusterState.metadata()).thenReturn(metadata);
    // isAnalyticsIndex short-circuits on the cluster.pluggable.dataformat setting; the per-index
    // path is only exercised when this returns something other than "composite".
    when(clusterService.getSettings()).thenReturn(Settings.EMPTY);

    pluginSettings = mock(org.opensearch.sql.common.setting.Settings.class);
    @SuppressWarnings("unchecked")
    QueryPlanExecutor<RelNode, Iterable<Object[]>> executor = mock(QueryPlanExecutor.class);
    action =
        new RestUnifiedQueryAction(
            mock(NodeClient.class),
            clusterService,
            executor,
            mock(EngineContextProvider.class),
            pluginSettings,
            new org.opensearch.sql.executor.DirectExecutionDispatcher());
  }

  @Test
  public void knnCommandIsRejectedOnTheAnalyticsRoute() {
    UnresolvedPlan plan =
        AstDSL.head(
            AstDSL.knn(
                AstDSL.relation("parquet_logs"),
                AstDSL.field("embedding"),
                AstDSL.vectorLiteral(1, 2, 3)),
            10,
            0);
    SemanticCheckException e =
        assertThrows(
            SemanticCheckException.class, () -> RestUnifiedQueryAction.rejectKnnSteps(plan));
    assertEquals("knn is not supported on the analytics engine yet", e.getMessage());
  }

  @Test
  public void knnFunctionIsRejectedOnTheAnalyticsRouteInAnyBranch() {
    UnresolvedPlan knnWhere =
        AstDSL.filter(
            AstDSL.relation("vectors"),
            AstDSL.knnFunction(
                AstDSL.field("embedding"),
                AstDSL.vectorLiteral(1, 2, 3),
                AstDSL.unresolvedArg("k", AstDSL.intLiteral(5))));
    UnresolvedPlan joined =
        AstDSL.join(knnWhere, Join.JoinType.INNER, Optional.empty())
            .attach(AstDSL.relation("parquet_logs"));
    UnresolvedPlan inSubsearch =
        AstDSL.filter(
            AstDSL.relation("parquet_logs"),
            AstDSL.inSubquery(
                AstDSL.project(knnWhere, AstDSL.field("doc_id")), AstDSL.field("doc_id")));
    for (UnresolvedPlan plan : List.of(knnWhere, joined, inSubsearch)) {
      SemanticCheckException e =
          assertThrows(
              SemanticCheckException.class, () -> RestUnifiedQueryAction.rejectKnnSteps(plan));
      assertEquals("knn is not supported on the analytics engine yet", e.getMessage());
    }
  }

  /**
   * knn() in a sort key, an aggregation, a join condition, the value list of an in, or the source
   * of parse is found too.
   */
  @Test
  public void knnFunctionIsRejectedOnTheAnalyticsRouteInAnyExpression() {
    UnresolvedPlan sorted =
        AstDSL.sort(
            AstDSL.relation("parquet_logs"),
            AstDSL.field(knnCall(), AstDSL.argument("asc", AstDSL.booleanLiteral(false))));
    UnresolvedPlan stats =
        AstDSL.agg(
            AstDSL.relation("parquet_logs"),
            List.of(AstDSL.alias("m", AstDSL.aggregate("max", knnCall()))),
            List.of(),
            List.of(),
            AstDSL.defaultStatsArgs());
    UnresolvedPlan joined =
        AstDSL.join(
                AstDSL.relation("docs"),
                Join.JoinType.INNER,
                Optional.of(AstDSL.compare(">", knnCall(), AstDSL.intLiteral(0))))
            .attach(AstDSL.relation("parquet_logs"));
    UnresolvedPlan inList =
        AstDSL.filter(
            AstDSL.relation("parquet_logs"),
            AstDSL.in(AstDSL.field("a"), AstDSL.intLiteral(1), knnCall()));
    UnresolvedPlan parsed =
        AstDSL.parse(
            AstDSL.relation("parquet_logs"),
            ParseMethod.REGEX,
            knnCall(),
            AstDSL.stringLiteral("(?<x>.*)"),
            Map.of());
    for (UnresolvedPlan plan : List.of(sorted, stats, joined, inList, parsed)) {
      SemanticCheckException e =
          assertThrows(
              SemanticCheckException.class, () -> RestUnifiedQueryAction.rejectKnnSteps(plan));
      assertEquals("knn is not supported on the analytics engine yet", e.getMessage());
    }
  }

  private static UnresolvedExpression knnCall() {
    return AstDSL.knnFunction(AstDSL.field("embedding"), AstDSL.vectorLiteral(1, 2, 3));
  }

  @Test
  public void queriesWithoutKnnPassTheAnalyticsRouteCheck() {
    RestUnifiedQueryAction.rejectKnnSteps(
        AstDSL.head(
            AstDSL.filter(
                AstDSL.relation("parquet_logs"),
                AstDSL.compare("=", AstDSL.field("knn"), AstDSL.intLiteral(1))),
            10,
            0));
  }

  @Test
  public void pluggableDataformatIndexRoutesToAnalytics() {
    registerIndex(
        "parquet_logs",
        Settings.builder()
            .put(IndexSettings.PLUGGABLE_DATAFORMAT_ENABLED_SETTING.getKey(), true)
            .put(IndexSettings.PLUGGABLE_DATAFORMAT_VALUE_SETTING.getKey(), "composite")
            .build());

    assertTrue(action.isAnalyticsIndex("source = parquet_logs | fields ts", QueryType.PPL));
    assertTrue(
        action.isAnalyticsIndex("source = opensearch.parquet_logs | fields ts", QueryType.PPL));
  }

  @Test
  public void pluggableEnabledButLuceneFormatRoutesToLucene() {
    registerIndex(
        "lucene_logs",
        Settings.builder()
            .put(IndexSettings.PLUGGABLE_DATAFORMAT_ENABLED_SETTING.getKey(), true)
            .put(IndexSettings.PLUGGABLE_DATAFORMAT_VALUE_SETTING.getKey(), "lucene")
            .build());

    assertFalse(action.isAnalyticsIndex("source = lucene_logs | fields ts", QueryType.PPL));
  }

  @Test
  public void indexWithoutSettingRoutesToLucene() {
    registerIndex("plain_logs", Settings.EMPTY);

    assertFalse(action.isAnalyticsIndex("source = plain_logs | fields ts", QueryType.PPL));
  }

  @Test
  public void missingIndexRoutesToLucene() {
    assertFalse(action.isAnalyticsIndex("source = does_not_exist | fields ts", QueryType.PPL));
  }

  @Test
  public void sqlQueryRoutesToAnalyticsForPluggableIndex() {
    registerIndex(
        "parquet_logs",
        Settings.builder()
            .put(IndexSettings.PLUGGABLE_DATAFORMAT_ENABLED_SETTING.getKey(), true)
            .put(IndexSettings.PLUGGABLE_DATAFORMAT_VALUE_SETTING.getKey(), "composite")
            .build());

    assertTrue(action.isAnalyticsIndex("SELECT * FROM parquet_logs", QueryType.SQL));
    assertTrue(
        action.isAnalyticsIndex(
            "SELECT ts, level FROM parquet_logs WHERE level = 'ERROR'", QueryType.SQL));
  }

  @Test
  public void sqlQueryWithSchemaRoutesToAnalytics() {
    registerIndex(
        "parquet_logs",
        Settings.builder()
            .put(IndexSettings.PLUGGABLE_DATAFORMAT_ENABLED_SETTING.getKey(), true)
            .put(IndexSettings.PLUGGABLE_DATAFORMAT_VALUE_SETTING.getKey(), "composite")
            .build());

    assertTrue(action.isAnalyticsIndex("SELECT * FROM opensearch.parquet_logs", QueryType.SQL));
  }

  @Test
  public void sqlQueryRoutesToLuceneForNonPluggableIndex() {
    registerIndex("plain_logs", Settings.EMPTY);

    assertFalse(action.isAnalyticsIndex("SELECT * FROM plain_logs", QueryType.SQL));
  }

  @Test
  public void sqlQueryRoutesToLuceneForMissingIndex() {
    assertFalse(action.isAnalyticsIndex("SELECT * FROM does_not_exist", QueryType.SQL));
  }

  @Test
  public void nullAndEmptyQueriesRouteToLucene() {
    assertFalse(action.isAnalyticsIndex(null, QueryType.PPL));
    assertFalse(action.isAnalyticsIndex("", QueryType.PPL));
    assertFalse(action.isAnalyticsIndex(null, QueryType.SQL));
    assertFalse(action.isAnalyticsIndex("", QueryType.SQL));
  }

  @Test
  public void showStatementNotRoutedToAnalyticsEngine() {
    registerIndex(
        "parquet_logs",
        Settings.builder()
            .put(IndexSettings.PLUGGABLE_DATAFORMAT_ENABLED_SETTING.getKey(), true)
            .put(IndexSettings.PLUGGABLE_DATAFORMAT_VALUE_SETTING.getKey(), "composite")
            .build());

    assertFalse(action.isAnalyticsIndex("SHOW TABLES LIKE 'parquet_logs'", QueryType.SQL));
  }

  @Test
  public void describeStatementNotRoutedToAnalyticsEngine() {
    registerIndex(
        "parquet_logs",
        Settings.builder()
            .put(IndexSettings.PLUGGABLE_DATAFORMAT_ENABLED_SETTING.getKey(), true)
            .put(IndexSettings.PLUGGABLE_DATAFORMAT_VALUE_SETTING.getKey(), "composite")
            .build());

    assertFalse(action.isAnalyticsIndex("DESCRIBE TABLES LIKE 'parquet_logs'", QueryType.SQL));
  }

  @Test
  public void showStatementNotRoutedToAnalyticsEngineUnderClusterComposite() {
    enableClusterComposite();
    assertFalse(action.isAnalyticsIndex("SHOW TABLES LIKE 'parquet_logs'", QueryType.SQL));
  }

  @Test
  public void describeStatementNotRoutedToAnalyticsEngineUnderClusterComposite() {
    enableClusterComposite();
    assertFalse(action.isAnalyticsIndex("DESCRIBE TABLES LIKE 'parquet_logs'", QueryType.SQL));
  }

  @Test
  public void restCommandNotRoutedToAnalyticsEngineUnderClusterComposite() {
    enableClusterComposite();
    assertFalse(action.isAnalyticsIndex("| rest '/_cluster/health'", QueryType.PPL));
  }

  @Test
  public void dataQueryStillRoutesToAnalyticsUnderClusterComposite() {
    enableClusterComposite();
    assertTrue(action.isAnalyticsIndex("SELECT * FROM parquet_logs", QueryType.SQL));
  }

  @Test
  public void unparseableQueryRoutesToAnalyticsUnderClusterComposite() {
    enableClusterComposite();
    // malformed -> AE re-parses & reports
    assertTrue(action.isAnalyticsIndex("SELECT FROM WHERE", QueryType.SQL));
  }

  @Test
  public void legacyShowNotRoutedToAnalyticsEngineUnderClusterComposite() {
    enableClusterComposite();
    // unquoted LIKE is rejected by the V2 parser, but still belongs on the default pipeline
    assertFalse(action.isAnalyticsIndex("SHOW TABLES LIKE %", QueryType.SQL));
  }

  @Test
  public void legacyDescribeNotRoutedToAnalyticsEngineUnderClusterComposite() {
    enableClusterComposite();
    // legacy DESCRIBE syntax is rejected by the V2 parser, but belongs on the default pipeline
    assertFalse(action.isAnalyticsIndex("DESCRIBE my_index", QueryType.SQL));
  }

  @Test
  public void pplDescribeNotRoutedToAnalyticsEngineUnderClusterComposite() {
    enableClusterComposite();
    assertFalse(action.isAnalyticsIndex("describe parquet_logs", QueryType.PPL));
  }

  @Test
  public void pplShowDatasourcesNotRoutedToAnalyticsEngineUnderClusterComposite() {
    enableClusterComposite();
    assertFalse(action.isAnalyticsIndex("show datasources", QueryType.PPL));
  }

  @Test
  public void pplDataQueryStillRoutesToAnalyticsUnderClusterComposite() {
    enableClusterComposite();
    assertTrue(action.isAnalyticsIndex("source = parquet_logs | fields ts", QueryType.PPL));
  }

  @Test
  public void pplUnparseableQueryRoutesToAnalyticsUnderClusterComposite() {
    enableClusterComposite();
    // malformed -> AE re-parses & reports
    assertTrue(action.isAnalyticsIndex("source = parquet_logs | | fields ts", QueryType.PPL));
  }

  @Test
  public void clusterQuerySizeLimitReachesAnalyticsContext() {
    // Regression: the AE path pinned QUERY_SIZE_LIMIT to the builder's hardcoded default (10000),
    // silently ignoring the configured cluster value. Forwarding must carry the live value through
    // to the plan context, since addQuerySizeLimit reads it from there.
    when(pluginSettings.getSettingValue(Key.QUERY_SIZE_LIMIT)).thenReturn(500);

    assertEquals(
        "Cluster plugins.query.size_limit must reach the AE plan context",
        Integer.valueOf(500),
        buildAnalyticsContext().getPlanContext().sysLimit.querySizeLimit());
  }

  @Test
  public void calciteEngineEnabledNotOverriddenByCluster() {
    // CALCITE_ENGINE_ENABLED is deliberately excluded from forwarding: the unified path is
    // Calcite-based by definition and must stay true even if the cluster disables it.
    when(pluginSettings.getSettingValue(Key.CALCITE_ENGINE_ENABLED)).thenReturn(false);

    assertEquals(
        "Unified path must force Calcite on regardless of the cluster setting",
        Boolean.TRUE,
        buildAnalyticsContext().getSettings().getSettingValue(Key.CALCITE_ENGINE_ENABLED));
  }

  /**
   * Every setting the AE path reads that {@link RestUnifiedQueryAction} deliberately does not
   * forward, with the reason it stays unforwarded. Derived from the call sites reachable from the
   * unified context's {@code Settings}: {@code SysLimit.fromSettings}, the {@code AstBuilder} /
   * {@code AstExpressionBuilder} / {@code AstBuildGuard} behind its parser, and {@code
   * UnresolvedPlanHelper}.
   *
   * <ul>
   *   <li>{@link Key#CALCITE_ENGINE_ENABLED} — the unified path is Calcite-based by definition.
   *   <li>{@link Key#PPL_SUBSEARCH_MAXOUT} / {@link Key#PPL_JOIN_SUBSEARCH_MAXOUT} — seeded to
   *       {@code 0} (unlimited) on purpose, for external consumers of the unified query API.
   *   <li>{@link Key#PPL_VALUES_MAX_LIMIT} — forwarding it lowers {@code values()} to {@code
   *       array_agg(DISTINCT x, limit)}, which the backend cannot bind, so the query fails outright
   *       where today it merely ignores the cap.
   *   <li>{@link Key#CALCITE_SUPPORT_ALL_JOIN_TYPES} — never seeded, so {@code
   *       AstBuilder.validateJoinType} reads {@code null} and skips the high-cost-join guard.
   *       Restoring it is a user-visible tightening.
   * </ul>
   */
  private static final List<Key> DELIBERATELY_NOT_FORWARDED =
      List.of(
          Key.CALCITE_ENGINE_ENABLED,
          Key.PPL_SUBSEARCH_MAXOUT,
          Key.PPL_JOIN_SUBSEARCH_MAXOUT,
          Key.PPL_VALUES_MAX_LIMIT,
          Key.CALCITE_SUPPORT_ALL_JOIN_TYPES);

  /**
   * Drift guard for the defect class this forwarding exists to prevent: the builder's seed map and
   * the handler's forward list are maintained independently, so a planning setting seeded but not
   * forwarded silently regresses to its hardcoded default (this is how {@code
   * plugins.query.size_limit} came to be ignored on the AE path). Every seeded key must therefore
   * be classified — forwarded, or explicitly excluded with a reason.
   */
  @Test
  public void everySeededPlanningSettingIsClassified() {
    UnifiedQueryContext defaults = UnifiedQueryContext.builder().language(QueryType.PPL).build();

    for (Object entry : defaults.getSettings().getSettings()) {
      Key seeded = ((Map.Entry<Key, ?>) entry).getKey();
      assertTrue(
          "Setting "
              + seeded.getKeyValue()
              + " is seeded with a hardcoded default by UnifiedQueryContext.Builder but is neither"
              + " forwarded from the cluster nor listed in DELIBERATELY_NOT_FORWARDED. Add it to"
              + " RestUnifiedQueryAction.FORWARDED_CLUSTER_SETTINGS so the configured cluster value"
              + " reaches the Analytics Engine, or document why it must stay hardcoded.",
          FORWARDED_CLUSTER_SETTINGS.contains(seeded)
              || DELIBERATELY_NOT_FORWARDED.contains(seeded));
    }
  }

  @Test
  public void clusterPatternSettingsReachAnalyticsContext() {
    // patterns command defaults are read straight off the context's settings in AstBuilder, so a
    // cluster-configured method/mode/limit must be visible there rather than the seeded default.
    when(pluginSettings.getSettingValue(Key.PATTERN_METHOD)).thenReturn("BRAIN");
    when(pluginSettings.getSettingValue(Key.PATTERN_MODE)).thenReturn("AGGREGATION");
    when(pluginSettings.getSettingValue(Key.PATTERN_MAX_SAMPLE_COUNT)).thenReturn(42);
    when(pluginSettings.getSettingValue(Key.PATTERN_BUFFER_LIMIT)).thenReturn(60000);
    when(pluginSettings.getSettingValue(Key.PATTERN_SHOW_NUMBERED_TOKEN)).thenReturn(true);

    org.opensearch.sql.common.setting.Settings forwarded = buildAnalyticsContext().getSettings();

    assertEquals("BRAIN", forwarded.getSettingValue(Key.PATTERN_METHOD));
    assertEquals("AGGREGATION", forwarded.getSettingValue(Key.PATTERN_MODE));
    assertEquals(Integer.valueOf(42), forwarded.getSettingValue(Key.PATTERN_MAX_SAMPLE_COUNT));
    assertEquals(Integer.valueOf(60000), forwarded.getSettingValue(Key.PATTERN_BUFFER_LIMIT));
    assertEquals(Boolean.TRUE, forwarded.getSettingValue(Key.PATTERN_SHOW_NUMBERED_TOKEN));
  }

  @Test
  public void documentedExclusionsAreNotForwarded() {
    // Pins the exclusion list in RestUnifiedQueryAction's javadoc: each of these is a conscious
    // decision, not an oversight, so a well-meaning "just forward everything" change fails here.
    // PPL_VALUES_MAX_LIMIT in particular must stay out: forwarding it lowers values() to
    // array_agg(DISTINCT x, limit), which the AE backend cannot bind, turning a silently-ignored
    // cap into a 500 (verified on a live composite/parquet cluster).
    DELIBERATELY_NOT_FORWARDED.forEach(
        key -> when(pluginSettings.getSettingValue(key)).thenReturn(SENTINEL));

    org.opensearch.sql.common.setting.Settings forwarded = buildAnalyticsContext().getSettings();

    for (Key key : DELIBERATELY_NOT_FORWARDED) {
      assertNotEquals(
          key.getKeyValue() + " must not be forwarded to the Analytics Engine context",
          SENTINEL,
          forwarded.getSettingValue(key));
    }
  }

  /** Value no seeded default uses, so "forwarded" is distinguishable from "seeded" or "absent". */
  private static final Integer SENTINEL = -12345;

  /** Builds the context the AE path plans against, with the mocked cluster settings applied. */
  private UnifiedQueryContext buildAnalyticsContext() {
    return action
        .applyClusterOverrides(UnifiedQueryContext.builder().language(QueryType.PPL))
        .build();
  }

  private void enableClusterComposite() {
    when(clusterService.getSettings())
        .thenReturn(
            Settings.builder()
                .put(
                    IndicesService.CLUSTER_PLUGGABLE_DATAFORMAT_VALUE_SETTING.getKey(), "composite")
                .build());
  }

  private void registerIndex(String name, Settings settings) {
    IndexMetadata indexMetadata = mock(IndexMetadata.class);
    when(indexMetadata.getSettings()).thenReturn(settings);
    when(metadata.index(name)).thenReturn(indexMetadata);
  }
}
