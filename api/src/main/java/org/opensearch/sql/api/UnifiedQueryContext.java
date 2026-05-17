/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api;

import static org.opensearch.sql.common.setting.Settings.Key.CALCITE_ENGINE_ENABLED;
import static org.opensearch.sql.common.setting.Settings.Key.PPL_JOIN_SUBSEARCH_MAXOUT;
import static org.opensearch.sql.common.setting.Settings.Key.PPL_REX_MAX_MATCH_LIMIT;
import static org.opensearch.sql.common.setting.Settings.Key.PPL_SUBSEARCH_MAXOUT;
import static org.opensearch.sql.common.setting.Settings.Key.QUERY_SIZE_LIMIT;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Callable;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Programs;
import org.opensearch.sql.api.parser.PPLQueryParser;
import org.opensearch.sql.api.parser.SqlV2QueryParser;
import org.opensearch.sql.api.parser.UnifiedQueryParser;
import org.opensearch.sql.api.spec.LanguageSpec;
import org.opensearch.sql.api.spec.UnifiedPplSpec;
import org.opensearch.sql.api.spec.UnifiedSqlSpec;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.SysLimit;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.executor.QueryType;
import org.opensearch.sql.monitor.profile.MetricName;
import org.opensearch.sql.monitor.profile.ProfileMetric;
import org.opensearch.sql.monitor.profile.QueryProfile;
import org.opensearch.sql.monitor.profile.QueryProfiling;

/**
 * A reusable abstraction shared across unified query components (planner, compiler, etc.). This
 * centralizes configuration for catalog schemas, query type, execution limits, and other settings,
 * enabling consistent behavior across all unified query operations.
 */
@AllArgsConstructor
@Getter
public class UnifiedQueryContext implements AutoCloseable {

  /** CalcitePlanContext containing Calcite framework configuration and query type. */
  private final CalcitePlanContext planContext;

  /** Settings containing execution limits and feature flags used by parsers and planners. */
  private final Settings settings;

  /** Query parser created eagerly from this context's configuration. */
  private final UnifiedQueryParser<?> parser;

  /** Language spec for the query's frontend (SQL or PPL). */
  private final LanguageSpec langSpec;

  /**
   * Returns the profiling result. Call after query execution to retrieve collected metrics. Returns
   * empty if profiling was not enabled.
   */
  public Optional<QueryProfile> getProfile() {
    return Optional.ofNullable(QueryProfiling.current().finish());
  }

  /**
   * Measures the execution time of the given action and records it as a profiling metric. When
   * profiling is disabled, the action executes with no overhead. Use this for phases outside
   * unified query components (e.g., execution, formatting).
   *
   * @param <T> the return type of the action
   * @param metricName the metric to record
   * @param action the action to measure
   * @return the result of the action
   * @throws Exception if the action throws
   */
  public <T> T measure(MetricName metricName, Callable<T> action) throws Exception {
    ProfileMetric metric = QueryProfiling.current().getOrCreateMetric(metricName);
    long start = System.nanoTime();
    try {
      return action.call();
    } finally {
      metric.set(System.nanoTime() - start);
    }
  }

  /**
   * Closes the underlying resource managed by this context.
   *
   * @throws Exception if an error occurs while closing the connection
   */
  @Override
  public void close() throws Exception {
    QueryProfiling.clear();
    if (planContext != null && planContext.connection != null) {
      planContext.connection.close();
    }
  }

  /** Creates a new builder for UnifiedQueryContext. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builder that constructs UnifiedQueryContext. */
  public static class Builder {
    private QueryType queryType;
    private final Map<String, Schema> catalogs = new HashMap<>();
    private String defaultNamespace;
    private boolean cacheMetadata = false;
    private boolean profiling = false;

    /**
     * Setting values with defaults from SysLimit.DEFAULT. Only includes planning-required settings
     * to avoid coupling with OpenSearchSettings.
     *
     * <p>{@link Settings.Key#PPL_JOIN_SUBSEARCH_MAXOUT} defaults to {@code 0} to avoid injecting
     * {@code LogicalSystemLimit} into the logical plan, which is an OpenSearch-specific operational
     * concern irrelevant to external consumers of the unified query API. {@link
     * Settings.Key#PPL_SUBSEARCH_MAXOUT} is set to {@code 0} for the same reason.
     *
     * <p>{@link Settings.Key#CALCITE_ENGINE_ENABLED} defaults to {@code true} here because the
     * unified query path is by definition Calcite-based — every query reaching this context flows
     * through Calcite's planner, never the v2 engine. The PPL {@link
     * org.opensearch.sql.api.parser.PPLQueryParser} reuses the v2 {@code AstBuilder}, which gates
     * Calcite-only commands (e.g. {@code visitTableCommand}) on this setting; without the default,
     * those commands fail at parse time even when the cluster setting is true.
     *
     * <p>{@link Settings.Key#PPL_REX_MAX_MATCH_LIMIT} defaults to {@code 10} here because {@code
     * AstBuilder.visitRexCommand} reads it unconditionally and unboxes to {@code int} — a {@code
     * null} return from {@code getSettingValue} NPEs the planner before any operator-level
     * capability check runs. The value mirrors the cluster-side default of {@code 10} registered by
     * {@code OpenSearchSettings.PPL_REX_MAX_MATCH_LIMIT_SETTING}. Cluster-side overrides reach this
     * map via {@link #setting(String, Object)} — the REST handler reads the live value from {@code
     * OpenSearchSettings} and routes it through that existing API, keeping {@link
     * UnifiedQueryContext} decoupled from any specific {@link Settings} implementation.
     */
    private final Map<Settings.Key, Object> settings =
        new HashMap<Settings.Key, Object>(
            Map.of(
                QUERY_SIZE_LIMIT, SysLimit.DEFAULT.querySizeLimit(),
                PPL_SUBSEARCH_MAXOUT, SysLimit.UNLIMITED_SUBSEARCH.subsearchLimit(),
                PPL_JOIN_SUBSEARCH_MAXOUT, SysLimit.UNLIMITED_SUBSEARCH.joinSubsearchLimit(),
                CALCITE_ENGINE_ENABLED, true,
                PPL_REX_MAX_MATCH_LIMIT, 10));

    /**
     * Sets the query language frontend to be used.
     *
     * @param queryType the {@link QueryType}, such as PPL
     * @return this builder instance
     */
    public Builder language(QueryType queryType) {
      this.queryType = queryType;
      return this;
    }

    /**
     * Registers a catalog with the specified name and its associated schema. The schema can be a
     * flat or nested structure (e.g., catalog → schema → table), depending on how data is
     * organized.
     *
     * @param name the name of the catalog to register
     * @param schema the schema representing the structure of the catalog
     * @return this builder instance
     */
    public Builder catalog(String name, Schema schema) {
      catalogs.put(name, schema);
      return this;
    }

    /**
     * Sets the default namespace path for resolving unqualified table names.
     *
     * @param namespace dot-separated path (e.g., "spark_catalog.default" or "opensearch")
     * @return this builder instance
     */
    public Builder defaultNamespace(String namespace) {
      this.defaultNamespace = namespace;
      return this;
    }

    /**
     * Enables or disables catalog metadata caching in the root schema.
     *
     * @param cache whether to enable metadata caching
     * @return this builder instance
     */
    public Builder cacheMetadata(boolean cache) {
      this.cacheMetadata = cache;
      return this;
    }

    /**
     * Enables or disables query profiling. When enabled, profiling metrics are collected during
     * query planning and execution, retrievable via {@link UnifiedQueryContext#getProfile()}.
     *
     * @param enabled whether to enable profiling
     * @return this builder instance
     */
    public Builder profiling(boolean enabled) {
      this.profiling = enabled;
      return this;
    }

    /**
     * Sets a specific setting value by name.
     *
     * @param name the setting key name (e.g., "plugins.query.size_limit")
     * @param value the setting value
     * @throws IllegalArgumentException if the setting name is not recognized
     */
    public Builder setting(String name, Object value) {
      Settings.Key key =
          Settings.Key.of(name)
              .orElseThrow(() -> new IllegalArgumentException("Unknown setting name: " + name));
      settings.put(key, value);
      return this;
    }

    /**
     * Builds a {@link UnifiedQueryContext} with the configuration.
     *
     * @return a new instance of {@link UnifiedQueryContext}
     */
    public UnifiedQueryContext build() {
      Objects.requireNonNull(queryType, "Must specify language before build");

      LanguageSpec langSpec =
          switch (queryType) {
            case SQL -> UnifiedSqlSpec.extended();
            case PPL -> UnifiedPplSpec.create();
          };
      Settings settings = buildSettings();
      CalcitePlanContext planContext =
          CalcitePlanContext.create(
              buildFrameworkConfig(langSpec), SysLimit.fromSettings(settings), queryType);
      QueryProfiling.activate(profiling);
      return new UnifiedQueryContext(
          planContext, settings, createParser(planContext, settings), langSpec);
    }

    private UnifiedQueryParser<?> createParser(CalcitePlanContext planContext, Settings settings) {
      return switch (queryType) {
        case PPL -> new PPLQueryParser(settings);
        case SQL -> new SqlV2QueryParser();
      };
    }

    private Settings buildSettings() {
      return new Settings() {
        @Override
        @SuppressWarnings("unchecked")
        public <T> T getSettingValue(Key key) {
          return (T) settings.get(key);
        }

        @Override
        public List<?> getSettings() {
          return List.copyOf(settings.entrySet());
        }
      };
    }

    @SuppressWarnings({"rawtypes"})
    private FrameworkConfig buildFrameworkConfig(LanguageSpec langSpec) {
      SchemaPlus rootSchema = CalciteSchema.createRootSchema(true, cacheMetadata).plus();
      catalogs.forEach(rootSchema::add);

      SchemaPlus defaultSchema = findSchemaByPath(rootSchema, defaultNamespace);
      Frameworks.ConfigBuilder builder =
          Frameworks.newConfigBuilder()
              .defaultSchema(defaultSchema)
              .traitDefs((List<RelTraitDef>) null)
              .programs(Programs.calc(DefaultRelMetadataProvider.INSTANCE));

      return builder
          .parserConfig(langSpec.parserConfig())
          .sqlValidatorConfig(langSpec.validatorConfig())
          .operatorTable(langSpec.operatorTable())
          .build();
    }

    private SchemaPlus findSchemaByPath(SchemaPlus rootSchema, String defaultPath) {
      if (defaultPath == null) {
        return rootSchema;
      }

      SchemaPlus current = rootSchema;
      for (String part : defaultPath.split("\\.")) {
        current = current.getSubSchema(part);
        if (current == null) {
          throw new IllegalArgumentException("Invalid default catalog path: " + defaultPath);
        }
      }
      return current;
    }
  }
}
