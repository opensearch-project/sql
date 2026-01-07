/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api;

import static org.opensearch.sql.common.setting.Settings.Key.PPL_JOIN_SUBSEARCH_MAXOUT;
import static org.opensearch.sql.common.setting.Settings.Key.PPL_SUBSEARCH_MAXOUT;
import static org.opensearch.sql.common.setting.Settings.Key.QUERY_SIZE_LIMIT;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import lombok.Value;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Programs;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.SysLimit;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.executor.QueryType;

/**
 * A reusable abstraction shared across unified query components (planner, compiler, etc.). This
 * centralizes configuration for catalog schemas, query type, execution limits, and other settings,
 * enabling consistent behavior across all unified query operations.
 */
@Value
public class UnifiedQueryContext implements AutoCloseable {

  /** CalcitePlanContext containing Calcite framework configuration and query type. */
  CalcitePlanContext planContext;

  /** Settings containing execution limits and feature flags used by parsers and planners. */
  Settings settings;

  /**
   * Closes the underlying resource managed by this context.
   *
   * @throws Exception if an error occurs while closing the connection
   */
  @Override
  public void close() throws Exception {
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

    /**
     * Setting values with defaults from SysLimit.DEFAULT. Only includes planning-required settings
     * to avoid coupling with OpenSearchSettings.
     */
    private final Map<Settings.Key, Object> settings =
        new HashMap<Settings.Key, Object>(
            Map.of(
                QUERY_SIZE_LIMIT, SysLimit.DEFAULT.querySizeLimit(),
                PPL_SUBSEARCH_MAXOUT, SysLimit.DEFAULT.subsearchLimit(),
                PPL_JOIN_SUBSEARCH_MAXOUT, SysLimit.DEFAULT.joinSubsearchLimit()));

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

      Settings settings = buildSettings();
      CalcitePlanContext planContext =
          CalcitePlanContext.create(
              buildFrameworkConfig(), SysLimit.fromSettings(settings), queryType);
      return new UnifiedQueryContext(planContext, settings);
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
    private FrameworkConfig buildFrameworkConfig() {
      SchemaPlus rootSchema = CalciteSchema.createRootSchema(true, cacheMetadata).plus();
      catalogs.forEach(rootSchema::add);

      SchemaPlus defaultSchema = findSchemaByPath(rootSchema, defaultNamespace);
      return Frameworks.newConfigBuilder()
          .parserConfig(SqlParser.Config.DEFAULT)
          .defaultSchema(defaultSchema)
          .traitDefs((List<RelTraitDef>) null)
          .programs(Programs.calc(DefaultRelMetadataProvider.INSTANCE))
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
