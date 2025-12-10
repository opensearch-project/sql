/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api;

import static org.opensearch.sql.common.setting.Settings.Key.*;

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
import org.opensearch.sql.common.setting.Settings.Key;
import org.opensearch.sql.executor.QueryType;

/**
 * Represents a unified query context that encapsulates a CalcitePlanContext. Contexts are immutable
 * and thread-safe, designed to be reused across multiple queries.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * UnifiedQueryContext context = UnifiedQueryContext.builder()
 *     .queryType(QueryType.PPL)
 *     .catalog("opensearch", mySchema)
 *     .defaultNamespace("opensearch")
 *     .build();
 *
 * UnifiedQueryPlanner planner = new UnifiedQueryPlanner(context);
 * RelNode plan = planner.plan("source=logs | where status=200");
 * }</pre>
 */
@Value
public class UnifiedQueryContext {

  /** The CalcitePlanContext that holds all Calcite configuration and query type. */
  CalcitePlanContext planContext;

  /**
   * Settings for query execution configuration. Used by parsers to validate query features (e.g.,
   * join types).
   */
  Settings settings;

  /** Creates a new builder for UnifiedQueryContext. */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder for UnifiedQueryContext with validation. Builds the CalcitePlanContext with all
   * necessary configuration.
   */
  public static class Builder {
    private QueryType queryType;
    private final Map<String, Schema> catalogs = new HashMap<>();
    private String defaultNamespace;
    private boolean cacheMetadata = false;

    /**
     * Setting values with defaults from SysLimit.DEFAULT. Only includes planning-required settings
     * to avoid coupling with OpenSearchSettings.
     */
    private final Map<String, Object> settingValues =
        new HashMap<>(
            Map.of(
                QUERY_SIZE_LIMIT.getKeyValue(), SysLimit.DEFAULT.querySizeLimit(),
                PPL_SUBSEARCH_MAXOUT.getKeyValue(), SysLimit.DEFAULT.subsearchLimit(),
                PPL_JOIN_SUBSEARCH_MAXOUT.getKeyValue(), SysLimit.DEFAULT.joinSubsearchLimit()));

    /** Sets the query type. */
    public Builder queryType(QueryType queryType) {
      this.queryType = queryType;
      return this;
    }

    /** Registers a catalog with the specified name and schema. */
    public Builder catalog(String name, Schema schema) {
      catalogs.put(name, schema);
      return this;
    }

    /** Sets the default namespace path. */
    public Builder defaultNamespace(String namespace) {
      this.defaultNamespace = namespace;
      return this;
    }

    /** Enables or disables metadata caching. */
    public Builder cacheMetadata(boolean cache) {
      this.cacheMetadata = cache;
      return this;
    }

    /**
     * Sets a specific setting value by name.
     *
     * @param name the setting key name (e.g., "plugins.query.size_limit")
     * @param value the setting value
     */
    public Builder setting(String name, Object value) {
      settingValues.put(name, value);
      return this;
    }

    /**
     * Builds the UnifiedQueryContext with validation. Validates the default namespace path to fail
     * fast on invalid configuration.
     */
    public UnifiedQueryContext build() {
      Objects.requireNonNull(queryType, "QueryType must be specified");

      CalcitePlanContext planContext =
          CalcitePlanContext.create(
              buildFrameworkConfig(), SysLimit.fromSettings(buildSettings()), queryType);
      return new UnifiedQueryContext(planContext, buildSettings());
    }

    /** Builds Settings from the settingValues map (which includes defaults). */
    private Settings buildSettings() {
      final Map<Key, Object> settingsMap = new HashMap<>();
      settingValues.forEach(
          (name, value) -> {
            Key.of(name).ifPresent(key -> settingsMap.put(key, value));
          });

      return new Settings() {
        @Override
        @SuppressWarnings("unchecked")
        public <T> T getSettingValue(Key key) {
          return (T) settingsMap.get(key);
        }

        @Override
        public List<?> getSettings() {
          return List.copyOf(settingsMap.entrySet());
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

    @SuppressWarnings("deprecation")
    private static SchemaPlus findSchemaByPath(SchemaPlus rootSchema, String defaultPath) {
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
