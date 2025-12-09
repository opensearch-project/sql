/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

  /**
   * The CalcitePlanContext that holds all Calcite configuration and query type. This is the only
   * field stored - everything else is just for building this.
   */
  CalcitePlanContext planContext;

  /** Creates a new builder for UnifiedQueryContext. */
  public static UnifiedQueryContextBuilder builder() {
    return new UnifiedQueryContextBuilder();
  }

  /**
   * Builder for UnifiedQueryContext with validation. Builds the CalcitePlanContext with all
   * necessary configuration.
   */
  public static class UnifiedQueryContextBuilder {
    private QueryType queryType;
    private final Map<String, Schema> catalogs = new HashMap<>();
    private String defaultNamespace;
    private boolean cacheMetadata = false;
    private SysLimit sysLimit = new SysLimit(10000, 10000, 10000);

    /** Sets the query type. */
    public UnifiedQueryContextBuilder queryType(QueryType queryType) {
      this.queryType = queryType;
      return this;
    }

    /** Registers a catalog with the specified name and schema. */
    public UnifiedQueryContextBuilder catalog(String name, Schema schema) {
      catalogs.put(name, schema);
      return this;
    }

    /** Sets the default namespace path. */
    public UnifiedQueryContextBuilder defaultNamespace(String namespace) {
      this.defaultNamespace = namespace;
      return this;
    }

    /** Enables or disables metadata caching. */
    public UnifiedQueryContextBuilder cacheMetadata(boolean cache) {
      this.cacheMetadata = cache;
      return this;
    }

    /** Sets the query execution limits. */
    public UnifiedQueryContextBuilder sysLimit(SysLimit limit) {
      this.sysLimit = limit;
      return this;
    }

    /**
     * Builds the UnifiedQueryContext with validation. Validates the default namespace path to fail
     * fast on invalid configuration.
     */
    public UnifiedQueryContext build() {
      if (queryType == null) {
        throw new IllegalArgumentException("QueryType must be specified");
      }

      // Build and validate framework config
      FrameworkConfig frameworkConfig = buildFrameworkConfig();

      // Create CalcitePlanContext with all configuration
      CalcitePlanContext planContext =
          CalcitePlanContext.create(frameworkConfig, sysLimit, queryType);

      return new UnifiedQueryContext(planContext);
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
