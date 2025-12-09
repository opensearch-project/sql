/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.opensearch.sql.ast.statement.Query;
import org.opensearch.sql.ast.statement.Statement;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.calcite.CalciteRelNodeVisitor;
import org.opensearch.sql.common.antlr.Parser;
import org.opensearch.sql.common.antlr.SyntaxCheckException;
import org.opensearch.sql.executor.QueryType;
import org.opensearch.sql.ppl.antlr.PPLSyntaxParser;
import org.opensearch.sql.ppl.parser.AstBuilder;
import org.opensearch.sql.ppl.parser.AstStatementBuilder;

/**
 * {@code UnifiedQueryPlanner} provides a high-level API for parsing and analyzing queries using the
 * Calcite-based query engine. It serves as the primary integration point for external consumers
 * such as Spark or command-line tools, abstracting away Calcite internals.
 */
public class UnifiedQueryPlanner {
  /** The parser instance responsible for converting query text into a parse tree. */
  private final Parser parser;

  /** Unified query context containing CalcitePlanContext with all configuration. */
  private final UnifiedQueryContext context;

  /** AST-to-RelNode visitor that builds logical plans from the parsed AST. */
  private final CalciteRelNodeVisitor relNodeVisitor =
      new CalciteRelNodeVisitor(new EmptyDataSourceService());

  /**
   * Constructs a UnifiedQueryPlanner with a unified query context.
   * This is the recommended constructor for new code.
   *
   * @param context the unified query context containing CalcitePlanContext
   */
  public UnifiedQueryPlanner(UnifiedQueryContext context) {
    this.parser = buildQueryParser(context.getPlanContext().queryType);
    this.context = context;
  }

  /**
   * Constructs a UnifiedQueryPlanner for a given query type and schema root.
   * This constructor is maintained for backward compatibility.
   *
   * @param queryType the query language type (e.g., PPL)
   * @param rootSchema the root Calcite schema containing all catalogs and tables
   * @param defaultPath dot-separated path of schema to set as default schema
   * @deprecated Use {@link #UnifiedQueryPlanner(UnifiedQueryContext)} instead
   */
  @Deprecated
  public UnifiedQueryPlanner(QueryType queryType, SchemaPlus rootSchema, String defaultPath) {
    this.parser = buildQueryParser(queryType);
    this.context = createContextFromLegacyParams(queryType, rootSchema, defaultPath);
  }

  /**
   * Parses and analyzes a query string into a Calcite logical plan (RelNode). TODO: Generate
   * optimal physical plan to fully unify query execution and leverage Calcite's optimizer.
   *
   * @param query the raw query string in PPL or other supported syntax
   * @return a logical plan representing the query
   */
  public RelNode plan(String query) {
    try {
      return preserveCollation(analyze(parse(query)));
    } catch (SyntaxCheckException e) {
      // Re-throw syntax error without wrapping
      throw e;
    } catch (Exception e) {
      throw new IllegalStateException("Failed to plan query", e);
    }
  }

  private Parser buildQueryParser(QueryType queryType) {
    if (queryType == QueryType.PPL) {
      return new PPLSyntaxParser();
    }
    throw new IllegalArgumentException("Unsupported query type: " + queryType);
  }

  /**
   * Creates a UnifiedQueryContext from legacy constructor parameters.
   * Used internally to support backward compatibility.
   */
  @SuppressWarnings("deprecation")
  private static UnifiedQueryContext createContextFromLegacyParams(
      QueryType queryType, SchemaPlus rootSchema, String defaultPath) {
    // Extract catalogs from root schema
    Map<String, Schema> catalogs = new HashMap<>();
    for (String catalogName : rootSchema.getSubSchemaNames()) {
      Schema catalog = rootSchema.getSubSchema(catalogName);
      if (catalog != null) {
        catalogs.put(catalogName, catalog);
      }
    }

    // Build context with extracted catalogs
    UnifiedQueryContext.UnifiedQueryContextBuilder builder =
        UnifiedQueryContext.builder().queryType(queryType);
    catalogs.forEach(builder::catalog);

    if (defaultPath != null) {
      builder.defaultNamespace(defaultPath);
    }

    return builder.build();
  }

  private UnresolvedPlan parse(String query) {
    ParseTree cst = parser.parse(query);
    AstStatementBuilder astStmtBuilder =
        new AstStatementBuilder(
            new AstBuilder(query), AstStatementBuilder.StatementBuilderContext.builder().build());
    Statement statement = cst.accept(astStmtBuilder);

    if (statement instanceof Query) {
      return ((Query) statement).getPlan();
    }
    throw new UnsupportedOperationException(
        "Only query statements are supported but got " + statement.getClass().getSimpleName());
  }

  private RelNode analyze(UnresolvedPlan ast) {
    return relNodeVisitor.analyze(ast, context.getPlanContext());
  }

  private RelNode preserveCollation(RelNode logical) {
    RelNode calcitePlan = logical;
    RelCollation collation = logical.getTraitSet().getCollation();
    if (!(logical instanceof Sort) && collation != RelCollations.EMPTY) {
      calcitePlan = LogicalSort.create(logical, collation, null, null);
    }
    return calcitePlan;
  }

  /** Builder for {@link UnifiedQueryPlanner}, supporting declarative fluent API. */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder for {@link UnifiedQueryPlanner}, supporting both new context-based API
   * and legacy catalog-based API for backward compatibility.
   * Delegates all configuration to UnifiedQueryContext.Builder.
   */
  public static class Builder {
    private UnifiedQueryContext context;
    private UnifiedQueryContext.UnifiedQueryContextBuilder contextBuilder;

    /**
     * Sets the query language frontend to be used by the planner.
     * Delegates to context builder.
     *
     * @param queryType the {@link QueryType}, such as PPL
     * @return this builder instance
     */
    public Builder language(QueryType queryType) {
      ensureContextBuilder();
      contextBuilder.queryType(queryType);
      return this;
    }

    /**
     * Sets the unified query context directly (new API).
     *
     * @param context the unified query context
     * @return this builder instance
     */
    public Builder context(UnifiedQueryContext context) {
      if (contextBuilder != null) {
        throw new IllegalStateException(
            "Cannot set context after using language/catalog/defaultNamespace/cacheMetadata methods");
      }
      this.context = context;
      return this;
    }

    /**
     * Registers a catalog with the specified name and its associated schema.
     * Delegates to context builder.
     *
     * @param name the name of the catalog to register
     * @param schema the schema representing the structure of the catalog
     * @return this builder instance
     */
    public Builder catalog(String name, Schema schema) {
      ensureContextBuilder();
      contextBuilder.catalog(name, schema);
      return this;
    }

    /**
     * Sets the default namespace path for resolving unqualified table names.
     * Delegates to context builder.
     *
     * @param namespace dot-separated path (e.g., "spark_catalog.default" or "opensearch")
     * @return this builder instance
     */
    public Builder defaultNamespace(String namespace) {
      ensureContextBuilder();
      contextBuilder.defaultNamespace(namespace);
      return this;
    }

    /**
     * Enables or disables catalog metadata caching in the root schema.
     * Delegates to context builder.
     *
     * @param cache whether to enable metadata caching
     * @return this builder instance
     */
    public Builder cacheMetadata(boolean cache) {
      ensureContextBuilder();
      contextBuilder.cacheMetadata(cache);
      return this;
    }

    /**
     * Builds a {@link UnifiedQueryPlanner} with the configuration.
     *
     * @return a new instance of {@link UnifiedQueryPlanner}
     */
    public UnifiedQueryPlanner build() {
      // Build context if not provided directly
      if (context == null) {
        if (contextBuilder == null) {
          throw new IllegalStateException(
              "Must provide either context or use language/catalog configuration");
        }
        context = contextBuilder.build();
      }

      return new UnifiedQueryPlanner(context);
    }

    private void ensureContextBuilder() {
      if (context != null) {
        throw new IllegalStateException(
            "Cannot use language/catalog/defaultNamespace/cacheMetadata after setting context");
      }
      if (contextBuilder == null) {
        contextBuilder = UnifiedQueryContext.builder();
      }
    }
  }
}
