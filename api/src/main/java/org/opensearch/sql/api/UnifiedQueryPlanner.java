/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Programs;
import org.opensearch.sql.ast.statement.Query;
import org.opensearch.sql.ast.statement.Statement;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.CalciteRelNodeVisitor;
import org.opensearch.sql.common.antlr.Parser;
import org.opensearch.sql.executor.OpenSearchTypeSystem;
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
  /** The type of query language being used (e.g., PPL). */
  private final QueryType queryType;

  /** The parser instance responsible for converting query text into a parse tree. */
  private final Parser parser;

  /** Calcite framework configuration used during logical plan construction. */
  private final FrameworkConfig config;

  /** AST-to-RelNode visitor that builds logical plans from the parsed AST. */
  private final CalciteRelNodeVisitor relNodeVisitor = new CalciteRelNodeVisitor();

  /**
   * Constructs a UnifiedQueryPlanner for a given query type and schema root.
   *
   * @param queryType the query language type (e.g., PPL)
   * @param rootSchema the root Calcite schema containing all catalogs and tables
   */
  public UnifiedQueryPlanner(QueryType queryType, SchemaPlus rootSchema) {
    this.queryType = queryType;
    this.parser = buildQueryParser(queryType);
    this.config = buildCalciteConfig(rootSchema);
  }

  /**
   * Parses and analyzes a query string into a Calcite logical plan (RelNode). TODO: Generate
   * optimal physical plan to fully unify query execution and leverage Calcite's optimzer.
   *
   * @param query the raw query string in PPL or other supported syntax
   * @return a logical plan representing the query
   */
  public RelNode plan(String query) {
    return preserveCollation(analyze(parse(query)));
  }

  private Parser buildQueryParser(QueryType queryType) {
    if (queryType == QueryType.PPL) {
      return new PPLSyntaxParser();
    }
    throw new IllegalArgumentException("Unsupported query type: " + queryType);
  }

  private FrameworkConfig buildCalciteConfig(SchemaPlus defaultSchema) {
    return Frameworks.newConfigBuilder()
        .parserConfig(SqlParser.Config.DEFAULT) // TODO check
        .defaultSchema(defaultSchema)
        .traitDefs((List<RelTraitDef>) null)
        .programs(Programs.calc(DefaultRelMetadataProvider.INSTANCE))
        .typeSystem(OpenSearchTypeSystem.INSTANCE)
        .build();
  }

  private UnresolvedPlan parse(String query) {
    ParseTree cst = parser.parse(query);
    AstStatementBuilder astStmtBuilder =
        new AstStatementBuilder(
            new AstBuilder(query), AstStatementBuilder.StatementBuilderContext.builder().build());
    Statement statement = cst.accept(astStmtBuilder);
    return ((Query) statement).getPlan();
  }

  private RelNode analyze(UnresolvedPlan ast) {
    // TODO: Hardcoded query size limit (10000) for now as only logical plan is generated.
    CalcitePlanContext calcitePlanContext = CalcitePlanContext.create(config, 10000, queryType);
    return relNodeVisitor.analyze(ast, calcitePlanContext);
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
   * Builder for {@link UnifiedQueryPlanner}, supporting both declarative and dynamic schema
   * registration for use in query planning.
   */
  public static class Builder {
    private final Map<String, Schema> catalogs = new HashMap<>();
    private QueryType queryType;
    private boolean cacheSchema;

    /**
     * Sets the query language frontend to be used by the planner.
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
     * Enables or disables schema caching in the root schema.
     *
     * @param cache whether to enable schema caching
     * @return this builder instance
     */
    public Builder cacheSchema(boolean cache) {
      this.cacheSchema = cache;
      return this;
    }

    /**
     * Builds a {@link UnifiedQueryPlanner} with the configuration.
     *
     * @return a new instance of {@link UnifiedQueryPlanner}
     */
    public UnifiedQueryPlanner build() {
      Objects.requireNonNull(queryType, "Must specify language before build");
      SchemaPlus rootSchema = CalciteSchema.createRootSchema(true, cacheSchema).plus();
      catalogs.forEach(rootSchema::add);
      return new UnifiedQueryPlanner(queryType, rootSchema);
    }
  }
}
