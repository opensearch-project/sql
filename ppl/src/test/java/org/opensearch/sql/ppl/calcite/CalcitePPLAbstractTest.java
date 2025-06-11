/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import static org.apache.calcite.test.Matchers.hasTree;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.function.UnaryOperator;
import lombok.Getter;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.rel2sql.SqlImplementor;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.SparkSqlDialect;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelRunners;
import org.opensearch.sql.ast.Node;
import org.opensearch.sql.ast.statement.Query;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.CalciteRelNodeVisitor;
import org.opensearch.sql.ppl.antlr.PPLSyntaxParser;
import org.opensearch.sql.ppl.parser.AstBuilder;
import org.opensearch.sql.ppl.parser.AstStatementBuilder;

public class CalcitePPLAbstractTest {
  @Getter private final Frameworks.ConfigBuilder config;
  @Getter private final CalcitePlanContext context;
  private final CalciteRelNodeVisitor planTransformer;
  private final RelToSqlConverter converter;

  public CalcitePPLAbstractTest(CalciteAssert.SchemaSpec... schemaSpecs) {
    this.config = config(schemaSpecs);
    this.context = createBuilderContext();
    this.planTransformer = new CalciteRelNodeVisitor();
    this.converter = new RelToSqlConverter(SparkSqlDialect.DEFAULT);
  }

  public PPLSyntaxParser pplParser = new PPLSyntaxParser();

  protected Frameworks.ConfigBuilder config(CalciteAssert.SchemaSpec... schemaSpecs) {
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    final SchemaPlus schema = CalciteAssert.addSchema(rootSchema, schemaSpecs);
    return Frameworks.newConfigBuilder()
        .parserConfig(SqlParser.Config.DEFAULT)
        .defaultSchema(schema)
        .traitDefs((List<RelTraitDef>) null)
        .programs(Programs.heuristicJoinOrder(Programs.RULE_SET, true, 2));
  }

  /** Creates a RelBuilder with default config. */
  protected CalcitePlanContext createBuilderContext() {
    return createBuilderContext(c -> c);
  }

  /** Creates a CalcitePlanContext with transformed config. */
  private CalcitePlanContext createBuilderContext(UnaryOperator<RelBuilder.Config> transform) {
    config.context(Contexts.of(transform.apply(RelBuilder.Config.DEFAULT)));
    return CalcitePlanContext.create(config.build());
  }

  /** Get the root RelNode of the given PPL query */
  public RelNode getRelNode(String ppl) {
    Query query = (Query) plan(pplParser, ppl);
    planTransformer.analyze(query.getPlan(), context);
    RelNode root = context.relBuilder.build();
    System.out.println(root.explain());
    return root;
  }

  private Node plan(PPLSyntaxParser parser, String query) {
    final AstStatementBuilder builder =
        new AstStatementBuilder(
            new AstBuilder(query), AstStatementBuilder.StatementBuilderContext.builder().build());
    return builder.visit(parser.parse(query));
  }

  /** Verify the logical plan of the given RelNode */
  public void verifyLogical(RelNode rel, String expectedLogical) {
    assertThat(rel, hasTree(expectedLogical));
  }

  /** Execute and verify the result of the given RelNode */
  public void verifyResult(RelNode rel, String expectedResult) {
    try (PreparedStatement preparedStatement = RelRunners.run(rel)) {
      String s = CalciteAssert.toString(preparedStatement.executeQuery());
      assertThat(s, is(expectedResult));
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  /** Execute and verify the result count of the given RelNode */
  public void verifyResultCount(RelNode rel, int expectedRows) {
    try (PreparedStatement preparedStatement = RelRunners.run(rel)) {
      CalciteAssert.checkResultCount(is(expectedRows)).accept(preparedStatement.executeQuery());
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  /** Verify the generated Spark SQL of the given RelNode */
  public void verifyPPLToSparkSQL(RelNode rel, String expected) {
    SqlImplementor.Result result = converter.visitRoot(rel);
    final SqlNode sqlNode = result.asStatement();
    final String sql = sqlNode.toSqlString(SparkSqlDialect.DEFAULT).getSql();
    assertThat(sql, is(expected));
  }
}
