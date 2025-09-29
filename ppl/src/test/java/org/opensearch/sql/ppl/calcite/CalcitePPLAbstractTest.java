/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import static org.apache.calcite.test.Matchers.hasTree;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.opensearch.sql.executor.QueryType.PPL;

import java.io.PrintWriter;
import java.io.StringWriter;
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
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelRunners;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.opensearch.sql.ast.Node;
import org.opensearch.sql.ast.statement.Query;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.CalciteRelNodeVisitor;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.common.setting.Settings.Key;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.ppl.antlr.PPLSyntaxParser;
import org.opensearch.sql.ppl.parser.AstBuilder;
import org.opensearch.sql.ppl.parser.AstStatementBuilder;

public class CalcitePPLAbstractTest {
  @Getter private final Frameworks.ConfigBuilder config;
  private final CalciteRelNodeVisitor planTransformer;
  private final RelToSqlConverter converter;
  protected final Settings settings;
  private final DataSourceService dataSourceService;
  public PPLSyntaxParser pplParser = new PPLSyntaxParser();

  public CalcitePPLAbstractTest(CalciteAssert.SchemaSpec... schemaSpecs) {
    this.config = config(schemaSpecs);
    this.dataSourceService = mock(DataSourceService.class);
    this.planTransformer = new CalciteRelNodeVisitor(dataSourceService);
    this.converter = new RelToSqlConverter(OpenSearchSparkSqlDialect.DEFAULT);
    this.settings = mock(Settings.class);
  }

  @Before
  public void init() {
    doReturn(true).when(settings).getSettingValue(Settings.Key.CALCITE_ENGINE_ENABLED);
    doReturn(true).when(settings).getSettingValue(Settings.Key.CALCITE_SUPPORT_ALL_JOIN_TYPES);
    doReturn(true).when(settings).getSettingValue(Settings.Key.PPL_SYNTAX_LEGACY_PREFERRED);
    doReturn(false).when(dataSourceService).dataSourceExists(any());
  }

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
    return CalcitePlanContext.create(
        config.build(), settings.getSettingValue(Key.QUERY_SIZE_LIMIT), PPL);
  }

  /** Get the root RelNode of the given PPL query */
  public RelNode getRelNode(String ppl) {
    CalcitePlanContext context = createBuilderContext();
    Query query = (Query) plan(pplParser, ppl);
    planTransformer.analyze(query.getPlan(), context);
    RelNode root = context.relBuilder.build();
    System.out.println(root.explain());
    return root;
  }

  private Node plan(PPLSyntaxParser parser, String query) {
    final AstStatementBuilder builder =
        new AstStatementBuilder(
            new AstBuilder(query, settings),
            AstStatementBuilder.StatementBuilderContext.builder().build());
    return builder.visit(parser.parse(query));
  }

  /**
   * Fluent API for building count(eval) test cases. Provides a clean and readable way to define PPL
   * queries and their expected outcomes.
   */
  protected PPLQueryTestBuilder withPPLQuery(String ppl) {
    return new PPLQueryTestBuilder(ppl);
  }

  protected class PPLQueryTestBuilder {
    private final RelNode relNode;

    public PPLQueryTestBuilder(String ppl) {
      this.relNode = getRelNode(ppl);
    }

    public PPLQueryTestBuilder expectLogical(String expectedLogical) {
      verifyLogical(relNode, expectedLogical);
      return this;
    }

    public PPLQueryTestBuilder expectResult(String expectedResult) {
      verifyResult(relNode, expectedResult);
      return this;
    }

    public PPLQueryTestBuilder expectSparkSQL(String expectedSparkSql) {
      verifyPPLToSparkSQL(relNode, expectedSparkSql);
      return this;
    }
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
    String normalized = expected.replace("\n", System.lineSeparator());
    SqlImplementor.Result result = converter.visitRoot(rel);
    final SqlNode sqlNode = result.asStatement();
    final String sql = sqlNode.toSqlString(OpenSearchSparkSqlDialect.DEFAULT).getSql();
    assertThat(sql, is(normalized));
  }

  private static String getStackTrace(final Throwable throwable) {
    if (throwable == null) {
      return StringUtils.EMPTY;
    }
    final StringWriter sw = new StringWriter();
    throwable.printStackTrace(new PrintWriter(sw, true));
    return sw.toString();
  }

  public void verifyErrorMessageContains(Throwable t, String msg) {
    String stackTrace = getStackTrace(t);
    assertThat(String.format("Actual stack trace was:\n%s", stackTrace), stackTrace.contains(msg));
  }

  protected void verifyQueryThrowsException(String query, String expectedErrorMessage) {
    Exception e = Assert.assertThrows(ExpressionEvaluationException.class, () -> getRelNode(query));
    verifyErrorMessageContains(e, expectedErrorMessage);
  }
}
