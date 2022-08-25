/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.ppl.parser;

import static org.opensearch.sql.ast.dsl.AstDSL.qualifiedName;

import java.util.List;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.antlr.v4.runtime.tree.ParseTree;
import org.opensearch.sql.ast.tree.DataDefinitionPlan;
import org.opensearch.sql.ddl.Column;
import org.opensearch.sql.ddl.DataDefinitionTask;
import org.opensearch.sql.ddl.view.CreateMaterializedViewTask;
import org.opensearch.sql.ddl.view.RefreshMaterializedViewTask;
import org.opensearch.sql.ddl.view.ViewConfig;
import org.opensearch.sql.ddl.view.ViewDefinition;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParserBaseVisitor;

@RequiredArgsConstructor
public class AstDDLBuilder extends OpenSearchPPLParserBaseVisitor<DataDefinitionTask> {

  private final AstBuilder astBuilder;

  public DataDefinitionPlan build(ParseTree tree) {
    return new DataDefinitionPlan(tree.accept(this));
  }

  @Override
  public DataDefinitionTask visitCreateMaterializedView(
      OpenSearchPPLParser.CreateMaterializedViewContext ctx) {
    String viewName = ctx.tableName().getText();
    List<Column> columns = ctx.createDefinitions().createDefinition().stream()
        .map(def -> new Column(
            def.columnName().getText(),
            def.dataType().getText())
        ).collect(Collectors.toList());

    ViewDefinition definition = new ViewDefinition(viewName, columns, ViewDefinition.ViewType.MATERIALIZED_VIEW);
    definition.setQuery(astBuilder.visit(ctx.dmlStatement()));

    return new CreateMaterializedViewTask(
        definition,
        new ViewConfig(ViewConfig.RefreshMode.MANUAL, ViewConfig.DistributeOption.EVEN)
    );
  }

  @Override
  public DataDefinitionTask visitRefreshMaterializedView(
      OpenSearchPPLParser.RefreshMaterializedViewContext ctx) {
    String viewName = ctx.tableName().getText();

    return new RefreshMaterializedViewTask(qualifiedName(viewName));
  }

  @Override
  protected DataDefinitionTask aggregateResult(DataDefinitionTask aggregate,
                                               DataDefinitionTask nextResult) {
    return nextResult != null ? nextResult : aggregate;
  }
}
