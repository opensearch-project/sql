/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql.parser;

import static org.opensearch.sql.ddl.view.ViewConfig.DistributeOption;
import static org.opensearch.sql.ddl.view.ViewConfig.RefreshMode;
import static org.opensearch.sql.ddl.view.ViewDefinition.ViewType;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.CreateMaterializedViewContext;

import java.util.List;
import java.util.stream.Collectors;

import lombok.RequiredArgsConstructor;
import org.antlr.v4.runtime.tree.ParseTree;
import org.opensearch.sql.ast.tree.DataDefinitionPlan;
import org.opensearch.sql.ddl.Column;
import org.opensearch.sql.ddl.DataDefinitionTask;
import org.opensearch.sql.ddl.view.CreateMaterializedViewTask;
import org.opensearch.sql.ddl.view.ViewConfig;
import org.opensearch.sql.ddl.view.ViewDefinition;
import org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParserBaseVisitor;


/**
 * Build AST for data definition plan.
 */
@RequiredArgsConstructor
public class AstDDLBuilder extends OpenSearchSQLParserBaseVisitor<DataDefinitionTask> {

  private final AstBuilder astBuilder;

  public DataDefinitionPlan build(ParseTree tree) {
    return new DataDefinitionPlan(tree.accept(this));
  }

  @Override
  public DataDefinitionTask visitCreateMaterializedView(CreateMaterializedViewContext ctx) {
    String viewName = ctx.tableName().getText();
    List<Column> columns = ctx.createDefinitions().createDefinition().stream()
        .map(def -> new Column(
            def.columnName().getText(),
            def.dataType().getText())
        ).collect(Collectors.toList());

    ViewDefinition definition = new ViewDefinition(viewName, columns, ViewType.MATERIALIZED_VIEW);
    definition.setQuery(astBuilder.visit(ctx.selectStatement()));

    return new CreateMaterializedViewTask(
        definition,
        new ViewConfig(RefreshMode.MANUAL, DistributeOption.EVEN)
    );
  }

  @Override
  protected DataDefinitionTask aggregateResult(DataDefinitionTask aggregate,
                                           DataDefinitionTask nextResult) {
    return nextResult != null ? nextResult : aggregate;
  }
}
