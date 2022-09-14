package org.opensearch.sql.planner.logical;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.opensearch.sql.ast.dsl.AstDSL.stringLiteral;
import static org.opensearch.sql.ast.dsl.AstDSL.unresolvedArg;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.opensearch.sql.analysis.AnalyzerTestBase;
import org.opensearch.sql.ast.dsl.AstDSL;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.config.ExpressionConfig;
import org.opensearch.sql.storage.Table;
import org.springframework.context.annotation.Configuration;
import org.springframework.expression.spel.ast.StringLiteral;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

@Configuration
@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = {ExpressionConfig.class, AnalyzerTestBase.class})
public class LogicalTableFunctionTest extends AnalyzerTestBase {
  @Mock
  Table table;

  @Test
  public void logicalTableFunction() {
    LogicalPlan tableFunction = LogicalPlanDSL.tableFunction(dsl.query_range_function(
        dsl.namedArgument("query", DSL.literal("http_latency")),
        dsl.namedArgument("starttime", DSL.literal(12345)),
        dsl.namedArgument("endtime", DSL.literal(12345)),
        dsl.namedArgument("step", DSL.literal(14))), table);
    assertNull(tableFunction.accept(new LogicalPlanNodeVisitor<Integer, Object>() {
    }, null));
    assertEquals(0, tableFunction.getChild().size());
  }


}
