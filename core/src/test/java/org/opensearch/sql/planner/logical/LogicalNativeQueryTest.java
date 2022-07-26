package org.opensearch.sql.planner.logical;

import static org.opensearch.sql.ast.dsl.AstDSL.argument;
import static org.opensearch.sql.ast.dsl.AstDSL.stringLiteral;
import static org.opensearch.sql.constants.TestConstants.DUMMY_CATALOG;
import static org.opensearch.sql.constants.TestConstants.QUERY_ARG_NAME;
import static org.opensearch.sql.constants.TestConstants.QUERY_ARG_VALUE;

import java.util.Collections;
import java.util.HashMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.opensearch.sql.analysis.AnalyzerTestBase;
import org.opensearch.sql.ast.dsl.AstDSL;
import org.opensearch.sql.ast.expression.DataType;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.expression.config.ExpressionConfig;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

@Configuration
@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = {ExpressionConfig.class, AnalyzerTestBase.class})
class LogicalNativeQueryTest extends AnalyzerTestBase {


  @Test
  public void analyze_plan_with_native_query() {
    assertAnalyzeEqual(
        LogicalPlanDSL.nativeQuery(DUMMY_CATALOG,
            new HashMap<>() {{
              put(QUERY_ARG_NAME, new Literal(QUERY_ARG_VALUE, DataType.STRING));
            }
            }),
        AstDSL.nativeQuery(DUMMY_CATALOG,
            Collections.singletonList(argument("query", stringLiteral(QUERY_ARG_VALUE))
            )));
  }

}
