/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.planner.logical;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.opensearch.sql.data.type.ExprCoreType.BOOLEAN;
import static org.opensearch.sql.data.type.ExprCoreType.BYTE;
import static org.opensearch.sql.data.type.ExprCoreType.DATE;
import static org.opensearch.sql.data.type.ExprCoreType.DATETIME;
import static org.opensearch.sql.data.type.ExprCoreType.DOUBLE;
import static org.opensearch.sql.data.type.ExprCoreType.FLOAT;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.LONG;
import static org.opensearch.sql.data.type.ExprCoreType.SHORT;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.data.type.ExprCoreType.TIME;
import static org.opensearch.sql.data.type.ExprCoreType.TIMESTAMP;

import java.util.Arrays;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.analysis.AnalyzerTestBase;
import org.opensearch.sql.ast.dsl.AstDSL;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.config.ExpressionConfig;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

@Configuration
@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = {ExpressionConfig.class, AnalyzerTestBase.class})
@ExtendWith(MockitoExtension.class)
public class LogicalParseTest extends AnalyzerTestBase {

  @Test
  public void parse_relation() {
    assertAnalyzeEqual(
        LogicalPlanDSL.parse(
            LogicalPlanDSL.relation("schema"),
            dsl.equal(DSL.ref("string_value", STRING), DSL.literal("raw")), "(?<pattern>.*)"),
        AstDSL.parse(
            AstDSL.relation("schema"),
            AstDSL.equalTo(AstDSL.field("string_value"), AstDSL.stringLiteral("raw")),
            AstDSL.stringLiteral("(?<pattern>.*)")));
  }

  @Test
  public void parse_named_groups() {
    String pattern =
        "\\b[(?<dateTIMESTAMP>[^\\]])]\\s(?<city>[A-Za-z\\s]+),\\s(?<stateSTRING>[A-Z]{2,2}):\\s"
            + "(?<areaCodeSHORT>[0-9]{3,3})\\s(?<countINTEGER>[0-9]+)\\b";
    LogicalParse logicalParse = new LogicalParse(LogicalPlanDSL.relation("schema"),
        dsl.equal(DSL.ref("string_value", STRING), DSL.literal("raw")), pattern);
    Map<String, String> groups = logicalParse.getGroups();
    assertThat(groups, hasEntry("date", "TIMESTAMP"));
    assertThat(groups, hasEntry("city", ""));
    assertThat(groups, hasEntry("state", "STRING"));
    assertThat(groups, hasEntry("areaCode", "SHORT"));
    assertThat(groups, hasEntry("count", "INTEGER"));
  }

  @Test
  public void convert_type_string_to_ExprType() {
    assertEquals(LogicalParse.typeStrToExprType(""), STRING);
    Arrays.asList(STRING, BYTE, SHORT, INTEGER, LONG, FLOAT, DOUBLE, BOOLEAN, TIME, DATE, TIMESTAMP,
            DATETIME).forEach(exprType -> {
              assertEquals(LogicalParse.typeStrToExprType(exprType.typeName()), exprType);
            });
  }
}
