/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql.parser;

import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.opensearch.sql.ast.dsl.AstDSL.alias;
import static org.opensearch.sql.ast.dsl.AstDSL.intLiteral;
import static org.opensearch.sql.ast.dsl.AstDSL.project;
import static org.opensearch.sql.ast.dsl.AstDSL.values;
import static org.opensearch.sql.executor.QueryType.SQL;

import org.junit.jupiter.api.Test;
import org.opensearch.sql.ast.statement.Query;
import org.opensearch.sql.ast.statement.Statement;
import org.opensearch.sql.sql.antlr.SQLSyntaxParser;

class AstStatementBuilderTest {

  private final SQLSyntaxParser parser = new SQLSyntaxParser();

  @Test
  public void queryWithoutStatementShouldFail() {
    for (String query : new String[] {"", " \n\t ", "/* only a comment */", "-- line", ";"}) {
      IllegalArgumentException e =
          assertThrows(IllegalArgumentException.class, () -> buildStatement(query));
      assertEquals("Query is empty", e.getMessage(), "[" + query + "]");
    }
  }

  @Test
  public void canBuildStatementWithSurroundingCommentsAndSemiColon() {
    assertEquals(
        new Query(project(values(emptyList()), alias("123", intLiteral(123))), 0, SQL),
        buildStatement("/* leading */ SELECT 123 -- trailing\n;"));
  }

  private Statement buildStatement(String query) {
    return parser
        .parse(query)
        .accept(
            new AstStatementBuilder(
                new AstBuilder(query),
                AstStatementBuilder.StatementBuilderContext.builder().build()));
  }
}
