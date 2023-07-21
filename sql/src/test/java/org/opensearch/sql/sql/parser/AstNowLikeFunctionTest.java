/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.sql.parser;

import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.opensearch.sql.ast.dsl.AstDSL.alias;
import static org.opensearch.sql.ast.dsl.AstDSL.filter;
import static org.opensearch.sql.ast.dsl.AstDSL.function;
import static org.opensearch.sql.ast.dsl.AstDSL.intLiteral;
import static org.opensearch.sql.ast.dsl.AstDSL.project;
import static org.opensearch.sql.ast.dsl.AstDSL.qualifiedName;
import static org.opensearch.sql.ast.dsl.AstDSL.relation;
import static org.opensearch.sql.ast.dsl.AstDSL.values;

import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.opensearch.sql.ast.expression.AllFields;

class AstNowLikeFunctionTest extends AstBuilderTestBase {

  private static Stream<Arguments> allFunctions() {
    return Stream.of("curdate",
        "current_date",
        "current_time",
        "current_timestamp",
        "curtime",
        "localtimestamp",
        "localtime",
        "now",
        "sysdate",
        "utc_date",
        "utc_time",
        "utc_timestamp")
        .map(Arguments::of);
  }

  private static Stream<Arguments> supportFsp() {
    return Stream.of("sysdate")
        .map(Arguments::of);
  }

  private static Stream<Arguments> supportShortcut() {
    return Stream.of("current_date",
            "current_time",
            "current_timestamp",
            "localtimestamp",
            "localtime")
        .map(Arguments::of);
  }

  @ParameterizedTest
  @MethodSource("allFunctions")
  void project_call(String name) {
    String call = name + "()";
    assertEquals(
        project(
            values(emptyList()),
            alias(call, function(name))
        ),
        buildAST("SELECT " + call)
    );
  }

  @ParameterizedTest
  @MethodSource("allFunctions")
  void filter_call(String name) {
    String call = name + "()";
    assertEquals(
        project(
            filter(
                relation("test"),
                function(
                    "=",
                    qualifiedName("data"),
                    function(name))
            ),
            AllFields.of()
        ),
        buildAST("SELECT * FROM test WHERE data = " + call)
    );
  }


  @ParameterizedTest
  @MethodSource("supportFsp")
  void fsp(String name) {
    assertEquals(
        project(
            values(emptyList()),
            alias(name + "(0)", function(name, intLiteral(0)))
        ),
        buildAST("SELECT " + name + "(0)")
    );
  }
}
