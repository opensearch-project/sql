/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.knn;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.opensearch.sql.ast.dsl.AstDSL.argument;
import static org.opensearch.sql.ast.dsl.AstDSL.doubleLiteral;
import static org.opensearch.sql.ast.dsl.AstDSL.field;
import static org.opensearch.sql.ast.dsl.AstDSL.function;
import static org.opensearch.sql.ast.dsl.AstDSL.intLiteral;
import static org.opensearch.sql.ast.dsl.AstDSL.knnFunction;
import static org.opensearch.sql.ast.dsl.AstDSL.stringLiteral;
import static org.opensearch.sql.ast.dsl.AstDSL.unresolvedArg;
import static org.opensearch.sql.ast.dsl.AstDSL.vectorLiteral;

import java.math.BigDecimal;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ast.dsl.AstDSL;
import org.opensearch.sql.ast.expression.Argument;
import org.opensearch.sql.exception.SemanticCheckException;

class KnnArgumentsTest {

  @Test
  void bothFormsGiveTheSameArguments() {
    KnnArguments fromCommand =
        KnnArguments.of(
            AstDSL.knn(
                AstDSL.relation("t"),
                field("embedding"),
                vectorLiteral(new BigDecimal("0.5"), 1, -2.0),
                argument("k", intLiteral(5))));
    KnnArguments fromFunction =
        KnnArguments.of(
            knnFunction(
                field("embedding"),
                vectorLiteral(new BigDecimal("0.5"), 1, -2.0),
                unresolvedArg("k", intLiteral(5))));
    assertEquals(fromCommand, fromFunction);
    assertEquals("embedding", fromFunction.fieldName());
    assertEquals(List.of(new BigDecimal("0.5"), 1, -2.0), fromFunction.vectorElements());
    assertEquals(new KnnOptions(5, null, null), fromFunction.parsedOptions());
  }

  @Test
  void functionNeedsAFieldAndAVector() {
    SemanticCheckException tooFew =
        assertThrows(
            SemanticCheckException.class,
            () -> KnnArguments.of(function("knn", field("embedding"))));
    assertEquals(
        "knn requires a knn_vector field and a query vector, e.g. knn(<field>, [<v1>, <v2>, ...],"
            + " k=<n>), got [knn(Field(field=embedding, fieldArgs=[]))]",
        tooFew.getMessage());
    SemanticCheckException notAField =
        assertThrows(
            SemanticCheckException.class,
            () -> KnnArguments.of(function("knn", stringLiteral("e"), vectorLiteral(1))));
    assertEquals("knn field [e] is not a knn_vector field", notAField.getMessage());
  }

  @Test
  void functionOptionsMustBeNamedLiterals() {
    SemanticCheckException positional =
        assertThrows(
            SemanticCheckException.class,
            () -> KnnArguments.of(function("knn", field("e"), vectorLiteral(1), intLiteral(10))));
    assertEquals("knn options must be given as name=value, got [10]", positional.getMessage());
    SemanticCheckException computed =
        assertThrows(
            SemanticCheckException.class,
            () ->
                KnnArguments.of(
                    knnFunction(
                        field("e"),
                        vectorLiteral(1),
                        unresolvedArg("k", function("+", intLiteral(1), intLiteral(2))))));
    assertEquals("knn option [k] must be a positive integer, got [+(1, 2)]", computed.getMessage());
  }

  @Test
  void vectorMustBeANonEmptyArrayOfNumericLiterals() {
    for (var vector :
        List.of(
            function("array"),
            function("array", stringLiteral("a")),
            function("array", field("x")),
            function("abs", doubleLiteral(1.0)),
            field("other"))) {
      KnnArguments arguments = new KnnArguments(field("e"), vector, List.<Argument>of());
      SemanticCheckException e =
          assertThrows(SemanticCheckException.class, arguments::vectorElements);
      assertEquals(
          "knn vector must be a non-empty array of numeric literals, got [" + vector + "]",
          e.getMessage());
    }
  }
}
