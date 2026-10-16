/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.knn;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.opensearch.sql.ast.dsl.AstDSL.argument;
import static org.opensearch.sql.ast.dsl.AstDSL.doubleLiteral;
import static org.opensearch.sql.ast.dsl.AstDSL.intLiteral;
import static org.opensearch.sql.ast.dsl.AstDSL.longLiteral;
import static org.opensearch.sql.ast.dsl.AstDSL.stringLiteral;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ast.expression.Argument;
import org.opensearch.sql.exception.SemanticCheckException;

class KnnOptionsTest {

  private static String failure(Argument... options) {
    return assertThrows(SemanticCheckException.class, () -> KnnOptions.parse(List.of(options)))
        .getMessage();
  }

  @Test
  void parsesPhaseOneOptionsCaseInsensitively() {
    assertEquals(
        new KnnOptions(50, 100, 8),
        KnnOptions.parse(
            List.of(
                argument("K", intLiteral(50)),
                argument("ef_search", longLiteral(100L)),
                argument("NPROBES", intLiteral(8)))));
    assertEquals(KnnOptions.NONE, KnnOptions.parse(List.of()));
  }

  @Test
  void kMustBeAnIntegerBetweenOneAndTenThousand() {
    assertEquals(
        new KnnOptions(1, null, null), KnnOptions.parse(List.of(argument("k", intLiteral(1)))));
    assertEquals(
        new KnnOptions(10000, null, null),
        KnnOptions.parse(List.of(argument("k", intLiteral(10000)))));
    assertEquals("k must be between 1 and 10000, got [0]", failure(argument("k", intLiteral(0))));
    assertEquals(
        "k must be between 1 and 10000, got [10001]", failure(argument("k", intLiteral(10001))));
    assertEquals(
        "k must be between 1 and 10000, got [2.5]", failure(argument("k", doubleLiteral(2.5))));
    assertEquals(
        "k must be between 1 and 10000, got [ten]", failure(argument("k", stringLiteral("ten"))));
  }

  @Test
  void otherOptionsMustBePositiveIntegers() {
    assertEquals(
        "knn option [ef_search] must be a positive integer, got [0]",
        failure(argument("ef_search", intLiteral(0))));
    assertEquals(
        "knn option [nprobes] must be a positive integer, got [1.5]",
        failure(argument("nprobes", doubleLiteral(1.5))));
  }

  @Test
  void unknownAndDuplicateOptions() {
    assertEquals(
        "unknown knn option [max_distance]", failure(argument("max_distance", intLiteral(1))));
    assertEquals(
        "duplicate knn option [k]",
        failure(argument("k", intLiteral(1)), argument("K", intLiteral(2))));
  }
}
