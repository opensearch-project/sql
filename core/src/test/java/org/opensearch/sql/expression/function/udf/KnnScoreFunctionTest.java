/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.TYPE_FACTORY;

import java.math.BigDecimal;
import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.calcite.knn.KnnErrors;
import org.opensearch.sql.data.type.VectorDataType;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.function.PPLBuiltinOperators;
import org.opensearch.sql.expression.function.PPLFuncImpTable;

class KnnScoreFunctionTest {

  private final RexBuilder rexBuilder = new RexBuilder(TYPE_FACTORY);

  private RexNode knnScoreCall() {
    RelDataType vectorType = TYPE_FACTORY.createVectorType(3, VectorDataType.FLOAT, "l2", true);
    RelDataType real = TYPE_FACTORY.createSqlType(SqlTypeName.REAL);
    RexNode query =
        rexBuilder.makeCall(
            SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR,
            rexBuilder.makeApproxLiteral(new BigDecimal("0.5"), real),
            rexBuilder.makeApproxLiteral(new BigDecimal("1.5"), real),
            rexBuilder.makeApproxLiteral(new BigDecimal("2.5"), real));
    RexNode k =
        rexBuilder.makeCall(
            SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
            rexBuilder.makeLiteral("k"),
            rexBuilder.makeExactLiteral(BigDecimal.TEN));
    return rexBuilder.makeCall(
        PPLBuiltinOperators.KNN_SCORE, rexBuilder.makeInputRef(vectorType, 0), query, k);
  }

  @Test
  void nameIsUpperCaseKnnScore() {
    assertEquals("KNN_SCORE", PPLBuiltinOperators.KNN_SCORE.getName());
  }

  @Test
  void returnsNullableReal() {
    RelDataType type = knnScoreCall().getType();
    assertEquals(SqlTypeName.REAL, type.getSqlTypeName());
    assertTrue(type.isNullable());
  }

  @Test
  void digestFollowsTheRelevanceOptionEncoding() {
    assertEquals(
        "KNN_SCORE($0, ARRAY(0.5E0:REAL, 1.5E0:REAL, 2.5E0:REAL), MAP('k', 10))",
        knnScoreCall().toString());
  }

  @Test
  void isNotResolvableByName() {
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> PPLFuncImpTable.INSTANCE.resolve(rexBuilder, "KNN_SCORE"));
    assertEquals("Unsupported function: KNN_SCORE", e.getMessage());
  }

  @Test
  void implementorFailsWhenNotPushedDown() {
    SemanticCheckException e =
        assertThrows(
            SemanticCheckException.class,
            () -> new KnnScoreFunction.KnnScoreImplementor().implement(null, null, List.of()));
    assertEquals(KnnErrors.NOT_PUSHED_DOWN_MESSAGE, e.getMessage());
  }
}
