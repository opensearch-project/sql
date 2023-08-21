/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.antlr.semantic.types.function;

import static org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType.BOOLEAN;
import static org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType.DOUBLE;
import static org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType.INTEGER;
import static org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType.NUMBER;
import static org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType.OPENSEARCH_TYPE;
import static org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType.STRING;
import static org.opensearch.sql.legacy.antlr.semantic.types.special.Generic.T;

import org.opensearch.sql.legacy.antlr.semantic.types.Type;
import org.opensearch.sql.legacy.antlr.semantic.types.TypeExpression;
import org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType;

/** Scalar SQL function */
public enum ScalarFunction implements TypeExpression {
  ABS(func(T(NUMBER)).to(T)), // translate to Java: <T extends Number> T ABS(T)
  ACOS(func(T(NUMBER)).to(DOUBLE)),
  ADD(func(T(NUMBER), NUMBER).to(T)),
  ASCII(func(T(STRING)).to(INTEGER)),
  ASIN(func(T(NUMBER)).to(DOUBLE)),
  ATAN(func(T(NUMBER)).to(DOUBLE)),
  ATAN2(func(T(NUMBER), NUMBER).to(DOUBLE)),
  CAST(),
  CBRT(func(T(NUMBER)).to(T)),
  CEIL(func(T(NUMBER)).to(T)),
  CONCAT(), // TODO: varargs support required
  CONCAT_WS(),
  COS(func(T(NUMBER)).to(DOUBLE)),
  COSH(func(T(NUMBER)).to(DOUBLE)),
  COT(func(T(NUMBER)).to(DOUBLE)),
  CURDATE(func().to(OpenSearchDataType.DATE)),
  DATE(func(OpenSearchDataType.DATE).to(OpenSearchDataType.DATE)),
  DATE_FORMAT(
      func(OpenSearchDataType.DATE, STRING).to(STRING),
      func(OpenSearchDataType.DATE, STRING, STRING).to(STRING)),
  DAYOFMONTH(func(OpenSearchDataType.DATE).to(INTEGER)),
  DEGREES(func(T(NUMBER)).to(DOUBLE)),
  DIVIDE(func(T(NUMBER), NUMBER).to(T)),
  E(func().to(DOUBLE)),
  EXP(func(T(NUMBER)).to(T)),
  EXPM1(func(T(NUMBER)).to(T)),
  FLOOR(func(T(NUMBER)).to(T)),
  IF(func(BOOLEAN, OPENSEARCH_TYPE, OPENSEARCH_TYPE).to(OPENSEARCH_TYPE)),
  IFNULL(func(OPENSEARCH_TYPE, OPENSEARCH_TYPE).to(OPENSEARCH_TYPE)),
  ISNULL(func(OPENSEARCH_TYPE).to(INTEGER)),
  LEFT(func(T(STRING), INTEGER).to(T)),
  LENGTH(func(STRING).to(INTEGER)),
  LN(func(T(NUMBER)).to(DOUBLE)),
  LOCATE(func(STRING, STRING, INTEGER).to(INTEGER), func(STRING, STRING).to(INTEGER)),
  LOG(func(T(NUMBER)).to(DOUBLE), func(T(NUMBER), NUMBER).to(DOUBLE)),
  LOG2(func(T(NUMBER)).to(DOUBLE)),
  LOG10(func(T(NUMBER)).to(DOUBLE)),
  LOWER(func(T(STRING)).to(T), func(T(STRING), STRING).to(T)),
  LTRIM(func(T(STRING)).to(T)),
  MAKETIME(func(INTEGER, INTEGER, INTEGER).to(OpenSearchDataType.DATE)),
  MODULUS(func(T(NUMBER), NUMBER).to(T)),
  MONTH(func(OpenSearchDataType.DATE).to(INTEGER)),
  MONTHNAME(func(OpenSearchDataType.DATE).to(STRING)),
  MULTIPLY(func(T(NUMBER), NUMBER).to(NUMBER)),
  NOW(func().to(OpenSearchDataType.DATE)),
  PI(func().to(DOUBLE)),
  POW(func(T(NUMBER)).to(T), func(T(NUMBER), NUMBER).to(T)),
  POWER(func(T(NUMBER)).to(T), func(T(NUMBER), NUMBER).to(T)),
  RADIANS(func(T(NUMBER)).to(DOUBLE)),
  RAND(func().to(NUMBER), func(T(NUMBER)).to(T)),
  REPLACE(func(T(STRING), STRING, STRING).to(T)),
  RIGHT(func(T(STRING), INTEGER).to(T)),
  RINT(func(T(NUMBER)).to(T)),
  ROUND(func(T(NUMBER)).to(T)),
  RTRIM(func(T(STRING)).to(T)),
  SIGN(func(T(NUMBER)).to(T)),
  SIGNUM(func(T(NUMBER)).to(T)),
  SIN(func(T(NUMBER)).to(DOUBLE)),
  SINH(func(T(NUMBER)).to(DOUBLE)),
  SQRT(func(T(NUMBER)).to(T)),
  SUBSTRING(func(T(STRING), INTEGER, INTEGER).to(T)),
  SUBTRACT(func(T(NUMBER), NUMBER).to(T)),
  TAN(func(T(NUMBER)).to(DOUBLE)),
  TIMESTAMP(func(OpenSearchDataType.DATE).to(OpenSearchDataType.DATE)),
  TRIM(func(T(STRING)).to(T)),
  UPPER(func(T(STRING)).to(T), func(T(STRING), STRING).to(T)),
  YEAR(func(OpenSearchDataType.DATE).to(INTEGER));

  private final TypeExpressionSpec[] specifications;

  ScalarFunction(TypeExpressionSpec... specifications) {
    this.specifications = specifications;
  }

  @Override
  public String getName() {
    return name();
  }

  @Override
  public TypeExpressionSpec[] specifications() {
    return specifications;
  }

  private static TypeExpressionSpec func(Type... argTypes) {
    return new TypeExpressionSpec().map(argTypes);
  }

  @Override
  public String toString() {
    return "Function [" + name() + "]";
  }
}
