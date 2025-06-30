/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf.ip;

public class IpComparisonOperators {

  public enum ComparisonOperator {
    EQUALS,
    NOT_EQUALS,
    LESS,
    LESS_OR_EQUAL,
    GREATER,
    GREATER_OR_EQUAL
  }

  public static CompareIpFunction equalsIp() {
    return new CompareIpFunction(ComparisonOperator.EQUALS);
  }

  public static CompareIpFunction notEqualsIp() {
    return new CompareIpFunction(ComparisonOperator.NOT_EQUALS);
  }

  public static CompareIpFunction lessThanIp() {
    return new CompareIpFunction(ComparisonOperator.LESS);
  }

  public static CompareIpFunction greaterThanIp() {
    return new CompareIpFunction(ComparisonOperator.GREATER);
  }

  public static CompareIpFunction lessOrEqualsIp() {
    return new CompareIpFunction(ComparisonOperator.LESS_OR_EQUAL);
  }

  public static CompareIpFunction greaterOrEqualsIp() {
    return new CompareIpFunction(ComparisonOperator.GREATER_OR_EQUAL);
  }
}
