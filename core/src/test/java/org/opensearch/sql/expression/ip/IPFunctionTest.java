/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.ip;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.data.model.ExprValueUtils.*;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.env.Environment;

@ExtendWith(MockitoExtension.class)
public class IPFunctionTest {

  // IP range and address constants for testing.
  private static final ExprValue IPv4Range = ExprValueUtils.stringValue("198.51.100.0/24");
  private static final ExprValue IPv6Range = ExprValueUtils.stringValue("2001:0db8::/32");

  private static final ExprValue IPv4AddressBelow = ExprValueUtils.stringValue("198.51.99.1");
  private static final ExprValue IPv4AddressWithin = ExprValueUtils.stringValue("198.51.100.1");
  private static final ExprValue IPv4AddressAbove = ExprValueUtils.stringValue("198.51.101.2");

  private static final ExprValue IPv6AddressBelow =
      ExprValueUtils.stringValue("2001:0db7::ff00:42:8329");
  private static final ExprValue IPv6AddressWithin =
      ExprValueUtils.stringValue("2001:0db8::ff00:42:8329");
  private static final ExprValue IPv6AddressAbove =
      ExprValueUtils.stringValue("2001:0db9::ff00:42:8329");

  // Mock value environment for testing.
  @Mock private Environment<Expression, ExprValue> env;

  @Test
  public void cidr_invalid_address() {
    assertEquals(LITERAL_NULL, execute(ExprValueUtils.stringValue("INVALID"), IPv4Range));
  }

  @Test
  public void cidr_invalid_range() {
    assertThrows(
        SemanticCheckException.class,
        () -> execute(IPv4AddressWithin, ExprValueUtils.stringValue("INVALID")));
    assertThrows(
        SemanticCheckException.class,
        () -> execute(IPv4AddressWithin, ExprValueUtils.stringValue("INVALID/32")));
    assertThrows(
        SemanticCheckException.class,
        () -> execute(IPv4AddressWithin, ExprValueUtils.stringValue("198.51.100.0/33")));
  }

  @Test
  public void cidr_valid_ipv4() {
    assertEquals(LITERAL_FALSE, execute(IPv4AddressBelow, IPv4Range));
    assertEquals(LITERAL_TRUE, execute(IPv4AddressWithin, IPv4Range));
    assertEquals(LITERAL_FALSE, execute(IPv4AddressAbove, IPv4Range));
  }

  @Test
  public void cidr_valid_ipv6() {
    assertEquals(LITERAL_FALSE, execute(IPv6AddressBelow, IPv6Range));
    assertEquals(LITERAL_TRUE, execute(IPv6AddressWithin, IPv6Range));
    assertEquals(LITERAL_FALSE, execute(IPv6AddressAbove, IPv6Range));
  }

  @Test
  public void cidr_valid_different_versions() {
    assertEquals(LITERAL_FALSE, execute(IPv4AddressWithin, IPv6Range));
    assertEquals(LITERAL_FALSE, execute(IPv6AddressWithin, IPv4Range));
  }

  /**
   * Builds and evaluates a CIDR function expression with the given field and range expression
   * values, and returns the resulting value.
   */
  private ExprValue execute(ExprValue field, ExprValue range) {

    final String fieldName = "ip_address";
    FunctionExpression exp = DSL.cidr(DSL.ref(fieldName, STRING), DSL.literal(range));

    // Mock the value environment to return the specified field
    // expression as the value for the "ip_address" field.
    when(DSL.ref(fieldName, STRING).valueOf(env)).thenReturn(field);

    return exp.valueOf(env);
  }
}
