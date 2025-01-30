/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.ip;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_FALSE;
import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_TRUE;
import static org.opensearch.sql.data.type.ExprCoreType.IP;

import lombok.ToString;
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

@ToString
@ExtendWith(MockitoExtension.class)
class IPFunctionsTest {

  // IP range and address constants for testing.
  private static final ExprValue IPv4Range = ExprValueUtils.stringValue("198.51.100.0/24");
  private static final ExprValue IPv4RangeMapped =
      ExprValueUtils.stringValue("::ffff:198.51.100.0/24");
  private static final ExprValue IPv6Range = ExprValueUtils.stringValue("2001:0db8::/32");

  private static final ExprValue IPv4AddressBelow = ExprValueUtils.ipValue("198.51.99.1");
  private static final ExprValue IPv4AddressWithin = ExprValueUtils.ipValue("198.51.100.1");
  private static final ExprValue IPv4AddressAbove = ExprValueUtils.ipValue("198.51.101.2");

  private static final ExprValue IPv6AddressBelow =
      ExprValueUtils.ipValue("2001:0db7::ff00:42:8329");
  private static final ExprValue IPv6AddressWithin =
      ExprValueUtils.ipValue("2001:0db8::ff00:42:8329");
  private static final ExprValue IPv6AddressAbove =
      ExprValueUtils.ipValue("2001:0db9::ff00:42:8329");

  // Mock value environment for testing.
  @Mock private Environment<Expression, ExprValue> env;

  @Test
  void cidrmatch_invalid_arguments() {
    Exception ex;

    ex =
        assertThrows(
            SemanticCheckException.class,
            () -> execute(ExprValueUtils.ipValue("INVALID"), IPv4Range));
    assertTrue(
        ex.getMessage().matches("^IP address string 'INVALID' is not valid. Error details: .*"));

    ex =
        assertThrows(
            SemanticCheckException.class,
            () -> execute(IPv4AddressWithin, ExprValueUtils.stringValue("INVALID")));
    assertTrue(
        ex.getMessage()
            .matches("^IP address range string 'INVALID' is not valid. Error details: .*"));
  }

  @Test
  void cidrmatch_valid_arguments() {

    assertEquals(LITERAL_FALSE, execute(IPv4AddressBelow, IPv4Range));
    assertEquals(LITERAL_TRUE, execute(IPv4AddressWithin, IPv4Range));
    assertEquals(LITERAL_FALSE, execute(IPv4AddressAbove, IPv4Range));

    assertEquals(LITERAL_FALSE, execute(IPv4AddressBelow, IPv4RangeMapped));
    assertEquals(LITERAL_TRUE, execute(IPv4AddressWithin, IPv4RangeMapped));
    assertEquals(LITERAL_FALSE, execute(IPv4AddressAbove, IPv4RangeMapped));

    assertEquals(LITERAL_FALSE, execute(IPv6AddressBelow, IPv6Range));
    assertEquals(LITERAL_TRUE, execute(IPv6AddressWithin, IPv6Range));
    assertEquals(LITERAL_FALSE, execute(IPv6AddressAbove, IPv6Range));
  }

  /**
   * Builds and evaluates a {@code cidrmatch} function expression with the given address and range
   * expression values, and returns the resulting value.
   */
  private ExprValue execute(ExprValue address, ExprValue range) {

    final String fieldName = "ip_address";
    FunctionExpression exp = DSL.cidrmatch(DSL.ref(fieldName, IP), DSL.literal(range));

    // Mock the value environment to return the specified field
    // expression as the value for the "ip_address" field.
    when(DSL.ref(fieldName, IP).valueOf(env)).thenReturn(address);

    return exp.valueOf(env);
  }
}
