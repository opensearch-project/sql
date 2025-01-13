/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.ip;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.data.type.ExprCoreType.BOOLEAN;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.env.Environment;

@ExtendWith(MockitoExtension.class)
public class GeoIPFunctionTest {

  // Mock value environment for testing.
  @Mock private Environment<Expression, ExprValue> env;

  @Test
  public void geoIpDefaultImplementation() {
    UnsupportedOperationException exception =
        assertThrows(
            UnsupportedOperationException.class,
            () ->
                DSL.geoip(DSL.literal("HARDCODED_DATASOURCE_NAME"), DSL.ref("ip_address", STRING))
                    .valueOf(env));
    assertTrue(exception.getMessage().matches(".*no default implementation available"));
  }

  @Test
  public void testGeoipFnctionSignature() {
    var geoip = DSL.geoip(DSL.literal("HARDCODED_DATASOURCE_NAME"), DSL.ref("ip_address", STRING));
    assertEquals(BOOLEAN, geoip.type());
  }

  /** To make sure no logic being evaluated when no environment being passed. */
  @Test
  public void testDefaultValueOf() {
    var geoip = DSL.geoip(DSL.literal("HARDCODED_DATASOURCE_NAME"), DSL.ref("ip_address", STRING));
    assertNull(geoip.valueOf());
  }
}
