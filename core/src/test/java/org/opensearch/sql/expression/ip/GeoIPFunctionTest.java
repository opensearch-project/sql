/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.ip;

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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_FALSE;
import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_TRUE;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;

@ExtendWith(MockitoExtension.class)
public class GeoIPFunctionTest {

  // Mock value environment for testing.
  @Mock private Environment<Expression, ExprValue> env;

  @Test
  public void geoIpDefaultImplementation() {
    UnsupportedOperationException exception =
        assertThrows(
            UnsupportedOperationException.class,
            () -> DSL.geoip(
                    DSL.literal("HARDCODED_DS"),
                    DSL.ref("ip_address", STRING)).valueOf(env));
    assertTrue(
        exception.getMessage().matches(".*no default implementation available"));
  }
}
