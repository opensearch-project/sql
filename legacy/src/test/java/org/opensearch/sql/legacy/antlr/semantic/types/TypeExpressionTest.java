/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.antlr.semantic.types;

import static org.junit.Assert.assertEquals;
import static org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType.BOOLEAN;
import static org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType.DATE;
import static org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType.DOUBLE;
import static org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType.GEO_POINT;
import static org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType.INTEGER;
import static org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType.NUMBER;
import static org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType.STRING;
import static org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType.TYPE_ERROR;
import static org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType.UNKNOWN;
import static org.opensearch.sql.legacy.antlr.semantic.types.special.Generic.T;

import java.util.Arrays;
import org.junit.Test;

/** Test cases for default implementation methods in interface TypeExpression */
public class TypeExpressionTest {

  private final TypeExpression test123 =
      new TypeExpression() {

        @Override
        public String getName() {
          return "TEST123";
        }

        @Override
        public TypeExpressionSpec[] specifications() {
          return new TypeExpressionSpec[] {
            new TypeExpressionSpec().map(T(NUMBER)).to(T),
            new TypeExpressionSpec().map(STRING, BOOLEAN).to(DATE)
          };
        }
      };

  @Test
  public void emptySpecificationShouldAlwaysReturnUnknown() {
    TypeExpression expr =
        new TypeExpression() {
          @Override
          public TypeExpressionSpec[] specifications() {
            return new TypeExpressionSpec[0];
          }

          @Override
          public String getName() {
            return "Temp type expression with empty specification";
          }
        };
    assertEquals(UNKNOWN, expr.construct(Arrays.asList(NUMBER)));
    assertEquals(UNKNOWN, expr.construct(Arrays.asList(STRING, BOOLEAN)));
    assertEquals(UNKNOWN, expr.construct(Arrays.asList(INTEGER, DOUBLE, GEO_POINT)));
  }

  @Test
  public void compatibilityCheckShouldPassIfAnySpecificationCompatible() {
    assertEquals(DOUBLE, test123.construct(Arrays.asList(DOUBLE)));
    assertEquals(DATE, test123.construct(Arrays.asList(STRING, BOOLEAN)));
  }

  @Test
  public void compatibilityCheckShouldFailIfNoSpecificationCompatible() {
    assertEquals(TYPE_ERROR, test123.construct(Arrays.asList(BOOLEAN)));
  }

  @Test
  public void usageShouldPrintAllSpecifications() {
    assertEquals("TEST123(NUMBER T) -> T or TEST123(STRING, BOOLEAN) -> DATE", test123.usage());
  }
}
