/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.validate;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableSet;
import java.util.Map;
import org.apache.calcite.sql.type.SqlTypeCoercionRule;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

public class PplTypeCoercionRuleTest {
  @Test
  public void testOtherToVarcharCoercion() {
    SqlTypeCoercionRule rule = PplTypeCoercionRule.instance();
    Map<SqlTypeName, ImmutableSet<SqlTypeName>> mapping = rule.getTypeMapping();

    // OTHER should be coercible to VARCHAR (IP type support)
    ImmutableSet<SqlTypeName> varcharCoercions = mapping.get(SqlTypeName.VARCHAR);
    assertNotNull(varcharCoercions);
    assertTrue(varcharCoercions.contains(SqlTypeName.OTHER));
  }

  @Test
  public void testOtherToCharCoercion() {
    SqlTypeCoercionRule rule = PplTypeCoercionRule.instance();
    Map<SqlTypeName, ImmutableSet<SqlTypeName>> mapping = rule.getTypeMapping();

    // OTHER should be coercible to CHAR (IP type support)
    ImmutableSet<SqlTypeName> charCoercions = mapping.get(SqlTypeName.CHAR);
    assertNotNull(charCoercions);
    assertTrue(charCoercions.contains(SqlTypeName.OTHER));
  }

  @Test
  public void testVarcharToOtherCoercion() {
    SqlTypeCoercionRule rule = PplTypeCoercionRule.instance();
    Map<SqlTypeName, ImmutableSet<SqlTypeName>> mapping = rule.getTypeMapping();

    // VARCHAR and CHAR should be coercible to OTHER (for IP type support)
    ImmutableSet<SqlTypeName> otherCoercions = mapping.get(SqlTypeName.OTHER);
    assertNotNull(otherCoercions);
    assertTrue(otherCoercions.contains(SqlTypeName.VARCHAR));
    assertTrue(otherCoercions.contains(SqlTypeName.CHAR));
  }

  @Test
  public void testNumericToVarcharCoercion() {
    SqlTypeCoercionRule rule = PplTypeCoercionRule.instance();
    Map<SqlTypeName, ImmutableSet<SqlTypeName>> mapping = rule.getTypeMapping();

    // VARCHAR should be coercible from numeric types
    ImmutableSet<SqlTypeName> varcharCoercions = mapping.get(SqlTypeName.VARCHAR);
    assertNotNull(varcharCoercions);

    // Check some numeric types are included
    assertTrue(varcharCoercions.contains(SqlTypeName.INTEGER));
    assertTrue(varcharCoercions.contains(SqlTypeName.BIGINT));
    assertTrue(varcharCoercions.contains(SqlTypeName.DOUBLE));
    assertTrue(varcharCoercions.contains(SqlTypeName.DECIMAL));
  }
}
