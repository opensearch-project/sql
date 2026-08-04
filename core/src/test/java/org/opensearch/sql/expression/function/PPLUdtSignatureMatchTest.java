/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.ExprUDT;
import org.opensearch.sql.calcite.utils.PPLOperandTypes;

/**
 * Exercises {@link PPLTypeChecker#wrapUDT} against plain Calcite types. Signatures declare
 * temporal/IP/BINARY operands as UDTs, but the analytics engine builds row types from plain Calcite
 * types, so a UDT signature must still accept the equivalent plain type.
 */
class PPLUdtSignatureMatchTest {

  private static final OpenSearchTypeFactory TF = OpenSearchTypeFactory.TYPE_FACTORY;

  /** The checker behind {@code list(<field>)}. */
  private static final PPLTypeChecker ANY_SCALAR =
      PPLTypeChecker.wrapUDT(
          ((UDFOperandMetadata.UDTOperandMetadata) PPLOperandTypes.ANY_SCALAR).allowedParamTypes());

  private static RelDataType nullable(RelDataType type) {
    return TF.createTypeWithNullability(type, true);
  }

  private static boolean accepts(RelDataType type) {
    return ANY_SCALAR.checkOperandTypes(List.of(type));
  }

  @Test
  void plainTimestampMatchesTimestampUdt() {
    // date -> TIMESTAMP(3), date_nanos -> TIMESTAMP(9) on the analytics route.
    assertTrue(accepts(nullable(TF.createSqlType(SqlTypeName.TIMESTAMP, 3))));
    assertTrue(accepts(nullable(TF.createSqlType(SqlTypeName.TIMESTAMP, 9))));
  }

  @Test
  void plainDateAndTimeMatchTheirUdts() {
    assertTrue(accepts(nullable(TF.createSqlType(SqlTypeName.DATE))));
    assertTrue(accepts(nullable(TF.createSqlType(SqlTypeName.TIME))));
  }

  @Test
  void plainVarbinaryMatchesBinaryUdt() {
    // ip and binary both map to VARBINARY on the analytics route.
    assertTrue(accepts(nullable(TF.createSqlType(SqlTypeName.VARBINARY))));
  }

  @Test
  void udtOperandsStillMatch() {
    assertTrue(accepts(TF.createUDT(ExprUDT.EXPR_TIMESTAMP)));
    assertTrue(accepts(TF.createUDT(ExprUDT.EXPR_DATE)));
    assertTrue(accepts(TF.createUDT(ExprUDT.EXPR_TIME)));
    assertTrue(accepts(TF.createUDT(ExprUDT.EXPR_IP)));
  }

  @Test
  void plainScalarsStillMatch() {
    assertTrue(accepts(TF.createSqlType(SqlTypeName.INTEGER)));
    assertTrue(accepts(TF.createSqlType(SqlTypeName.BIGINT)));
    assertTrue(accepts(TF.createSqlType(SqlTypeName.VARCHAR)));
    assertTrue(accepts(TF.createSqlType(SqlTypeName.BOOLEAN)));
  }

  @Test
  void nonScalarsAreStillRejected() {
    assertFalse(
        accepts(TF.createArrayType(TF.createSqlType(SqlTypeName.INTEGER), -1)),
        "ANY_SCALAR must not accept arrays");
    assertFalse(
        accepts(
            TF.createMapType(
                TF.createSqlType(SqlTypeName.VARCHAR), TF.createSqlType(SqlTypeName.INTEGER))),
        "ANY_SCALAR must not accept maps");
  }
}
