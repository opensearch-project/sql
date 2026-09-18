/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.TYPE_FACTORY;

import java.util.List;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

/**
 * A function that receives a flat_object leaf -- a VARIANT argument -- and cannot take one says so,
 * rather than reporting a type it does not have.
 */
public class PPLFuncImpTableFlatObjectTest {

  private final RexBuilder builder = new RexBuilder(TYPE_FACTORY);

  private RexNode leaf() {
    return builder.makeInputRef(TYPE_FACTORY.createSqlType(SqlTypeName.VARIANT, true), 0);
  }

  private void rejects(Runnable call, String what) {
    Exception ex = assertThrows(Exception.class, call::run);
    assertTrue(String.valueOf(ex.getMessage()).contains(what), String.valueOf(ex.getMessage()));
  }

  // typeof reports the static type of its argument; a leaf has none, so it would answer
  // "undefined" for every record.
  @Test
  public void typeofOfALeaf() {
    rejects(
        () -> PPLFuncImpTable.INSTANCE.resolve(builder, BuiltinFunctionName.TYPEOF, leaf()),
        "Cannot apply typeof to a flat_object leaf");
  }

  // An aggregation cannot take a leaf either, and says the same thing instead of a type error.
  @Test
  public void anAggregationOverALeaf() {
    rejects(
        () ->
            PPLFuncImpTable.INSTANCE.validateAggFunctionSignature(
                BuiltinFunctionName.AVG, leaf(), List.of(), builder),
        "Cannot apply avg to a flat_object leaf");
  }

  @Test
  public void aFunctionOverAnOrdinaryColumn() {
    RexNode text = builder.makeInputRef(TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR), 0);
    PPLFuncImpTable.INSTANCE.resolve(builder, BuiltinFunctionName.TYPEOF, text);
  }
}
