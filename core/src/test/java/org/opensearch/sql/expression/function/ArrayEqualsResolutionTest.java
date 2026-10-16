/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;

class ArrayEqualsResolutionTest {

  @Test
  void equalsOnArrayColumnResolvesToArrayContains() {
    RexBuilder builder = new RexBuilder(OpenSearchTypeFactory.TYPE_FACTORY);
    RelDataType varchar =
        OpenSearchTypeFactory.TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR);
    RelDataType arrayOfVarchar =
        OpenSearchTypeFactory.TYPE_FACTORY.createTypeWithNullability(
            OpenSearchTypeFactory.TYPE_FACTORY.createArrayType(varchar, -1), true);

    RexNode arrayRef = builder.makeInputRef(arrayOfVarchar, 0);
    RexNode literal = builder.makeLiteral("alpha");
    System.out.println("array type = " + arrayRef.getType().getSqlTypeName());
    System.out.println("literal type = " + literal.getType().getSqlTypeName());

    RexNode resolved =
        PPLFuncImpTable.INSTANCE.resolve(builder, BuiltinFunctionName.EQUAL, arrayRef, literal);
    System.out.println("resolved = " + resolved);
    assertEquals("ARRAY_CONTAINS", ((org.apache.calcite.rex.RexCall) resolved).getOperator().getName());
  }
}
