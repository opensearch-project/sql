/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.serde;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Map;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.function.PPLFuncImpTable;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
public class RelJsonSerializerTest {

  private final RexBuilder rexBuilder = new RexBuilder(OpenSearchTypeFactory.TYPE_FACTORY);
  private final RelOptCluster cluster = RelOptCluster.create(new VolcanoPlanner(), rexBuilder);
  private final RelJsonSerializer serializer = new RelJsonSerializer(cluster);
  private final RelDataType rowType =
      rexBuilder
          .getTypeFactory()
          .builder()
          .kind(StructKind.FULLY_QUALIFIED)
          .add("Referer", rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR))
          .build();
  private final Map<String, ExprType> fieldTypes = Map.of("Referer", ExprCoreType.STRING);

  @Test
  void testSerializeAndDeserialize() {
    RexNode rexUpper =
        PPLFuncImpTable.INSTANCE.resolve(
            rexBuilder,
            BuiltinFunctionName.UPPER,
            rexBuilder.makeInputRef(rowType.getFieldList().get(0).getType(), 0));

    String code = serializer.serialize(rexUpper, rowType, fieldTypes);
    Map<String, Object> objects = serializer.deserialize(code);

    assertEquals(rexUpper, objects.get(RelJsonSerializer.EXPR));
    assertEquals(rowType, objects.get(RelJsonSerializer.ROW_TYPE));
    assertEquals(fieldTypes, objects.get(RelJsonSerializer.FIELD_TYPES));
  }

  @Test
  void testSerializeUnsupportedRexNode() {
    RexNode illegalRex = rexBuilder.makeRangeReference(rowType, 0, true);

    assertThrows(
        IllegalStateException.class, () -> serializer.serialize(illegalRex, rowType, fieldTypes));
  }

  @Test
  void testDeserializeIllegalScript() {
    assertThrows(IllegalStateException.class, () -> serializer.deserialize("illegal script"));
  }

  @Test
  void testDeserializeFunctionOutOfScope() {
    RexNode outOfScopeRex =
        rexBuilder.makeCall(
            SqlLibraryOperators.SUBSTR_ORACLE,
            rexBuilder.makeInputRef(rowType.getFieldList().get(0).getType(), 0),
            rexBuilder.makeLiteral(
                1, rexBuilder.getTypeFactory().createSqlType(SqlTypeName.INTEGER)));

    String code = serializer.serialize(outOfScopeRex, rowType, fieldTypes);
    assertThrows(IllegalStateException.class, () -> serializer.deserialize(code));
  }
}
