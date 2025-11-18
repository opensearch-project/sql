/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.serde;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.TYPE_FACTORY;

import java.util.List;
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
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.function.PPLFuncImpTable;
import org.opensearch.sql.opensearch.data.type.OpenSearchBinaryType;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType.MappingType;
import org.opensearch.sql.opensearch.data.type.OpenSearchDateType;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
public class RelJsonSerializerTest {

  private final RexBuilder rexBuilder = new RexBuilder(TYPE_FACTORY);
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
    RexNode expectedNode =
        PPLFuncImpTable.INSTANCE.resolve(
            rexBuilder,
            BuiltinFunctionName.UPPER,
            rexBuilder.makeDynamicParam(rowType.getFieldList().get(0).getType(), 0));

    final ScriptParameterHelper helper =
        new ScriptParameterHelper(rowType.getFieldList(), fieldTypes);
    String code = serializer.serialize(rexUpper, helper);
    RexNode rexNode = serializer.deserialize(code);

    assertEquals(expectedNode, rexNode);
    assertEquals(List.of(0), helper.sources);
    assertEquals(List.of("Referer"), helper.digests);
  }

  @Test
  void testSerializeAndDeserializeUDT() {
    RelDataType rowTypeWithUDT =
        rexBuilder
            .getTypeFactory()
            .builder()
            .kind(StructKind.FULLY_QUALIFIED)
            .add("date", UserDefinedFunctionUtils.NULLABLE_DATE_UDT)
            .add("time", UserDefinedFunctionUtils.NULLABLE_TIME_UDT)
            .add("timestamp", UserDefinedFunctionUtils.NULLABLE_TIMESTAMP_UDT)
            .add("ip", UserDefinedFunctionUtils.NULLABLE_IP_UDT)
            .add("binary", TYPE_FACTORY.createUDT(OpenSearchTypeFactory.ExprUDT.EXPR_BINARY))
            .build();
    Map<String, ExprType> fieldTypesWithUDT =
        Map.ofEntries(
            Map.entry("date", OpenSearchDateType.of(ExprCoreType.DATE)),
            Map.entry("time", OpenSearchDateType.of(ExprCoreType.TIME)),
            Map.entry("timestamp", OpenSearchDateType.of(ExprCoreType.TIMESTAMP)),
            Map.entry("ip", OpenSearchDataType.of(ExprCoreType.IP)),
            Map.entry("binary", OpenSearchBinaryType.of()));
    RexNode rexNode =
        PPLFuncImpTable.INSTANCE.resolve(
            rexBuilder,
            BuiltinFunctionName.JSON_ARRAY,
            rexBuilder.makeInputRef(rowTypeWithUDT.getFieldList().get(0).getType(), 0),
            rexBuilder.makeInputRef(rowTypeWithUDT.getFieldList().get(1).getType(), 1),
            rexBuilder.makeInputRef(rowTypeWithUDT.getFieldList().get(2).getType(), 2),
            rexBuilder.makeInputRef(rowTypeWithUDT.getFieldList().get(3).getType(), 3),
            rexBuilder.makeInputRef(rowTypeWithUDT.getFieldList().get(4).getType(), 4));
    RexNode expectedNode =
        PPLFuncImpTable.INSTANCE.resolve(
            rexBuilder,
            BuiltinFunctionName.JSON_ARRAY,
            rexBuilder.makeDynamicParam(rowTypeWithUDT.getFieldList().get(0).getType(), 0),
            rexBuilder.makeDynamicParam(rowTypeWithUDT.getFieldList().get(1).getType(), 1),
            rexBuilder.makeDynamicParam(rowTypeWithUDT.getFieldList().get(2).getType(), 2),
            rexBuilder.makeDynamicParam(TYPE_FACTORY.createSqlType(SqlTypeName.OTHER, true), 3),
            rexBuilder.makeDynamicParam(rowTypeWithUDT.getFieldList().get(4).getType(), 4));
    final ScriptParameterHelper helper =
        new ScriptParameterHelper(rowTypeWithUDT.getFieldList(), fieldTypesWithUDT);
    String serialized = serializer.serialize(rexNode, helper);
    RexNode expr = serializer.deserialize(serialized);
    assertEquals(expectedNode, expr);
    assertEquals(List.of(0, 0, 0, 0, 0), helper.sources);
    assertEquals(List.of("date", "time", "timestamp", "ip", "binary"), helper.digests);
  }

  @Test
  void testSerializeUnsupportedRexNode() {
    RexNode illegalRex = rexBuilder.makeRangeReference(rowType, 0, true);
    assertThrows(
        IllegalStateException.class,
        () ->
            serializer.serialize(
                illegalRex, new ScriptParameterHelper(rowType.getFieldList(), fieldTypes)));
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
    final ScriptParameterHelper helper =
        new ScriptParameterHelper(rowType.getFieldList(), fieldTypes);
    String code = serializer.serialize(outOfScopeRex, helper);
    assertThrows(IllegalStateException.class, () -> serializer.deserialize(code));
  }

  @Test
  void testSerializeIndexRemappedRexNode() {
    RelDataType originalRowType =
        rexBuilder
            .getTypeFactory()
            .builder()
            .kind(StructKind.FULLY_QUALIFIED)
            .add("Firstname", rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR))
            .add("Referer", rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR))
            .build();
    Map<String, ExprType> originalFieldTypes =
        Map.of("Referer", ExprCoreType.STRING, "Firstname", ExprCoreType.STRING);
    RexNode originalRexUpper =
        PPLFuncImpTable.INSTANCE.resolve(
            rexBuilder,
            BuiltinFunctionName.UPPER,
            rexBuilder.makeInputRef(originalRowType.getFieldList().get(1).getType(), 1));
    RexNode remappedRexUpper =
        PPLFuncImpTable.INSTANCE.resolve(
            rexBuilder,
            BuiltinFunctionName.UPPER,
            rexBuilder.makeInputRef(rowType.getFieldList().get(0).getType(), 0));
    RexNode expectedNode =
        PPLFuncImpTable.INSTANCE.resolve(
            rexBuilder,
            BuiltinFunctionName.UPPER,
            rexBuilder.makeDynamicParam(rowType.getFieldList().get(0).getType(), 0));
    final ScriptParameterHelper helper =
        new ScriptParameterHelper(originalRowType.getFieldList(), originalFieldTypes);
    String code = serializer.serialize(originalRexUpper, helper);
    RexNode rex = serializer.deserialize(code);
    assertEquals(expectedNode, rex);
  }

  @Test
  void testSerializeAndDeserializeLiteral() {
    RelDataType rowTypeWithUDT =
        rexBuilder
            .getTypeFactory()
            .builder()
            .kind(StructKind.FULLY_QUALIFIED)
            .add("date", UserDefinedFunctionUtils.NULLABLE_DATE_UDT)
            .add("text", TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR, true))
            .build();
    Map<String, ExprType> fieldTypesWithUDT =
        Map.ofEntries(
            Map.entry("date", OpenSearchDateType.of(ExprCoreType.DATE)),
            Map.entry("text", OpenSearchDataType.of(MappingType.Text)));
    RexNode rexNode =
        PPLFuncImpTable.INSTANCE.resolve(
            rexBuilder,
            BuiltinFunctionName.JSON_ARRAY,
            rexBuilder.makeInputRef(rowTypeWithUDT.getFieldList().get(0).getType(), 0),
            rexBuilder.makeInputRef(rowTypeWithUDT.getFieldList().get(1).getType(), 1),
            rexBuilder.makeLiteral(1, TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER, true)));
    RexNode expectedNode =
        PPLFuncImpTable.INSTANCE.resolve(
            rexBuilder,
            BuiltinFunctionName.JSON_ARRAY,
            rexBuilder.makeDynamicParam(rowTypeWithUDT.getFieldList().get(0).getType(), 0),
            rexBuilder.makeDynamicParam(rowTypeWithUDT.getFieldList().get(1).getType(), 1),
            rexBuilder.makeDynamicParam(TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER, false), 2));
    final ScriptParameterHelper helper =
        new ScriptParameterHelper(rowTypeWithUDT.getFieldList(), fieldTypesWithUDT);
    String serialized = serializer.serialize(rexNode, helper);
    RexNode expr = serializer.deserialize(serialized);
    assertEquals(expectedNode, expr);
    assertEquals(List.of(0, 1, 2), helper.sources);
    assertEquals(List.of("date", "text", 1), helper.digests);
  }
}
