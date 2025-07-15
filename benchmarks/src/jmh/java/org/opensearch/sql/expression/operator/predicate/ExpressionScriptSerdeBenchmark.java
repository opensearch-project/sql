/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.operator.predicate;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.function.PPLFuncImpTable;
import org.opensearch.sql.opensearch.storage.serialization.DefaultExpressionSerializer;
import org.opensearch.sql.opensearch.storage.serialization.RelJsonSerializer;

@Warmup(iterations = 1)
@Measurement(iterations = 10)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
@Fork(value = 1)
public class ExpressionScriptSerdeBenchmark {

  @Benchmark
  public void testV2ExpressionSerde() {
    DefaultExpressionSerializer defaultSerializer = new DefaultExpressionSerializer();
    Expression exprUpper = DSL.upper(DSL.ref("Referer", ExprCoreType.STRING));
    Expression exprNotEquals = DSL.notequal(exprUpper, DSL.literal("ABOUT"));

    String serializedStr = defaultSerializer.serialize(exprNotEquals);
    defaultSerializer.deserialize(serializedStr);
  }

  @Benchmark
  public void testRexNodeJsonSerde() {
    RexBuilder rexBuilder = new RexBuilder(OpenSearchTypeFactory.TYPE_FACTORY);
    RelOptCluster cluster = RelOptCluster.create(new VolcanoPlanner(), rexBuilder);
    RelJsonSerializer relJsonSerializer = new RelJsonSerializer(cluster);
    RelDataType rowType =
        rexBuilder
            .getTypeFactory()
            .builder()
            .kind(StructKind.FULLY_QUALIFIED)
            .add("Referer", rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR))
            .build();
    RexNode rexUpper =
        PPLFuncImpTable.INSTANCE.resolve(
            rexBuilder,
            BuiltinFunctionName.UPPER,
            rexBuilder.makeInputRef(rowType.getFieldList().get(0).getType(), 0));
    RexNode rexNotEquals =
        rexBuilder.makeCall(
            SqlStdOperatorTable.NOT_EQUALS, rexUpper, rexBuilder.makeLiteral("ABOUT"));
    Map<String, ExprType> fieldTypes = Map.of("Referer", ExprCoreType.STRING);

    String serializedStr = relJsonSerializer.serialize(rexNotEquals, rowType, fieldTypes);
    relJsonSerializer.deserialize(serializedStr);
  }
}
