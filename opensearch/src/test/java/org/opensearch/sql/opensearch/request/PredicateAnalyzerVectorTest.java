/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.request;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.TYPE_FACTORY;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.Holder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.ScriptQueryBuilder;
import org.opensearch.sql.calcite.type.ExprVectorType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.data.type.VectorDataType;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.function.PPLFuncImpTable;
import org.opensearch.sql.opensearch.data.type.OpenSearchKnnVectorType;
import org.opensearch.sql.opensearch.request.PredicateAnalyzer.ExpressionNotAnalyzableException;

/** Filter pushdown over knn_vector columns. */
class PredicateAnalyzerVectorTest {

  private final RexBuilder builder = new RexBuilder(TYPE_FACTORY);
  private final RelOptCluster cluster = RelOptCluster.create(new VolcanoPlanner(), builder);
  private final ExprVectorType vectorType =
      TYPE_FACTORY.createVectorType(3, VectorDataType.FLOAT, "l2", true);
  private final RelDataType rowType =
      TYPE_FACTORY.builder().kind(StructKind.FULLY_QUALIFIED).add("embedding", vectorType).build();
  private final List<String> schema = List.of("embedding");
  private final Map<String, ExprType> fieldTypes =
      Map.of(
          "embedding", OpenSearchKnnVectorType.of(3, VectorDataType.FLOAT, "l2", "lucene", "hnsw"));
  private Hook.Closeable currentTime;

  @BeforeEach
  void setCurrentTime() {
    currentTime = Hook.CURRENT_TIME.addThread((Consumer<Holder<Long>>) h -> h.set(0L));
  }

  @AfterEach
  void clearCurrentTime() {
    currentTime.close();
  }

  /**
   * An exists query would find a vector that is excluded from _source, while the in-memory read of
   * that field is null, so null tests on a vector are evaluated in memory, not pushed.
   */
  @Test
  void nullTestsOnVectorAreNotPushed() {
    RexNode ref = builder.makeInputRef(vectorType, 0);
    for (RexNode call :
        List.of(
            builder.makeCall(SqlStdOperatorTable.IS_NULL, ref),
            builder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, ref))) {
      assertThrows(
          ExpressionNotAnalyzableException.class,
          () -> PredicateAnalyzer.analyze(call, schema, fieldTypes, rowType, cluster));
    }
  }

  @Test
  void expressionOverVectorElementsIsPushedAsScript() throws ExpressionNotAnalyzableException {
    RexNode first =
        PPLFuncImpTable.INSTANCE.resolve(
            builder,
            BuiltinFunctionName.MVINDEX,
            builder.makeInputRef(vectorType, 0),
            builder.makeExactLiteral(BigDecimal.ZERO));
    RexNode call =
        PPLFuncImpTable.INSTANCE.resolve(
            builder, ">", first, builder.makeExactLiteral(BigDecimal.valueOf(0.5)));
    QueryBuilder query = PredicateAnalyzer.analyze(call, schema, fieldTypes, rowType, cluster);
    assertInstanceOf(ScriptQueryBuilder.class, query);
    // 4 is FLOAT_VECTOR_SOURCE: the script converts the _source elements to Float.
    assertTrue(query.toString().contains("\"SOURCES\""), query::toString);
    assertTrue(query.toString().matches("(?s).*\"SOURCES\"\\s*:\\s*\\[[^]]*4.*"), query::toString);
  }
}
