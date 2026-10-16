/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.TYPE_FACTORY;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexExecutable;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.util.Holder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.search.lookup.SourceLookup;
import org.opensearch.sql.calcite.type.ExprVectorType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.data.type.VectorDataType;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.function.PPLFuncImpTable;
import org.opensearch.sql.opensearch.data.type.OpenSearchKnnVectorType;
import org.opensearch.sql.opensearch.storage.script.CalciteScriptEngine.ScriptDataContext;
import org.opensearch.sql.opensearch.storage.script.CalciteScriptEngine.Source;
import org.opensearch.sql.opensearch.storage.serde.RelJsonSerializer;
import org.opensearch.sql.opensearch.storage.serde.ScriptParameterHelper;

/** A pushed-down script over a knn_vector field reads typed elements from _source. */
class CalciteScriptEngineVectorTest {

  private final RexBuilder rexBuilder = new RexBuilder(TYPE_FACTORY);
  private final RelOptCluster cluster = RelOptCluster.create(new VolcanoPlanner(), rexBuilder);
  private final RelJsonSerializer serializer = new RelJsonSerializer(cluster);
  private Hook.Closeable currentTime;

  @BeforeEach
  void setCurrentTime() {
    currentTime = Hook.CURRENT_TIME.addThread((Consumer<Holder<Long>>) h -> h.set(0L));
  }

  @AfterEach
  void clearCurrentTime() {
    currentTime.close();
  }

  @Test
  void vectorFromSourceConvertsElements() {
    assertEquals(
        List.of(1.0f, 2.5f, -3.0f),
        ScriptDataContext.vectorFromSource(List.of(1.0d, 2.5d, -3), false));
    assertEquals(
        List.of((byte) 1, (byte) -2, (byte) 127),
        ScriptDataContext.vectorFromSource(List.of(1, -2, 127), true));
    assertNull(ScriptDataContext.vectorFromSource(null, false));
    assertNull(ScriptDataContext.vectorFromSource(List.of(), false));
    assertNull(ScriptDataContext.vectorFromSource("1,2", false));
    assertNull(ScriptDataContext.vectorFromSource(List.of(1, "x"), false));
  }

  /** k-NN also takes a base64 string (little-endian float32, or raw bytes) and a single number. */
  @Test
  void vectorFromSourceDecodesBase64AndSingleNumbers() {
    assertEquals(List.of(1.0f, -2.5f), ScriptDataContext.vectorFromSource("AACAPwAAIMA=", false));
    assertEquals(List.of((byte) 1, (byte) -1), ScriptDataContext.vectorFromSource("Af8=", true));
    assertEquals(List.of(7.0f), ScriptDataContext.vectorFromSource(7.0d, false));
    assertEquals(List.of((byte) 7), ScriptDataContext.vectorFromSource(7, true));
  }

  private static OpenSearchKnnVectorType knn(VectorDataType dataType) {
    return OpenSearchKnnVectorType.of(3, dataType, "l2", "lucene", "hnsw");
  }

  @Test
  void floatVectorIsReadThroughFloatVectorSource() {
    Object[] result =
        runOverVector(
            knn(VectorDataType.FLOAT),
            Source.FLOAT_VECTOR_SOURCE,
            List.of(1.0d, 2.0d, 3.0d),
            BigDecimal.valueOf(0.5));
    assertArrayEquals(new Object[] {true}, result);
  }

  @Test
  void byteVectorIsReadThroughByteVectorSource() {
    Object[] result =
        runOverVector(
            knn(VectorDataType.BYTE),
            Source.BYTE_VECTOR_SOURCE,
            List.of(-5, 2, 3),
            BigDecimal.ZERO);
    assertArrayEquals(new Object[] {false}, result);
  }

  /** A script reads base64 and single-number vectors as the scan does. */
  @Test
  void encodedVectorsAreDecodedInScripts() {
    // 1.0f, -2.5f little-endian: the first element is above 0.5.
    assertArrayEquals(
        new Object[] {true},
        runOverVector(
            knn(VectorDataType.FLOAT),
            Source.FLOAT_VECTOR_SOURCE,
            "AACAPwAAIMA=",
            BigDecimal.valueOf(0.5)));
    // The bytes -1, 1: the first element is not above 0.
    assertArrayEquals(
        new Object[] {false},
        runOverVector(
            knn(VectorDataType.BYTE), Source.BYTE_VECTOR_SOURCE, "/wE=", BigDecimal.ZERO));
    assertArrayEquals(
        new Object[] {true},
        runOverVector(
            knn(VectorDataType.FLOAT), Source.FLOAT_VECTOR_SOURCE, 7.0d, BigDecimal.valueOf(0.5)));
  }

  /**
   * A field that a float index and a byte index map reads Float elements; a script cannot tell a
   * document's index, so a base64 value fails rather than be decoded with the wrong data type.
   */
  @Test
  void mixedEncodingVectorIsReadThroughMixedVectorSource() {
    OpenSearchKnnVectorType mixed =
        knn(VectorDataType.FLOAT).inIndex("i0").mergedWith(knn(VectorDataType.BYTE).inIndex("i1"));
    assertArrayEquals(
        new Object[] {true},
        runOverVector(
            mixed, Source.MIXED_VECTOR_SOURCE, List.of(1, 2, 3), BigDecimal.valueOf(0.5)));
    SemanticCheckException e =
        assertThrows(
            SemanticCheckException.class,
            () ->
                runOverVector(mixed, Source.MIXED_VECTOR_SOURCE, "AQID", BigDecimal.valueOf(0.5)));
    assertTrue(e.getMessage().startsWith("cannot decode the base64 vector of [embedding]"));
  }

  /**
   * Serialize {@code mvindex(embedding, 0) > threshold} the way a pushed filter is, then compile
   * and run it against a document whose _source holds {@code source}. Before vectors had their own
   * source kind, the generated code cast a Double element to Float and threw ClassCastException.
   */
  private Object[] runOverVector(
      OpenSearchKnnVectorType fieldType, Source expected, Object source, BigDecimal threshold) {
    ExprVectorType vectorType = TYPE_FACTORY.createVectorType(fieldType, true);
    RelDataType rowType =
        TYPE_FACTORY
            .builder()
            .kind(StructKind.FULLY_QUALIFIED)
            .add("embedding", vectorType)
            .build();
    Map<String, ExprType> fieldTypes = Map.of("embedding", fieldType);
    RexNode first =
        PPLFuncImpTable.INSTANCE.resolve(
            rexBuilder,
            BuiltinFunctionName.MVINDEX,
            rexBuilder.makeInputRef(vectorType, 0),
            rexBuilder.makeExactLiteral(BigDecimal.ZERO));
    RexNode predicate =
        PPLFuncImpTable.INSTANCE.resolve(
            rexBuilder, ">", first, rexBuilder.makeExactLiteral(threshold));

    ScriptParameterHelper helper =
        new ScriptParameterHelper(rowType.getFieldList(), fieldTypes, rexBuilder);
    String code = serializer.serialize(predicate, helper);
    Map<String, Object> params = helper.getParameters();
    @SuppressWarnings("unchecked")
    List<Integer> sources = (List<Integer>) params.get(ScriptParameterHelper.SOURCES);
    assertTrue(sources.contains(expected.getValue()), sources::toString);
    assertFalse(sources.contains(Source.SOURCE.getValue()), sources::toString);

    RexNode deserialized = serializer.deserialize(code);
    String java =
        CalciteScriptEngine.translate(
            rexBuilder,
            List.of(deserialized),
            (blockBuilder, i, type) -> {
              throw new IllegalStateException("no input refs in a pushed script");
            },
            new RelRecordType(List.of()));
    Function1<DataContext, Object[]> function =
        new RexExecutable(java, "vector script").getFunction();

    SourceLookup sourceLookup = mock(SourceLookup.class);
    when(sourceLookup.extractValue("embedding", null)).thenReturn(source);
    Map<String, Integer> parameterToIndex =
        IntStream.range(0, sources.size()).boxed().collect(Collectors.toMap(i -> "?" + i, i -> i));
    return function.apply(new ScriptDataContext(Map.of(), sourceLookup, params, parameterToIndex));
  }
}
