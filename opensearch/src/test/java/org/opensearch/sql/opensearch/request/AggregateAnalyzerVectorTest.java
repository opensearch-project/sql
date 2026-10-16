/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.request;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.opensearch.response.AggregationResponseUtils.fromJson;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Holder;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.metrics.TopHits;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.data.type.VectorDataType;
import org.opensearch.sql.expression.function.PPLBuiltinOperators;
import org.opensearch.sql.opensearch.data.type.OpenSearchAliasType;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.data.type.OpenSearchKnnVectorType;
import org.opensearch.sql.opensearch.request.AggregateAnalyzer.ExpressionNotAnalyzableException;
import org.opensearch.sql.opensearch.response.agg.ArgMaxMinParser;
import org.opensearch.sql.opensearch.response.agg.MetricParser;
import org.opensearch.sql.opensearch.response.agg.MetricParserHelper;
import org.opensearch.sql.opensearch.response.agg.NoBucketAggregationParser;
import org.opensearch.sql.opensearch.response.agg.OpenSearchAggregationResponseParser;
import org.opensearch.sql.opensearch.response.agg.TopHitsParser;

/**
 * A {@code top_hits} pushdown reads a knn_vector field from {@code _source}. The fields API would
 * flatten the vector to its elements, of which {@code DocumentField.getValue()} keeps the first,
 * and the k-NN plugin's fetcher returns nothing for a vector indexed as a base64 string. A single
 * value of a vector is read from {@code _source} by name.
 */
class AggregateAnalyzerVectorTest {

  private final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

  private RelDataType array(SqlTypeName element) {
    return typeFactory.createArrayType(typeFactory.createSqlType(element), -1);
  }

  private final RelDataType rowType =
      typeFactory.createStructType(
          List.of(
              typeFactory.createSqlType(SqlTypeName.INTEGER),
              array(SqlTypeName.REAL),
              array(SqlTypeName.TINYINT),
              array(SqlTypeName.REAL),
              typeFactory.createMapType(
                  typeFactory.createSqlType(SqlTypeName.VARCHAR),
                  typeFactory.createSqlType(SqlTypeName.ANY)),
              array(SqlTypeName.REAL)),
          List.of("id", "v", "b", "al", "obj", "nal"));

  private static Map<String, Object> knn(String dataType) {
    return Map.of("type", "knn_vector", "dimension", 3, "data_type", dataType);
  }

  /** Flattened, with each field's {@code getExprType()}, as {@code OpenSearchIndex} gives them. */
  private final Map<String, ExprType> fieldTypes =
      OpenSearchDataType.traverseAndFlatten(
              OpenSearchDataType.parseMapping(
                  Map.of(
                      "id", Map.of("type", "integer"),
                      "v", knn("float"),
                      "b", knn("byte"),
                      "al", Map.of("type", "alias", "path", "v"),
                      "obj",
                          Map.of(
                              "properties",
                              Map.of("vec", knn("float"), "name", Map.of("type", "keyword"))),
                      "nal", Map.of("type", "alias", "path", "obj.vec"))))
          .entrySet()
          .stream()
          .collect(
              java.util.stream.Collectors.toUnmodifiableMap(
                  Map.Entry::getKey, e -> e.getValue().getExprType()));

  private RelBuilder relBuilder() {
    SchemaPlus root = Frameworks.createRootSchema(true);
    root.add(
        "test",
        new AbstractTable() {
          @Override
          public RelDataType getRowType(RelDataTypeFactory tf) {
            return rowType;
          }
        });
    return RelBuilder.create(Frameworks.newConfigBuilder().defaultSchema(root).build())
        .scan("test");
  }

  private Pair<List<AggregationBuilder>, OpenSearchAggregationResponseParser> analyze(
      String outputField, Function<RelBuilder, RelBuilder.AggCall> aggCall)
      throws ExpressionNotAnalyzableException {
    return analyze(fieldTypes, outputField, aggCall);
  }

  private Pair<List<AggregationBuilder>, OpenSearchAggregationResponseParser> analyze(
      Map<String, ExprType> fieldTypes,
      String outputField,
      Function<RelBuilder, RelBuilder.AggCall> aggCall)
      throws ExpressionNotAnalyzableException {
    Hook.CURRENT_TIME.addThread((Consumer<Holder<Long>>) h -> h.set(0L));
    RelBuilder builder = relBuilder();
    RelNode rel = builder.aggregate(builder.groupKey(), aggCall.apply(builder)).build();
    Aggregate aggregate = (Aggregate) rel;
    AggregateAnalyzer.AggregateBuilderHelper helper =
        new AggregateAnalyzer.AggregateBuilderHelper(
            rowType, fieldTypes, aggregate.getCluster(), true, 1000);
    return AggregateAnalyzer.analyze(aggregate, null, List.of(outputField), helper);
  }

  private static MetricParser parserOf(
      Pair<List<AggregationBuilder>, OpenSearchAggregationResponseParser> result) {
    return ((NoBucketAggregationParser) result.getRight())
        .getMetricsParser()
        .getMetricParserMap()
        .values()
        .iterator()
        .next();
  }

  private AggregateAnalyzer.AggregateBuilderHelper helper(Map<String, ExprType> fieldTypes) {
    return new AggregateAnalyzer.AggregateBuilderHelper(rowType, fieldTypes, null, true, 1000);
  }

  /** The arguments of a top_hits over the named columns, each output under its own name. */
  private List<Pair<RexNode, String>> args(String... columns) {
    return java.util.Arrays.stream(columns)
        .map(
            column ->
                Pair.<RexNode, String>of(
                    RexInputRef.of(rowType.getFieldNames().indexOf(column), rowType), column))
        .toList();
  }

  private static AggregateCall dedupCall() {
    return AggregateCall.create(
        SqlStdOperatorTable.COUNT,
        false,
        false,
        false,
        List.of(),
        List.of(),
        -1,
        null,
        RelCollations.EMPTY,
        new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT).createSqlType(SqlTypeName.INTEGER),
        "dedup");
  }

  /** The response of a top_hits with one hit of the given _source, as the cluster returns it. */
  private static String topHits(String name, String source) {
    return "{\"top_hits#"
        + name
        + "\":{\"hits\":{\"total\":{\"value\":1,\"relation\":\"eq\"},\"max_score\":1.0,"
        + "\"hits\":[{\"_index\":\"i1\",\"_id\":\"1\",\"_score\":1.0,\"_source\":"
        + source
        + "}]}}}";
  }

  private static Object parse(MetricParser parser, String response) {
    return parser.parse(fromJson(response).get(parser.getName())).getFirst().get(parser.getName());
  }

  @Test
  void firstReadsTheVectorFromSource() throws ExpressionNotAnalyzableException {
    Pair<List<AggregationBuilder>, OpenSearchAggregationResponseParser> result =
        analyze(
            "first_v", b -> b.aggregateCall(PPLBuiltinOperators.FIRST, b.field("v")).as("first_v"));
    assertEquals(
        "[{\"first_v\":{\"top_hits\":{\"from\":0,\"size\":1,\"version\":false,"
            + "\"seq_no_primary_term\":false,\"explain\":false,"
            + "\"_source\":{\"includes\":[\"v\"]}}}}]",
        result.getLeft().toString());
    // The whole vector, as indexed: an array or a base64 string.
    MetricParser parser = parserOf(result);
    assertEquals(List.of(1, 2.5, 3), parse(parser, topHits("first_v", "{\"v\":[1,2.5,3]}")));
    assertEquals(
        "AACAPwAAIMAAAABA", parse(parser, topHits("first_v", "{\"v\":\"AACAPwAAIMAAAABA\"}")));
  }

  @Test
  void lastReadsTheByteVectorFromSource() throws ExpressionNotAnalyzableException {
    assertEquals(
        "[{\"last_b\":{\"top_hits\":{\"from\":0,\"size\":1,\"version\":false,"
            + "\"seq_no_primary_term\":false,\"explain\":false,"
            + "\"_source\":{\"includes\":[\"b\"]},"
            + "\"sort\":[{\"_doc\":{\"order\":\"desc\"}}]}}}]",
        analyze("last_b", b -> b.aggregateCall(PPLBuiltinOperators.LAST, b.field("b")).as("last_b"))
            .getLeft()
            .toString());
  }

  /** An alias reads its target's value from _source, by the target's path. */
  @Test
  void firstOfAnAliasReadsTheTargetFromSource() throws ExpressionNotAnalyzableException {
    Pair<List<AggregationBuilder>, OpenSearchAggregationResponseParser> result =
        analyze(
            "first_al",
            b -> b.aggregateCall(PPLBuiltinOperators.FIRST, b.field("al")).as("first_al"));
    String dsl = result.getLeft().toString();
    assertTrue(dsl.contains("\"_source\":{\"includes\":[\"v\"]}"), dsl);
    assertFalse(dsl.contains("\"fields\""), dsl);
    assertEquals(List.of(1, 2), parse(parserOf(result), topHits("first_al", "{\"v\":[1,2]}")));
  }

  /** Other fields are fetched as before. */
  @Test
  void otherFieldsUseTheFieldsApi() throws ExpressionNotAnalyzableException {
    Pair<List<AggregationBuilder>, OpenSearchAggregationResponseParser> result =
        analyze(
            "first_id",
            b -> b.aggregateCall(PPLBuiltinOperators.FIRST, b.field("id")).as("first_id"));
    String dsl = result.getLeft().toString();
    assertTrue(dsl.contains("\"_source\":false,\"fields\":[{\"field\":\"id\"}]"), dsl);
    assertEquals(new TopHitsParser("first_id", true, false), parserOf(result));
    // An object field too: the whole _source, read as before.
    Pair<List<AggregationBuilder>, OpenSearchAggregationResponseParser> object =
        analyze(
            "first_obj",
            b -> b.aggregateCall(PPLBuiltinOperators.FIRST, b.field("obj")).as("first_obj"));
    assertEquals(
        "[{\"first_obj\":{\"top_hits\":{\"from\":0,\"size\":1,\"version\":false,"
            + "\"seq_no_primary_term\":false,\"explain\":false}}}]",
        object.getLeft().toString());
    assertEquals(new TopHitsParser("first_obj", true, false), parserOf(object));
  }

  /**
   * earliest and latest are ARG_MIN and ARG_MAX: the vector comes from _source, which the parser
   * reads by name, as the top_hits fetches no fields for it.
   */
  @Test
  void argMinAndArgMaxReadTheVectorFromSource() throws ExpressionNotAnalyzableException {
    Pair<List<AggregationBuilder>, OpenSearchAggregationResponseParser> earliest =
        analyze(
            "earliest_v",
            b ->
                b.aggregateCall(SqlStdOperatorTable.ARG_MIN, b.field("v"), b.field("id"))
                    .as("earliest_v"));
    assertEquals(
        "[{\"earliest_v\":{\"top_hits\":{\"from\":0,\"size\":1,\"version\":false,"
            + "\"seq_no_primary_term\":false,\"explain\":false,"
            + "\"_source\":{\"includes\":[\"v\"]},"
            + "\"sort\":[{\"id\":{\"order\":\"asc\"}}]}}}]",
        earliest.getLeft().toString());
    assertEquals(new ArgMaxMinParser("earliest_v", "v", false), parserOf(earliest));
    assertEquals(
        List.of(1, 2.5, 3), parse(parserOf(earliest), topHits("earliest_v", "{\"v\":[1,2.5,3]}")));
    Pair<List<AggregationBuilder>, OpenSearchAggregationResponseParser> latest =
        analyze(
            "latest_b",
            b ->
                b.aggregateCall(SqlStdOperatorTable.ARG_MAX, b.field("b"), b.field("id"))
                    .as("latest_b"));
    assertEquals("AQL/", parse(parserOf(latest), topHits("latest_b", "{\"b\":\"AQL/\"}")));
  }

  /**
   * A dedup fetches the whole _source and leaves vectors, and aliases to them, out of the fields
   * API, whose value would replace the vector's in the row.
   */
  @Test
  void dedupLeavesVectorsOutOfTheFieldsApi() {
    String dsl =
        AggregateAnalyzer.createTopHitsBuilder(
                dedupCall(),
                args("id", "v", "b", "al"),
                "dedup",
                helper(fieldTypes),
                1,
                false,
                false,
                null,
                null)
            .toString();
    assertTrue(dsl.contains("\"fields\":[{\"field\":\"id\"}]"), dsl);
    assertFalse(dsl.contains("\"_source\""), dsl);
  }

  /**
   * With a script column, a hit has no _source unless the request asks for it, so a dedup that
   * reads vectors, also through an alias, or objects asks for the whole _source.
   */
  @Test
  void dedupWithAScriptAsksForItsVectorsInSource() {
    try (Hook.Closeable ignored =
        Hook.CURRENT_TIME.addThread((Consumer<Holder<Long>>) h -> h.set(0L))) {
      RexBuilder rexBuilder = new RexBuilder(typeFactory);
      AggregateAnalyzer.AggregateBuilderHelper withCluster =
          new AggregateAnalyzer.AggregateBuilderHelper(
              rowType,
              fieldTypes,
              RelOptCluster.create(new VolcanoPlanner(), rexBuilder),
              true,
              1000);
      Pair<RexNode, String> script =
          Pair.of(rexBuilder.makeExactLiteral(java.math.BigDecimal.ONE), "x");
      List<Pair<RexNode, String>> withVectors = new java.util.ArrayList<>(args("id", "v", "al"));
      withVectors.add(script);
      String dsl =
          AggregateAnalyzer.createTopHitsBuilder(
                  dedupCall(), withVectors, "dedup", withCluster, 1, false, false, null, null)
              .toString();
      assertTrue(dsl.contains("\"_source\":true"), dsl);
      assertTrue(dsl.contains("\"fields\":[{\"field\":\"id\"}]"), dsl);
      // Without a vector, as before: no _source.
      String withoutVectors =
          AggregateAnalyzer.createTopHitsBuilder(
                  dedupCall(),
                  List.of(args("id").getFirst(), script),
                  "dedup",
                  withCluster,
                  1,
                  false,
                  false,
                  null,
                  null)
              .toString();
      assertFalse(withoutVectors.contains("\"_source\""), withoutVectors);
    }
  }

  /** An alias to a vector inside an object has no _source key a dedup row can map. */
  @Test
  void dedupOfAnAliasToAVectorInsideAnObjectIsNotPushed() {
    assertThrows(
        AggregateAnalyzer.AggregateAnalyzerException.class,
        () ->
            AggregateAnalyzer.createTopHitsBuilder(
                dedupCall(),
                args("id", "nal"),
                "dedup",
                helper(fieldTypes),
                1,
                false,
                false,
                null,
                null));
  }

  /**
   * A dedup row maps a field's value to every output column that reads the field, keeping the
   * column of the field's own name: the plan reads an alias or an eval copy as a reference to its
   * target, and an alias column to a vector reads its target's _source key.
   */
  @Test
  void dedupMapsAFieldToEveryColumnThatReadsIt() {
    RexNode v = RexInputRef.of(1, rowType);
    RexNode id = RexInputRef.of(0, rowType);
    assertEquals(
        Map.of("v", List.of("al", "v"), "id", List.of("id2", "id")),
        AggregateAnalyzer.dedupFieldNameMapping(
            List.of(Pair.of(v, "v"), Pair.of(v, "al"), Pair.of(id, "id"), Pair.of(id, "id2")),
            helper(fieldTypes)));
    // The alias column itself, as a scan reads it after a projection of it.
    assertEquals(
        Map.of("v", List.of("al")),
        AggregateAnalyzer.dedupFieldNameMapping(args("id", "al"), helper(fieldTypes)));
    // A rename alone still moves the value.
    assertEquals(
        Map.of("v", List.of("w")),
        AggregateAnalyzer.dedupFieldNameMapping(List.of(Pair.of(v, "w")), helper(fieldTypes)));
  }

  private Map<String, ExprType> mixedFieldTypes() {
    OpenSearchKnnVectorType mixed =
        OpenSearchKnnVectorType.of(3, VectorDataType.FLOAT, null, null, null)
            .inIndex("i0")
            .mergedWith(
                OpenSearchKnnVectorType.of(3, VectorDataType.BYTE, null, null, null).inIndex("i1"));
    Map<String, ExprType> types = new HashMap<>(fieldTypes);
    types.put("v", mixed);
    types.put("al", new OpenSearchAliasType("v", mixed));
    types.put("obj.vec", mixed);
    return types;
  }

  /**
   * The rows of a top_hits record the hit's index if it reads a field that a float index and a byte
   * index map, directly, through an alias, or inside an object or nested field.
   */
  @Test
  void rowsRecordTheHitIndexOnlyForMixedEncodingVectors() {
    AggregateAnalyzer.AggregateBuilderHelper mixed = helper(mixedFieldTypes());
    assertTrue(AggregateAnalyzer.readsMixedEncodingVector(args("v"), mixed, true));
    assertTrue(AggregateAnalyzer.readsMixedEncodingVector(args("al"), mixed, true));
    assertTrue(AggregateAnalyzer.readsMixedEncodingVector(args("id", "obj"), mixed, false));
    assertFalse(AggregateAnalyzer.readsMixedEncodingVector(args("id", "obj"), mixed, true));
    assertFalse(AggregateAnalyzer.readsMixedEncodingVector(args("id", "b"), mixed, false));
    AggregateAnalyzer.AggregateBuilderHelper plain = helper(fieldTypes);
    assertFalse(AggregateAnalyzer.readsMixedEncodingVector(args("v", "obj"), plain, false));
  }

  @Test
  void topHitsOverAMixedEncodingVectorRecordsTheHitIndex() throws ExpressionNotAnalyzableException {
    Function<RelBuilder, RelBuilder.AggCall> first =
        b -> b.aggregateCall(PPLBuiltinOperators.FIRST, b.field("v")).as("first_v");
    assertEquals(
        new MetricParserHelper(List.of(new TopHitsParser("first_v", true, false, null, true, "v"))),
        ((NoBucketAggregationParser) analyze(mixedFieldTypes(), "first_v", first).getRight())
            .getMetricsParser());
    assertEquals(
        new MetricParserHelper(
            List.of(new TopHitsParser("first_v", true, false, null, false, "v"))),
        ((NoBucketAggregationParser) analyze("first_v", first).getRight()).getMetricsParser());
    assertEquals(
        new ArgMaxMinParser("latest_v", "v", true),
        parserOf(
            analyze(
                mixedFieldTypes(),
                "latest_v",
                b ->
                    b.aggregateCall(SqlStdOperatorTable.ARG_MAX, b.field("v"), b.field("id"))
                        .as("latest_v"))));
  }

  /** A row of such a top_hits holds its hit's index under a key of the value's name. */
  @Test
  void rowsRecordTheHitIndexWhenAsked() {
    String response = topHits("first_v", "{\"v\":\"AQID\"}");
    assertEquals(
        List.of(Map.of("first_v", "AQID", TopHitsParser.HIT_INDEX_KEY + "first_v", "i1")),
        new TopHitsParser("first_v", true, false, null, true, "v")
            .parse(fromJson(response).get("first_v")));
    assertEquals(
        List.of(Map.of("latest_v", "AQID", TopHitsParser.HIT_INDEX_KEY + "latest_v", "i1")),
        new ArgMaxMinParser("latest_v", "v", true)
            .parse(fromJson(response.replace("first_v", "latest_v")).get("latest_v")));
    String dedup = response.replace("top_hits#first_v", "top_hits#dedup");
    assertEquals(
        List.of(Map.of("v", "AQID", TopHitsParser.HIT_INDEX_KEY, "i1")),
        new TopHitsParser("dedup", false, false, null, true).parse(fromJson(dedup).get("dedup")));
  }

  /**
   * A vector read from _source is read from there only: a meta field of the hit, such as {@code
   * _routing}, is in the hit's fields, and first, last, earliest and latest would otherwise return
   * it.
   */
  @Test
  void aVectorIsReadFromSourceOnlyWhateverTheHitsFields() {
    String routed =
        topHits("x", "{\"v\":[1,2]}")
            .replace("\"_id\":\"1\",", "\"_id\":\"1\",\"_routing\":\"r1\",");
    assertEquals(
        Set.of("_routing"),
        ((TopHits) fromJson(routed).get("x")).getHits().getHits()[0].getFields().keySet());
    assertEquals(
        List.of(1, 2), parse(new TopHitsParser("x", true, false, null, false, "v"), routed));
    assertEquals(List.of(1, 2), parse(new ArgMaxMinParser("x", "v", false), routed));
    // Without a source path, a field of the hit's fields is read as before.
    assertEquals("r1", parse(new ArgMaxMinParser("x"), routed));
  }
}
