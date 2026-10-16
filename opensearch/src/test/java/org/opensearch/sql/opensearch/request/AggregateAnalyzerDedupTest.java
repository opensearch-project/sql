/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.request;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Holder;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;

/** The mapping of a dedup's top_hits fields to its output columns. */
class AggregateAnalyzerDedupTest {

  private final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

  private final RelDataType rowType =
      typeFactory.createStructType(
          List.of(
              typeFactory.createSqlType(SqlTypeName.VARCHAR),
              typeFactory.createSqlType(SqlTypeName.VARCHAR),
              typeFactory.createSqlType(SqlTypeName.VARCHAR),
              typeFactory.createMapType(
                  typeFactory.createSqlType(SqlTypeName.VARCHAR),
                  typeFactory.createSqlType(SqlTypeName.ANY)),
              typeFactory.createSqlType(SqlTypeName.VARCHAR)),
          List.of("g", "a", "b", "obj", "obj.a"));

  private final Map<String, ExprType> fieldTypes =
      Map.of(
          "g",
          ExprCoreType.STRING,
          "a",
          ExprCoreType.STRING,
          "b",
          ExprCoreType.STRING,
          "obj",
          ExprCoreType.STRUCT,
          "obj.a",
          ExprCoreType.STRING);

  private final AggregateAnalyzer.AggregateBuilderHelper helper =
      new AggregateAnalyzer.AggregateBuilderHelper(rowType, fieldTypes, null, true, 1000);

  private RexNode ref(String field) {
    return RexInputRef.of(rowType.getFieldNames().indexOf(field), rowType);
  }

  /** The plan reads an alias or an eval copy as a reference to the field it reads. */
  @Test
  void aColumnThatKeepsItsFieldIsOneOfItsColumns() {
    assertEquals(
        Map.of("a", List.of("a2", "a")),
        AggregateAnalyzer.dedupFieldNameMapping(
            List.of(Pair.of(ref("g"), "g"), Pair.of(ref("a"), "a"), Pair.of(ref("a"), "a2")),
            helper));
    // A column that keeps its field's name alone needs no mapping.
    assertEquals(
        Map.of(),
        AggregateAnalyzer.dedupFieldNameMapping(
            List.of(Pair.of(ref("g"), "g"), Pair.of(ref("a"), "a")), helper));
  }

  /**
   * The computed columns, under their own names unless a fetched field has the name: {@code rename
   * a as j | eval a = 1, c = 2} fetches a for j.
   */
  @Test
  void scriptFieldsAreTheComputedColumns() {
    RexNode one = new RexBuilder(typeFactory).makeExactLiteral(BigDecimal.ONE);
    assertEquals(
        Map.of("a#script", "a", "c", "c"),
        AggregateAnalyzer.dedupScriptFields(
            List.of(Pair.of(ref("a"), "j"), Pair.of(one, "a"), Pair.of(one, "c")), helper));
  }

  /**
   * A new name is none of the fetched fields, the output columns and the other scripts: {@code
   * rename a as j | eval a = 1, `a#script` = 2, `a#script2` = 3, b = 4} with b fetched for another
   * column.
   */
  @Test
  void scriptFieldNamesAreUnique() {
    RexNode one = new RexBuilder(typeFactory).makeExactLiteral(BigDecimal.ONE);
    assertEquals(
        Map.of("a", "a#script3", "a#script", "a#script", "a#script2", "a#script2", "b", "b#script"),
        AggregateAnalyzer.scriptFieldNames(
            List.of(
                Pair.of(ref("a"), "j"),
                Pair.of(ref("b"), "k"),
                Pair.of(one, "a"),
                Pair.of(one, "a#script"),
                Pair.of(one, "a#script2"),
                Pair.of(one, "b")),
            helper));
  }

  private String dedupRequest(List<Pair<RexNode, String>> args) {
    // A script reads the time of the query.
    try (Hook.Closeable ignored =
        Hook.CURRENT_TIME.addThread((Consumer<Holder<Long>>) h -> h.set(0L))) {
      RelOptCluster cluster =
          RelOptCluster.create(new VolcanoPlanner(), new RexBuilder(typeFactory));
      AggregateAnalyzer.AggregateBuilderHelper withCluster =
          new AggregateAnalyzer.AggregateBuilderHelper(rowType, fieldTypes, cluster, true, 1000);
      AggregateCall call =
          AggregateCall.create(
              SqlStdOperatorTable.COUNT,
              false,
              false,
              false,
              List.of(),
              List.of(),
              -1,
              null,
              RelCollations.EMPTY,
              typeFactory.createSqlType(SqlTypeName.INTEGER),
              "dedup");
      return AggregateAnalyzer.createTopHitsBuilder(
              call, args, "dedup", withCluster, 1, false, false, null, null)
          .toString();
    }
  }

  /**
   * With script fields, a hit has no _source unless the request asks for it, and a dedup reads its
   * object columns, and the fields inside objects, from _source.
   */
  @Test
  void dedupWithScriptsAndObjectsAsksForSource() {
    RexNode one = new RexBuilder(typeFactory).makeExactLiteral(BigDecimal.ONE);
    String withObject =
        dedupRequest(
            List.of(Pair.of(ref("g"), "g"), Pair.of(ref("obj"), "obj"), Pair.of(one, "x")));
    assertTrue(withObject.contains("\"_source\":true"), withObject);
    String withoutObject = dedupRequest(List.of(Pair.of(ref("g"), "g"), Pair.of(one, "x")));
    assertFalse(withoutObject.contains("\"_source\""), withoutObject);
    // A field inside an object is read at its path in _source too.
    String withField =
        dedupRequest(
            List.of(Pair.of(ref("g"), "g"), Pair.of(ref("obj.a"), "obj.a"), Pair.of(one, "x")));
    assertTrue(withField.contains("\"_source\":true"), withField);
  }

  /** A script column named like a fetched field is returned under another name. */
  @Test
  void scriptColumnNamedLikeAFetchedFieldIsRenamedInTheRequest() {
    RexNode one = new RexBuilder(typeFactory).makeExactLiteral(BigDecimal.ONE);
    String request = dedupRequest(List.of(Pair.of(ref("a"), "j"), Pair.of(one, "a")));
    assertTrue(request.contains("\"script_fields\":{\"a#script\""), request);
  }
}
