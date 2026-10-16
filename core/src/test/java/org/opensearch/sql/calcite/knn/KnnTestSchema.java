/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.knn;

import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.TYPE_FACTORY;

import java.util.List;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelBuilder;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.SysLimit;
import org.opensearch.sql.calcite.type.ExprVectorType;
import org.opensearch.sql.data.type.VectorDataType;
import org.opensearch.sql.executor.QueryType;

/**
 * Tables for the k-NN planning tests. Like an OpenSearch index scan, each table ends with the
 * metadata columns {@code _id} and {@code _score}.
 *
 * <ul>
 *   <li>{@code products}: doc_id, name, price, category, embedding {@code VECTOR(3, FLOAT)}
 *   <li>{@code byte_vectors}: doc_id, embedding {@code VECTOR(3, BYTE)}
 *   <li>{@code binary_vectors}: doc_id, embedding {@code VECTOR(16, BINARY)}
 *   <li>{@code model_vectors}: doc_id, embedding {@code VECTOR(?, FLOAT)} (no dimension)
 *   <li>{@code docs}: doc_id, title (no vector)
 *   <li>{@code dimension_mismatch}, {@code data_type_mismatch}, {@code space_type_mismatch}:
 *       doc_id, embedding merged from indices {@code a} and {@code b} that map it with a different
 *       dimension (768 and 384), data type (float and half_float) or space type (l2 and
 *       innerproduct)
 *   <li>{@code unnamed_mismatch}: as {@code space_type_mismatch}, with the indices not known
 * </ul>
 */
final class KnnTestSchema {

  private KnnTestSchema() {}

  static CalcitePlanContext newContext() {
    SchemaPlus root = Frameworks.createRootSchema(true);
    root.add(
        "products",
        table(
            List.of("doc_id", "name", "price", "category", "embedding"),
            List.of(
                type(SqlTypeName.INTEGER),
                type(SqlTypeName.VARCHAR),
                type(SqlTypeName.DOUBLE),
                type(SqlTypeName.VARCHAR),
                TYPE_FACTORY.createVectorType(3, VectorDataType.FLOAT, "l2", true))));
    root.add("byte_vectors", vectorTable(3, VectorDataType.BYTE));
    root.add("binary_vectors", vectorTable(16, VectorDataType.BINARY));
    root.add("model_vectors", vectorTable(-1, VectorDataType.FLOAT));
    root.add("dimension_mismatch", mismatchTable(-1, null, List.of("a", "b")));
    root.add("data_type_mismatch", mismatchTable(3, "l2", List.of("a", "b")));
    root.add("space_type_mismatch", mismatchTable(3, null, List.of("a", "b")));
    root.add("unnamed_mismatch", mismatchTable(3, null, List.of()));
    root.add(
        "docs",
        table(
            List.of("doc_id", "title"),
            List.of(type(SqlTypeName.INTEGER), type(SqlTypeName.VARCHAR))));
    FrameworkConfig config =
        Frameworks.newConfigBuilder()
            .parserConfig(SqlParser.Config.DEFAULT)
            .defaultSchema(root)
            .traitDefs((List<RelTraitDef>) null)
            .programs(Programs.heuristicJoinOrder(Programs.RULE_SET, true, 2))
            .context(Contexts.of(RelBuilder.Config.DEFAULT))
            .build();
    return CalcitePlanContext.create(config, SysLimit.DEFAULT, QueryType.PPL);
  }

  static String explain(RelNode rel) {
    return RelOptUtil.toString(rel);
  }

  /** A table without rows that Calcite can still compile a plan over. */
  private abstract static class EmptyTable extends AbstractTable implements ScannableTable {
    @Override
    public Enumerable<@Nullable Object[]> scan(DataContext root) {
      return Linq4j.emptyEnumerable();
    }
  }

  private static AbstractTable vectorTable(int dimension, VectorDataType dataType) {
    return table(
        List.of("doc_id", "embedding"),
        List.of(
            type(SqlTypeName.INTEGER),
            TYPE_FACTORY.createVectorType(dimension, dataType, "l2", true)));
  }

  /** A float vector column merged from indices whose mappings of it mismatch. */
  private static AbstractTable mismatchTable(
      int dimension, @Nullable String spaceType, List<String> indices) {
    return table(
        List.of("doc_id", "embedding"),
        List.of(
            type(SqlTypeName.INTEGER),
            new ExprVectorType(
                type(SqlTypeName.REAL),
                true,
                dimension,
                VectorDataType.FLOAT,
                spaceType,
                new ExprVectorType.IndexMappings(true, indices, List.of()))));
  }

  private static RelDataType type(SqlTypeName name) {
    return TYPE_FACTORY.createTypeWithNullability(TYPE_FACTORY.createSqlType(name), true);
  }

  /**
   * An empty, scannable table with the given columns followed by the metadata columns _id and
   * _score.
   */
  private static AbstractTable table(List<String> names, List<RelDataType> types) {
    return new EmptyTable() {
      @Override
      public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        RelDataTypeFactory.Builder builder = TYPE_FACTORY.builder();
        for (int i = 0; i < names.size(); i++) {
          builder.add(names.get(i), types.get(i));
        }
        builder.add("_id", type(SqlTypeName.VARCHAR));
        builder.add("_score", type(SqlTypeName.REAL));
        return builder.build();
      }
    };
  }
}
