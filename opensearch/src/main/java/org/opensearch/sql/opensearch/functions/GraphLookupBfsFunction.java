/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.functions;

import java.util.List;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.expression.function.ImplementorUDF;
import org.opensearch.sql.expression.function.UDFOperandMetadata;

/**
 * UDF wrapper for graph traversal BFS function.
 *
 * <p>Parameters:
 *
 * <ul>
 *   <li>startValue: Object - starting value for BFS
 *   <li>lookupData: List - collected rows from lookup table
 *   <li>connectFromIdx: int - index of connectFrom field
 *   <li>connectToIdx: int - index of connectTo field
 *   <li>maxDepth: int - max traversal depth (-1 = unlimited)
 *   <li>bidirectional: boolean - traverse both directions
 *   <li>includeDepth: boolean - include depth in output
 * </ul>
 *
 * <p>Returns: List - array of [row_fields..., depth?]
 */
public class GraphLookupBfsFunction extends ImplementorUDF {

  public GraphLookupBfsFunction() {
    super(new GraphLookupBfsImplementor(), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    // Return ARRAY<ANY> - actual struct type depends on lookup table schema
    return opBinding -> {
      var typeFactory = opBinding.getTypeFactory();
      var anyType = typeFactory.createSqlType(SqlTypeName.ANY);
      return typeFactory.createArrayType(anyType, -1);
    };
  }

  @Override
  public UDFOperandMetadata getOperandMetadata() {
    return null; // Accept any operand types
  }

  private static class GraphLookupBfsImplementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      // Args: startValue, lookupData, connectFromIdx, connectToIdx, maxDepth, bidirectional,
      // includeDepth
      return Expressions.call(
          Types.lookupMethod(
              GraphLookupFunction.class,
              "executeWithDynamicLookup",
              Object.class, // startValue
              RexSubQuery.class, // lookupData
              int.class, // connectFromIdx
              int.class, // connectToIdx
              int.class, // maxDepth
              boolean.class, // bidirectional
              boolean.class), // includeDepth
          translatedOperands);
    }
  }
}
