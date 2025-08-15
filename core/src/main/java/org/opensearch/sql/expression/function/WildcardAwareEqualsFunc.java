/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import java.util.List;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SameOperandTypeChecker;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.expression.function.PPLFuncImpTable.FunctionImp2;

/**
 * Wildcard-aware equals function that resolves to LIKE when wildcards are detected in string
 * values. Supports OpenSearch native wildcards: * (zero or more chars) and ? (exactly one char).
 * Also supports numeric fields by casting them to strings for wildcard matching.
 */
public class WildcardAwareEqualsFunc implements FunctionImp2 {

  @Override
  public RexNode resolve(RexBuilder builder, RexNode arg1, RexNode arg2) {
    // Check if the second argument is a string literal with wildcards
    if (arg2.isA(SqlKind.LITERAL) && SqlTypeFamily.CHARACTER.contains(arg2.getType())) {
      String value = ((RexLiteral) arg2).getValueAs(String.class);
      if (value != null && containsWildcards(value)) {
        if (SqlTypeFamily.CHARACTER.contains(arg1.getType())) {
          // Direct wildcard matching for string fields
          String convertedValue = convertOpenSearchWildcardsToSql(value);
          RexNode convertedLiteral = builder.makeLiteral(convertedValue);
          return builder.makeCall(
              SqlLibraryOperators.ILIKE, arg1, convertedLiteral, builder.makeLiteral("\\"));
        } else if (SqlTypeFamily.NUMERIC.contains(arg1.getType()) ||
                   SqlTypeFamily.DATETIME.contains(arg1.getType()) ||
                   arg1.getType().getSqlTypeName().getName().equals("IP")) {
          // For non-string fields with wildcards, we need to prevent incorrect wildcard pushdown
          // Cast to VARCHAR and use regular EQUALS with the pattern
          // This will filter in-memory, not push down as wildcard query
          RexNode castToString =
              builder.makeCast(builder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR), arg1);
          
          // Don't convert wildcards - keep them as-is for in-memory filtering
          // Using EQUALS instead of LIKE/ILIKE to prevent wildcard query generation
          return builder.makeCall(SqlStdOperatorTable.EQUALS, castToString, arg2);
        }
      }
    }

    // Fall back to standard equals for non-wildcard cases
    return builder.makeCall(SqlStdOperatorTable.EQUALS, arg1, arg2);
  }

  @Override
  public PPLTypeChecker getTypeChecker() {
    // Support standard same-type equality
    // For wildcard support, only allow STRING types
    // Numeric fields with wildcards should use explicit CAST
    return PPLTypeChecker.wrapUDT(List.of(
        // Standard same-type equality
        List.of(ExprCoreType.BOOLEAN, ExprCoreType.BOOLEAN),
        List.of(ExprCoreType.INTEGER, ExprCoreType.INTEGER),
        List.of(ExprCoreType.LONG, ExprCoreType.LONG),
        List.of(ExprCoreType.FLOAT, ExprCoreType.FLOAT),
        List.of(ExprCoreType.DOUBLE, ExprCoreType.DOUBLE),
        List.of(ExprCoreType.STRING, ExprCoreType.STRING),
        List.of(ExprCoreType.DATE, ExprCoreType.DATE),
        List.of(ExprCoreType.TIME, ExprCoreType.TIME),
        List.of(ExprCoreType.TIMESTAMP, ExprCoreType.TIMESTAMP),
        List.of(ExprCoreType.IP, ExprCoreType.IP)
        
        // REMOVED: Wildcard support for non-string types
        // This prevents incorrect wildcard query generation on numeric fields
        // Users should use: CAST(int_field AS STRING) = "1*"
    ));
  }

  protected boolean containsWildcards(String value) {
    return value.contains("*") || value.contains("?");
  }

  /**
   * Convert OpenSearch/Lucene wildcards to SQL LIKE wildcards. * (zero or more chars) -> % (zero or
   * more chars) ? (exactly one char) -> _ (exactly one char)
   */
  protected String convertOpenSearchWildcardsToSql(String value) {
    return value.replace("*", "%").replace("?", "_");
  }
}
