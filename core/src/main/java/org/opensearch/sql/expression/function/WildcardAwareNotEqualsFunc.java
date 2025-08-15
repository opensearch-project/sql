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
 * Wildcard-aware not-equals function that resolves to NOT LIKE when wildcards are detected. Also
 * supports numeric fields by casting them to strings for wildcard matching.
 */
public class WildcardAwareNotEqualsFunc implements FunctionImp2 {

  @Override
  public RexNode resolve(RexBuilder builder, RexNode arg1, RexNode arg2) {
    // Check if the second argument is a string literal with wildcards
    if (arg2.isA(SqlKind.LITERAL) && SqlTypeFamily.CHARACTER.contains(arg2.getType())) {
      String value = ((RexLiteral) arg2).getValueAs(String.class);
      if (value != null && containsWildcards(value)) {
        // ONLY handle character/string types with wildcards
        // For numeric, IP, date etc., users should use explicit CAST
        if (SqlTypeFamily.CHARACTER.contains(arg1.getType())) {
          // Direct wildcard matching for string fields
          String convertedValue = convertOpenSearchWildcardsToSql(value);
          RexNode convertedLiteral = builder.makeLiteral(convertedValue);
          // Convert to NOT LIKE query for wildcard matching
          return builder.makeCall(
              SqlStdOperatorTable.NOT,
              builder.makeCall(
                  SqlLibraryOperators.ILIKE, arg1, convertedLiteral, builder.makeLiteral("\\")));
        }
        // For non-character types with wildcards, don't handle here
        // This will cause a type error, which is correct behavior
        // Users should explicitly CAST numeric fields to string for wildcard matching
      }
    }

    // Fall back to standard not-equals for non-wildcard cases
    return builder.makeCall(SqlStdOperatorTable.NOT_EQUALS, arg1, arg2);
  }

  @Override
  public PPLTypeChecker getTypeChecker() {
    // Support standard same-type not-equality
    // For wildcard support, only allow STRING types
    // Numeric fields with wildcards should use explicit CAST
    return PPLTypeChecker.wrapUDT(List.of(
        // Standard same-type not-equality
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
        // Users should use: CAST(int_field AS STRING) != "1*"
    ));
  }

  private boolean containsWildcards(String value) {
    return value.contains("*") || value.contains("?");
  }

  /**
   * Convert OpenSearch/Lucene wildcards to SQL LIKE wildcards. * (zero or more chars) -> % (zero or
   * more chars) ? (exactly one char) -> _ (exactly one char)
   */
  private String convertOpenSearchWildcardsToSql(String value) {
    return value.replace("*", "%").replace("?", "_");
  }
}
