/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.spec.datetime;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.api.spec.LanguageSpec.LanguageExtension;
import org.opensearch.sql.calcite.type.AbstractExprRelDataType;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.ExprUDT;

/** Datetime language extension that normalizes UDT types and casts output for wire-format. */
public class DatetimeExtension implements LanguageExtension {

  @Override
  public List<RelShuttle> postAnalysisRules() {
    return List.of(DatetimeUdtNormalizeRule.INSTANCE, DatetimeOutputCastRule.INSTANCE);
  }

  /** Maps datetime UDT types to their standard Calcite equivalents. */
  @Getter
  @RequiredArgsConstructor
  enum UdtMapping {
    DATE(ExprUDT.EXPR_DATE, SqlTypeName.DATE),
    TIME(ExprUDT.EXPR_TIME, SqlTypeName.TIME),
    TIMESTAMP(ExprUDT.EXPR_TIMESTAMP, SqlTypeName.TIMESTAMP);

    private final ExprUDT udtType;
    private final SqlTypeName stdType;

    /** Matches a UDT RelDataType to its mapping, or empty if not a datetime UDT. */
    static Optional<UdtMapping> fromUdtType(RelDataType type) {
      if (!(type instanceof AbstractExprRelDataType<?> e)) {
        return Optional.empty();
      }
      ExprUDT udt = e.getUdt();
      return Arrays.stream(values()).filter(u -> u.udtType == udt).findFirst();
    }

    /** Returns true if the given SqlTypeName is a standard datetime type. */
    static boolean isDatetimeType(SqlTypeName typeName) {
      return Arrays.stream(values()).anyMatch(u -> u.stdType == typeName);
    }
  }
}
