/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.spec.datetime;

import java.util.List;
import java.util.Optional;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.api.spec.LanguageSpec.LanguageExtension;
import org.opensearch.sql.api.spec.LanguageSpec.PostAnalysisRule;
import org.opensearch.sql.calcite.type.AbstractExprRelDataType;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.ExprUDT;

/**
 * Normalizes datetime UDT operations in the logical plan and casts remaining datetime output
 * columns to VARCHAR so the wire output matches PPL's String datetime contract.
 *
 * <p>Contributes two ordered post-analysis rules: {@link DatetimeUdtNormalizeRule} rewrites UDT
 * calls; {@link DatetimeUdtOutputCastRule} wraps the root with a varchar projection. The cast
 * depends on the normalized row type, so both rules live in a single extension to keep their
 * ordering encapsulated.
 */
public class DatetimeUdtExtension implements LanguageExtension {

  @Override
  public List<PostAnalysisRule> postAnalysisRules() {
    return List.of(new DatetimeUdtNormalizeRule(), new DatetimeUdtOutputCastRule());
  }

  /** Maps a datetime UDT to its standard Calcite equivalent with value conversion methods. */
  @Getter
  @RequiredArgsConstructor
  enum UdtMapping {
    DATE(ExprUDT.EXPR_DATE, SqlTypeName.DATE, "dateStringToUnixDate", "unixDateToString"),
    TIME(ExprUDT.EXPR_TIME, SqlTypeName.TIME, "timeStringToUnixDate", "unixTimeToString"),
    TIMESTAMP(
        ExprUDT.EXPR_TIMESTAMP,
        SqlTypeName.TIMESTAMP,
        "timestampStringToUnixDate",
        "unixTimestampToString");

    private final ExprUDT udtType;
    private final SqlTypeName stdType;
    private final String toStdMethod;
    private final String fromStdMethod;

    /** Matches a UDT type to its mapping. */
    static Optional<UdtMapping> fromUdtType(RelDataType type) {
      if (!(type instanceof AbstractExprRelDataType<?> e)) return Optional.empty();
      ExprUDT udt = e.getUdt();
      for (UdtMapping u : values()) {
        if (u.udtType == udt) return Optional.of(u);
      }
      return Optional.empty();
    }

    /** Matches a standard Calcite type to its mapping. */
    static Optional<UdtMapping> fromStdType(RelDataType type) {
      SqlTypeName name = type.getSqlTypeName();
      for (UdtMapping u : values()) {
        if (u.stdType == name) return Optional.of(u);
      }
      return Optional.empty();
    }

    RelDataType toStdType(RexBuilder rexBuilder, boolean nullable) {
      return rexBuilder
          .getTypeFactory()
          .createTypeWithNullability(rexBuilder.getTypeFactory().createSqlType(stdType), nullable);
    }

    /** UDT value (String) → standard value (int/long). */
    Expression toStdValue(Expression result) {
      return Expressions.call(
          DateTimeUtils.class, toStdMethod, Expressions.call(result, "toString"));
    }

    /** Standard value (int/long) → UDT value (String). */
    Expression fromStdValue(Expression operand) {
      return Expressions.call(DateTimeUtils.class, fromStdMethod, operand);
    }
  }
}
