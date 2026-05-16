/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.spec;

import static org.apache.calcite.sql.SqlFunctionCategory.TIMEDATE;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.LENGTH;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.REGEXP_REPLACE_3;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.FLOOR;
import static org.apache.calcite.sql.type.ReturnTypes.ARG1_NULLABLE;
import static org.apache.calcite.sql.type.ReturnTypes.BOOLEAN;
import static org.apache.calcite.sql.type.SqlTypeFamily.CHARACTER;
import static org.apache.calcite.sql.type.SqlTypeFamily.DATETIME;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.type.SqlOperandMetadata;
import org.apache.calcite.sql.util.SqlOperatorTables;

/**
 * Declarative registry of language-level functions for the unified query engine. Functions defined
 * here are part of the language spec — always resolvable regardless of the underlying data source.
 * They are grouped into {@link Category categories} that callers chain into Calcite's operator
 * table. Data-source capability is enforced at optimization time by pushdown rules.
 */
@Getter
@ToString(of = "funcName")
@EqualsAndHashCode(of = "funcName")
@AllArgsConstructor(access = AccessLevel.PACKAGE)
public final class UnifiedFunctionSpec {

  /** Function name as registered in the operator table (e.g., "match", "multi_match"). */
  private final String funcName;

  /** Calcite operator for chaining into the framework config's operator table. */
  private final SqlOperator operator;

  /** Optional late-binding implementation applied only at compilation time. */
  private final @Nullable BiFunction<RexBuilder, RexCall, RexNode> impl;

  /** Common scalar functions beyond standard. */
  public static final Category SCALAR =
      new Category(
          List.of(
              function("length").delegateTo(LENGTH).build(),
              function("regexp_replace").delegateTo(REGEXP_REPLACE_3).build(),
              function("date_trunc")
                  .operands(CHARACTER, DATETIME)
                  .returns(ARG1_NULLABLE)
                  .category(TIMEDATE)
                  .impl(
                      (rexBuilder, call) -> {
                        RexLiteral unitLiteral = (RexLiteral) call.operands.get(0);
                        String unit = unitLiteral.getValueAs(String.class);
                        RexNode datetime = call.operands.get(1);
                        return rexBuilder.makeCall(
                            FLOOR,
                            datetime,
                            rexBuilder.makeFlag(TimeUnitRange.valueOf(unit.toUpperCase())));
                      })
                  .build()));

  /** Full-text search functions. */
  public static final Category RELEVANCE =
      new Category(
          List.of(
              function("match").vararg("field", "query").returnType(BOOLEAN).build(),
              function("match_phrase").vararg("field", "query").returnType(BOOLEAN).build(),
              function("match_bool_prefix").vararg("field", "query").returnType(BOOLEAN).build(),
              function("match_phrase_prefix").vararg("field", "query").returnType(BOOLEAN).build(),
              function("multi_match").vararg("fields", "query").returnType(BOOLEAN).build(),
              function("simple_query_string").vararg("fields", "query").returnType(BOOLEAN).build(),
              function("query_string").vararg("fields", "query").returnType(BOOLEAN).build()));

  /** All registered function specs, keyed by function name. */
  public static final Map<String, UnifiedFunctionSpec> ALL_SPECS =
      Stream.of(SCALAR, RELEVANCE)
          .flatMap(c -> c.specs().stream())
          .collect(Collectors.toMap(UnifiedFunctionSpec::getFuncName, s -> s));

  /**
   * Looks up a function spec by name across all categories.
   *
   * @param name function name (case-insensitive)
   * @return the spec, or empty if not found
   */
  public static Optional<UnifiedFunctionSpec> of(String name) {
    return Optional.ofNullable(ALL_SPECS.get(name.toLowerCase()));
  }

  /**
   * @return required param names from {@link SqlOperandMetadata}, or empty if not available.
   */
  public List<String> getParamNames() {
    return operator.getOperandTypeChecker() instanceof SqlOperandMetadata metadata
        ? metadata.paramNames()
        : List.of();
  }

  /** A group of function specs that can be chained into Calcite's operator table. */
  public record Category(List<UnifiedFunctionSpec> specs) {
    public SqlOperatorTable operatorTable() {
      return SqlOperatorTables.of(specs.stream().map(UnifiedFunctionSpec::getOperator).toList());
    }

    /** Returns true if this category contains the given spec. */
    public boolean contains(UnifiedFunctionSpec spec) {
      return specs.contains(spec);
    }
  }

  /** Entry point for the function spec builder DSL. */
  private static FunctionSpecBuilder function(String name) {
    return new FunctionSpecBuilder(name);
  }
}
