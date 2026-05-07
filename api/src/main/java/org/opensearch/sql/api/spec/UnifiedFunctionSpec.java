/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.spec;

import static org.apache.calcite.sql.type.ReturnTypes.BOOLEAN;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlOperandMetadata;
import org.apache.calcite.sql.type.SqlTypeFamily;
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
public final class UnifiedFunctionSpec {

  /** Function name as registered in the operator table (e.g., "match", "multi_match"). */
  private final String funcName;

  /** Calcite operator for chaining into the framework config's operator table. */
  private final SqlOperator operator;

  /**
   * Optional Rex-level implementation that rewrites this function into executable RexNodes. Applied
   * only at compilation time (late binding) — the logical plan preserves the original function call
   * for consumers like the Analytics Engine.
   */
  private final @Nullable BiFunction<RexBuilder, RexCall, RexNode> impl;

  UnifiedFunctionSpec(
      String funcName,
      SqlOperator operator,
      @Nullable BiFunction<RexBuilder, RexCall, RexNode> impl) {
    this.funcName = funcName;
    this.operator = operator;
    this.impl = impl;
  }

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

  /** Common functions beyond ANSI standard (shared across SQL and PPL). */
  public static final Category LIBRARY =
      new Category(
          List.of(
              function("length").delegateTo(SqlLibraryOperators.LENGTH).build(),
              function("regexp_replace").delegateTo(SqlLibraryOperators.REGEXP_REPLACE_3).build(),
              function("date_trunc")
                  .operands(SqlTypeFamily.CHARACTER, SqlTypeFamily.DATETIME)
                  .returns(ReturnTypes.ARG1_NULLABLE)
                  .category(SqlFunctionCategory.TIMEDATE)
                  .build()));

  /** All registered function specs, keyed by function name. */
  private static final Map<String, UnifiedFunctionSpec> ALL_SPECS =
      Stream.of(RELEVANCE, LIBRARY)
          .flatMap(c -> c.specs().stream())
          .collect(Collectors.toMap(UnifiedFunctionSpec::getFuncName, s -> s));

  /**
   * Returns all specs that have a non-null impl, keyed by their operator. Used by pre-compilation
   * rules to bind function implementations at execution time.
   */
  public static Map<SqlOperator, BiFunction<RexBuilder, RexCall, RexNode>> implBindings() {
    return ALL_SPECS.values().stream()
        .filter(spec -> spec.impl != null)
        .collect(Collectors.toMap(spec -> spec.operator, spec -> spec.impl));
  }

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
  public static FunctionSpecBuilder function(String name) {
    return new FunctionSpecBuilder(name);
  }

  /**
   * Custom operand metadata that bypasses Calcite's built-in type checking. Calcite's {@code
   * FamilyOperandTypeChecker} rejects variadic calls (CALCITE-5366), so this implementation accepts
   * any operand types and delegates validation to pushdown.
   */
  record VariadicOperandMetadata(List<String> paramNames) implements SqlOperandMetadata {

    @Override
    public List<String> paramNames() {
      return paramNames;
    }

    @Override
    public List<RelDataType> paramTypes(RelDataTypeFactory tf) {
      return List.of();
    }

    @Override
    public boolean checkOperandTypes(SqlCallBinding binding, boolean throwOnFailure) {
      return true; // Bypass: CALCITE-5366 breaks optional argument type checking
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
      return SqlOperandCountRanges.from(paramNames.size());
    }

    @Override
    public String getAllowedSignatures(SqlOperator op, String opName) {
      return opName + "(" + String.join(", ", paramNames) + "[, option=value ...])";
    }
  }
}
