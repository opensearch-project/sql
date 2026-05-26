/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.spec;

import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlOperandMetadata;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;

/** Fluent DSL for building {@link UnifiedFunctionSpec} instances. */
@RequiredArgsConstructor
class FunctionSpecBuilder {
  /** Function name to register. */
  private final String name;

  /**
   * Wraps an existing Calcite operator, preserving its native type system and RexImpTable
   * implementation for in-memory execution.
   *
   * @param op the Calcite operator to delegate to
   * @return a builder that produces the spec on {@code build()}
   */
  DelegateFunctionBuilder delegateTo(SqlOperator op) {
    return new DelegateFunctionBuilder(name, op);
  }

  /**
   * Builds a pushdown-only UDF with relaxed type checking. The resulting function has no local
   * implementation and delegates execution to the data source via pushdown.
   *
   * @param paramNames required parameter names for signature display
   * @return a builder that produces the spec on {@code build()}
   */
  CatalogFunctionBuilder vararg(String... paramNames) {
    return new CatalogFunctionBuilder(name, List.of(paramNames));
  }

  /**
   * Builds a typed SqlFunction with strict operand type checking. Optionally accepts a late-binding
   * {@code impl} that rewrites the function into executable Calcite expressions at compilation
   * time.
   *
   * @param families operand type families for validation
   * @return a builder that produces the spec on {@code build()}
   */
  DefaultFunctionBuilder operands(SqlTypeFamily... families) {
    return new DefaultFunctionBuilder(name, families);
  }

  @RequiredArgsConstructor
  static class DefaultFunctionBuilder {
    private final String name;
    private final SqlTypeFamily[] operandFamilies;
    private SqlReturnTypeInference returnType;
    private SqlFunctionCategory category = SqlFunctionCategory.USER_DEFINED_FUNCTION;
    private @Nullable BiFunction<RexBuilder, RexCall, RexNode> impl;

    DefaultFunctionBuilder returns(SqlReturnTypeInference type) {
      this.returnType = type;
      return this;
    }

    DefaultFunctionBuilder category(SqlFunctionCategory cat) {
      this.category = cat;
      return this;
    }

    /**
     * Defines how this function executes by rewriting to existing Calcite operators. Applied only
     * at compilation time (late binding) — the logical plan preserves the original function call.
     *
     * @param impl rewrite function that converts this call into executable RexNodes
     * @return this builder
     */
    DefaultFunctionBuilder impl(BiFunction<RexBuilder, RexCall, RexNode> impl) {
      this.impl = impl;
      return this;
    }

    UnifiedFunctionSpec build() {
      Objects.requireNonNull(returnType, "returns() is required");
      SqlFunction op =
          new SqlFunction(
              name.toUpperCase(),
              SqlKind.OTHER_FUNCTION,
              returnType,
              null,
              OperandTypes.family(operandFamilies),
              category);
      return new UnifiedFunctionSpec(name.toLowerCase(), op, impl);
    }
  }

  @RequiredArgsConstructor
  static class DelegateFunctionBuilder {
    private final String name;
    private final SqlOperator operator;

    UnifiedFunctionSpec build() {
      return new UnifiedFunctionSpec(name.toLowerCase(), operator, null);
    }
  }

  @RequiredArgsConstructor
  static class CatalogFunctionBuilder {
    private final String name;
    private final List<String> paramNames;
    private SqlReturnTypeInference returnType;

    CatalogFunctionBuilder returnType(SqlReturnTypeInference type) {
      this.returnType = type;
      return this;
    }

    UnifiedFunctionSpec build() {
      Objects.requireNonNull(returnType, "returnType is required");
      return new UnifiedFunctionSpec(
          name,
          new SqlUserDefinedFunction(
              new SqlIdentifier(name, SqlParserPos.ZERO),
              SqlKind.OTHER_FUNCTION,
              returnType,
              InferTypes.ANY_NULLABLE,
              new VariadicOperandMetadata(paramNames),
              List::of), // Pushdown-only: no local implementation
          null);
    }
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
      return true;
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
