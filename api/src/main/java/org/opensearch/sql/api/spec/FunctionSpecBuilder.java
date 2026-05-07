/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.spec;

import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;
import javax.annotation.Nullable;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;

/**
 * Fluent DSL for building {@link UnifiedFunctionSpec} instances. Dispatches to specialized builders
 * based on the construction strategy:
 *
 * <ul>
 *   <li>{@link #delegateTo(SqlOperator)} — wraps an existing Calcite operator
 *   <li>{@link #vararg(String...)} — builds a pushdown-only UDF with relaxed type checking
 *   <li>{@link #operands(SqlTypeFamily...)} — builds a typed SqlFunction with strict operands
 * </ul>
 */
public class FunctionSpecBuilder {
  private final String name;

  FunctionSpecBuilder(String name) {
    this.name = name;
  }

  /** Wrap an existing Calcite operator (preserves native type system and RexImpTable impl). */
  public DelegateBuilder delegateTo(SqlOperator op) {
    return new DelegateBuilder(name, op);
  }

  /** Build a pushdown-only function with relaxed type checking. */
  public UdfBuilder vararg(String... paramNames) {
    return new UdfBuilder(name, List.of(paramNames));
  }

  /** Build a function with strict operand type checking. */
  public ScalarBuilder operands(SqlTypeFamily... families) {
    return new ScalarBuilder(name, families);
  }

  /** Wraps an existing Calcite operator as a function spec. */
  public static class DelegateBuilder {
    private final String name;
    private final SqlOperator operator;

    DelegateBuilder(String name, SqlOperator operator) {
      this.name = name;
      this.operator = operator;
    }

    public UnifiedFunctionSpec build() {
      return new UnifiedFunctionSpec(name.toLowerCase(), operator, null);
    }
  }

  /** Builds a pushdown-only SqlUserDefinedFunction with variadic operand metadata. */
  public static class UdfBuilder {
    private final String name;
    private final List<String> paramNames;
    private SqlReturnTypeInference returnType;

    UdfBuilder(String name, List<String> paramNames) {
      this.name = name;
      this.paramNames = paramNames;
    }

    /** Set return type inference. */
    public UdfBuilder returnType(SqlReturnTypeInference type) {
      this.returnType = type;
      return this;
    }

    public UnifiedFunctionSpec build() {
      Objects.requireNonNull(returnType, "returnType is required");
      return new UnifiedFunctionSpec(
          name,
          new SqlUserDefinedFunction(
              new SqlIdentifier(name, SqlParserPos.ZERO),
              SqlKind.OTHER_FUNCTION,
              returnType,
              InferTypes.ANY_NULLABLE,
              new UnifiedFunctionSpec.VariadicOperandMetadata(paramNames),
              List::of), // Pushdown-only: no local implementation
          null);
    }
  }

  /** Builds a SqlFunction with strict operand type families and optional late-binding impl. */
  public static class ScalarBuilder {
    private final String name;
    private final SqlTypeFamily[] operandFamilies;
    private SqlReturnTypeInference returnType;
    private SqlFunctionCategory category = SqlFunctionCategory.USER_DEFINED_FUNCTION;
    private @Nullable BiFunction<RexBuilder, RexCall, RexNode> impl;

    ScalarBuilder(String name, SqlTypeFamily[] operandFamilies) {
      this.name = name;
      this.operandFamilies = operandFamilies;
    }

    /** Set return type inference. */
    public ScalarBuilder returns(SqlReturnTypeInference type) {
      this.returnType = type;
      return this;
    }

    /** Set function category. */
    public ScalarBuilder category(SqlFunctionCategory cat) {
      this.category = cat;
      return this;
    }

    /**
     * Define how this function executes by rewriting to existing Calcite operators. Applied only at
     * compilation time (late binding) — the logical plan preserves the original function call.
     */
    public ScalarBuilder impl(BiFunction<RexBuilder, RexCall, RexNode> impl) {
      this.impl = impl;
      return this;
    }

    public UnifiedFunctionSpec build() {
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
}
