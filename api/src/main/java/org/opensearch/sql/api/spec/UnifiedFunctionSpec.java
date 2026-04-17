/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.spec;

import static org.apache.calcite.sql.type.ReturnTypes.BOOLEAN;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlOperandMetadata;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;

/**
 * Declarative registry of language-level functions for the unified query engine. Functions defined
 * here are part of the language spec — always resolvable regardless of the underlying data source.
 * They are grouped into {@link Category categories} that callers chain into Calcite's operator
 * table. Data-source capability is enforced at optimization time by pushdown rules.
 */
@Getter
@ToString(of = "funcName")
@EqualsAndHashCode(of = "funcName")
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class UnifiedFunctionSpec {

  /** Function name as registered in the operator table (e.g., "match", "multi_match"). */
  private final String funcName;

  /** Calcite operator for chaining into the framework config's operator table. */
  private final SqlOperator operator;

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
  private static final Map<String, UnifiedFunctionSpec> ALL_SPECS =
      Stream.of(RELEVANCE)
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

  public static Builder function(String name) {
    return new Builder(name);
  }

  /** Fluent builder for function specs. */
  @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
  public static class Builder {
    private final String funcName;
    private List<String> paramNames = List.of();
    private SqlReturnTypeInference returnType;

    public Builder vararg(String... names) {
      this.paramNames = List.of(names);
      return this;
    }

    public Builder returnType(SqlReturnTypeInference type) {
      this.returnType = type;
      return this;
    }

    public UnifiedFunctionSpec build() {
      Objects.requireNonNull(returnType, "returnType is required");
      return new UnifiedFunctionSpec(
          funcName,
          new SqlUserDefinedFunction(
              new SqlIdentifier(funcName, SqlParserPos.ZERO),
              SqlKind.OTHER_FUNCTION,
              returnType,
              InferTypes.ANY_NULLABLE,
              new VariadicOperandMetadata(paramNames),
              List::of)); // Pushdown-only: no local implementation
    }
  }

  /**
   * Custom operand metadata that bypasses Calcite's built-in type checking. Calcite's {@code
   * FamilyOperandTypeChecker} rejects variadic calls (CALCITE-5366), so this implementation accepts
   * any operand types and delegates validation to pushdown.
   */
  private record VariadicOperandMetadata(List<String> paramNames) implements SqlOperandMetadata {

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
