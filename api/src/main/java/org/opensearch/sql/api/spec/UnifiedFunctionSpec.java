/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.spec;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.type.SqlTypeName;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Central registry of language-specified function signatures (Unified Language Specification
 * layer). Each entry maps a function name to a canonical {@link ScalarFunction} with named required
 * parameters of type {@link SqlTypeName#ANY}.
 *
 * <p>This class defines <em>what functions exist</em> and their signatures. Function
 * <em>implementations</em> live in the Unified Execution Runtime (UER) layer — see {@link
 * org.opensearch.sql.api.function.UnifiedFunction} and {@link
 * org.opensearch.sql.api.function.UnifiedFunctionRepository}. For data-source-specific functions
 * (e.g., relevance search), execution is handled by adapter pushdown rules rather than UER.
 *
 * <p>Named parameters enable SQL named-argument syntax ({@code match(field => col, query =>
 * 'text')}) via Calcite's {@code ARGUMENT_ASSIGNMENT} operator. With fixed required parameters (no
 * optional params), <a href="https://issues.apache.org/jira/browse/CALCITE-5366">CALCITE-5366</a>
 * is avoided entirely.
 *
 * <p>Functions are registered globally on the root schema via {@link #registerAll(SchemaPlus)},
 * following the same pattern as Flink's {@code FlinkSqlOperatorTable} — engine-level primitives
 * available regardless of catalog. Pushdown rules enforce data-source capability at optimization
 * time.
 *
 * @see org.opensearch.sql.api.function.UnifiedFunction
 * @see org.opensearch.sql.api.function.UnifiedFunctionRepository
 */
// TODO: UnifiedFunctionRepository should resolve implementations for functions defined here,
//  rather than independently discovering from PPLBuiltinOperators. The spec is the source of
//  truth for what functions exist; UER provides how they execute. Decide whether to late-bind
//  UER implementations (ImplementableFunction) to spec-defined signatures for engine-independent
//  functions (e.g., upper, lower). Currently only data-source-specific functions (pushdown-only)
//  are registered here.
public final class UnifiedFunctionSpec {

  private UnifiedFunctionSpec() {}

  /** Single-field relevance function params: (field, query). */
  private static final List<String> SINGLE_FIELD_PARAMS = List.of("field", "query");

  /** Multi-field relevance function params: (fields, query). */
  private static final List<String> MULTI_FIELD_PARAMS = List.of("fields", "query");

  private static final Map<String, ScalarFunction> REGISTRY =
      Map.of(
          "match", scalarFunction(SINGLE_FIELD_PARAMS),
          "match_phrase", scalarFunction(SINGLE_FIELD_PARAMS),
          "match_bool_prefix", scalarFunction(SINGLE_FIELD_PARAMS),
          "match_phrase_prefix", scalarFunction(SINGLE_FIELD_PARAMS),
          "multi_match", scalarFunction(MULTI_FIELD_PARAMS),
          "simple_query_string", scalarFunction(MULTI_FIELD_PARAMS),
          "query_string", scalarFunction(MULTI_FIELD_PARAMS));

  /** Registers all language-specified functions on the given schema (typically root). */
  public static void registerAll(SchemaPlus schema) {
    REGISTRY.forEach(schema::add);
  }

  /** Returns the canonical ScalarFunction for a language-specified function, or null. */
  public static @Nullable ScalarFunction get(String name) {
    return REGISTRY.get(name);
  }

  /** Returns true if the name is a language-specified function. */
  public static boolean isLanguageFunction(String name) {
    return REGISTRY.containsKey(name);
  }

  /** All registered language function names. */
  public static Set<String> names() {
    return REGISTRY.keySet();
  }

  private static ScalarFunction scalarFunction(List<String> paramNames) {
    List<FunctionParameter> params =
        IntStream.range(0, paramNames.size())
            .mapToObj(i -> (FunctionParameter) new AnyParam(i, paramNames.get(i)))
            .toList();
    return new BooleanScalarFunction(params);
  }

  /** A ScalarFunction that returns BOOLEAN with the given parameters. */
  private record BooleanScalarFunction(List<FunctionParameter> params) implements ScalarFunction {
    @Override
    public List<FunctionParameter> getParameters() {
      return params;
    }

    @Override
    public RelDataType getReturnType(RelDataTypeFactory typeFactory) {
      return typeFactory.createSqlType(SqlTypeName.BOOLEAN);
    }
  }

  /** A required function parameter of type ANY. */
  private record AnyParam(int ordinal, String name) implements FunctionParameter {
    @Override
    public int getOrdinal() {
      return ordinal;
    }

    @Override
    public String getName() {
      return name;
    }

    @Override
    public boolean isOptional() {
      return false;
    }

    @Override
    public RelDataType getType(RelDataTypeFactory typeFactory) {
      return typeFactory.createSqlType(SqlTypeName.ANY);
    }
  }
}
