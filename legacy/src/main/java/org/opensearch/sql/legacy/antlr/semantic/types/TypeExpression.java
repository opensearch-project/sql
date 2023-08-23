/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.antlr.semantic.types;

import static org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType.TYPE_ERROR;
import static org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType.UNKNOWN;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.opensearch.sql.legacy.antlr.semantic.types.special.Generic;
import org.opensearch.sql.legacy.utils.StringUtils;

/**
 * Type expression representing specification(s) of constructor such as function, operator etc. Type
 * expression has to be an interface with default methods because most subclass needs to be Enum.
 */
public interface TypeExpression extends Type {

  @Override
  default Type construct(List<Type> actualArgs) {
    TypeExpressionSpec[] specifications = specifications();
    if (specifications.length == 0) {
      // Empty spec means type check for this type expression is not implemented yet.
      // Return this to be compatible with everything.
      return UNKNOWN;
    }

    // Create a temp specification for compatibility check.
    TypeExpressionSpec actualSpec = new TypeExpressionSpec();
    actualSpec.argTypes = actualArgs.toArray(new Type[0]);

    // Perform compatibility check between actual spec (argument types) and expected.
    // If found any compatible spec, it means actual spec is legal and thus apply to get result
    // type.
    // Ex. Actual=[INTEGER], Specs=[NUMBER->NUMBER], [STRING->NUMBER]. So first spec matches and
    // return NUMBER.
    for (TypeExpressionSpec spec : specifications) {
      if (spec.isCompatible(actualSpec)) {
        return spec.constructFunc.apply(actualArgs.toArray(new Type[0]));
      }
    }
    return TYPE_ERROR;
  }

  @Override
  default String usage() {
    return Arrays.stream(specifications())
        .map(spec -> getName() + spec)
        .collect(Collectors.joining(" or "));
  }

  /**
   * Each type expression may be overloaded and include multiple specifications.
   *
   * @return all valid specifications or empty which means not implemented yet
   */
  TypeExpressionSpec[] specifications();

  /**
   * A specification is combination of a construct function and arg types for a type expression
   * (represent a constructor)
   */
  class TypeExpressionSpec {
    Type[] argTypes;
    Function<Type[], Type> constructFunc;

    public TypeExpressionSpec map(Type... args) {
      this.argTypes = args;
      return this;
    }

    public TypeExpressionSpec to(Function<Type[], Type> constructFunc) {
      // Required for generic type to replace placeholder ex.T with actual position in argument
      // list.
      // So construct function of generic type can return binding type finally.
      this.constructFunc = Generic.specialize(constructFunc, argTypes);
      return this;
    }

    /** Return a base type no matter what's the arg types Mostly this is used for empty arg types */
    public TypeExpressionSpec to(Type returnType) {
      this.constructFunc = x -> returnType;
      return this;
    }

    public boolean isCompatible(TypeExpressionSpec otherSpec) {
      Type[] expectArgTypes = this.argTypes;
      Type[] actualArgTypes = otherSpec.argTypes;

      // Check if arg numbers exactly match
      if (expectArgTypes.length != actualArgTypes.length) {
        return false;
      }

      // Check if all arg types are compatible
      for (int i = 0; i < expectArgTypes.length; i++) {
        if (!expectArgTypes[i].isCompatible(actualArgTypes[i])) {
          return false;
        }
      }
      return true;
    }

    @Override
    public String toString() {
      String argTypesStr =
          Arrays.stream(argTypes).map(Type::usage).collect(Collectors.joining(", "));

      // Only show generic type name in return value for clarity
      Type returnType = constructFunc.apply(argTypes);
      String returnTypeStr =
          (returnType instanceof Generic) ? returnType.getName() : returnType.usage();

      return StringUtils.format("(%s) -> %s", argTypesStr, returnTypeStr);
    }
  }
}
