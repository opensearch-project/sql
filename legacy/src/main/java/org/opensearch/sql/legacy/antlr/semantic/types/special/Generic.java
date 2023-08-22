/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.antlr.semantic.types.special;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import org.opensearch.sql.legacy.antlr.semantic.types.Type;
import org.opensearch.sql.legacy.utils.StringUtils;

/** Generic type for more precise type expression */
public class Generic implements Type {

  /** Generic type placeholder namespace */
  private enum Name {
    T
  }

  /** Construct function to find generic type in argument list with same name */
  public static final Function<Type[], Type> T = types -> findSameGenericType(Name.T, types);

  /** Generic type name */
  private final Name name;

  /** Actual type binding to current generic type */
  private final Type binding;

  public Generic(Name name, Type type) {
    this.name = name;
    this.binding = type;
  }

  public static Type T(Type type) {
    return new Generic(Name.T, type);
  }

  /**
   * Return a function for replacing generic type in argument list with binding type. Ex. after T
   * instance found in argument list [T(NUMBER), STRING], create function to return actualTypes[0]
   *
   * @param func function for finding generic type in argument list (namely, function T above)
   * @param actualArgTypes actual argument types
   */
  public static Function<Type[], Type> specialize(
      Function<Type[], Type> func, Type[] actualArgTypes) {
    if (func != T) {
      return func;
    }

    Type genericType = func.apply(actualArgTypes);
    int genericTypeIndex = Arrays.asList(actualArgTypes).indexOf(genericType);
    return actualTypes -> actualTypes[genericTypeIndex];
  }

  /**
   * Find placeholder in argument list, ex. in [T(NUMBER), STRING] -> T, return instance at first T
   */
  private static Type findSameGenericType(Name name, Type[] types) {
    return Arrays.stream(types)
        .filter(type -> type instanceof Generic)
        .filter(type -> ((Generic) type).name == name)
        .findFirst()
        .orElseThrow(
            () ->
                new IllegalStateException(
                    StringUtils.format(
                        "Type definition is wrong. Could not unbind generic type [%s] in type list"
                            + " %s.",
                        name, types)));
  }

  @Override
  public String getName() {
    return this.name.name();
  }

  @Override
  public boolean isCompatible(Type other) {
    return binding.isCompatible(other);
  }

  @Override
  public Type construct(List<Type> others) {
    return binding.construct(others);
  }

  @Override
  public String usage() {
    return binding.usage() + " " + name;
  }
}
