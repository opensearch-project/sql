/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.calcite.adapter.enumerable.RexImpTable;
import org.apache.calcite.sql.SqlOperator;

/**
 * Determines, from the Java methods Calcite generates calls to, which operand positions of a
 * Calcite built-in operator need a Java {@code int}.
 *
 * <p>PPL widens integer arithmetic to BIGINT (#5603), so an argument such as {@code 1 + 1} reaches
 * code generation as a {@code long}. Calcite resolves the runtime method by name and argument types
 * and never narrows {@code long} to {@code int}, so the call fails to compile when every overload
 * takes an {@code int} at that position (e.g. {@code SqlFunctions.left(String, int)}). Positions
 * where some overload accepts a {@code long} (e.g. {@code substring(String, long, long)}) need no
 * narrowing.
 *
 * <p>The runtime methods are read from the implementor that {@link RexImpTable} registers for the
 * operator. That implementor is internal to Calcite, so its fields are read reflectively; when they
 * cannot be read (or the implementor exposes no method, as for {@code ITEM}) the result is empty
 * and callers fall back to the operator's declared signature.
 */
final class CalciteIntParameters {

  private record Key(SqlOperator operator, int arity) {}

  /** Parameter types to which code generation can pass a {@code long} or {@code Long}. */
  private static final Set<Class<?>> ACCEPTS_LONG =
      Set.of(long.class, Long.class, float.class, double.class, Object.class, BigDecimal.class);

  private static final Map<Key, Optional<boolean[]>> CACHE = new ConcurrentHashMap<>();

  private CalciteIntParameters() {}

  /**
   * Returns, for each operand position of a call to {@code operator} with {@code arity} operands,
   * whether a BIGINT operand there must be narrowed to INTEGER: some runtime overload takes an
   * {@code int} at that position and none accepts a {@code long}. Returns empty when the runtime
   * methods cannot be determined.
   */
  static Optional<boolean[]> narrowablePositions(SqlOperator operator, int arity) {
    return CACHE.computeIfAbsent(new Key(operator, arity), CalciteIntParameters::compute);
  }

  private static Optional<boolean[]> compute(Key key) {
    List<Method> overloads = new ArrayList<>();
    for (Method method : implementationMethods(key.operator())) {
      for (Method candidate : method.getDeclaringClass().getMethods()) {
        if (candidate.getName().equals(method.getName())
            && candidate.getParameterCount() == key.arity()
            && !candidate.isVarArgs()) {
          overloads.add(candidate);
        }
      }
    }
    if (overloads.isEmpty()) {
      return Optional.empty();
    }
    boolean[] narrowable = new boolean[key.arity()];
    for (int pos = 0; pos < key.arity(); pos++) {
      boolean acceptsLong = false;
      boolean acceptsInt = false;
      for (Method overload : overloads) {
        Class<?> parameter = overload.getParameterTypes()[pos];
        acceptsLong |= ACCEPTS_LONG.contains(parameter);
        acceptsInt |= parameter == int.class || parameter == Integer.class;
      }
      narrowable[pos] = acceptsInt && !acceptsLong;
    }
    return Optional.of(narrowable);
  }

  /** Methods held by the implementor Calcite registers for {@code operator}, if readable. */
  private static List<Method> implementationMethods(SqlOperator operator) {
    Object implementor;
    try {
      implementor = RexImpTable.INSTANCE.get(operator);
    } catch (RuntimeException e) {
      return List.of();
    }
    List<Method> methods = new ArrayList<>();
    if (implementor == null) {
      return methods;
    }
    for (Class<?> type = implementor.getClass();
        type != null && type.getName().startsWith("org.apache.calcite.");
        type = type.getSuperclass()) {
      for (Field field : type.getDeclaredFields()) {
        if (Modifier.isStatic(field.getModifiers())) {
          continue;
        }
        Object value;
        try {
          field.setAccessible(true);
          value = field.get(implementor);
        } catch (ReflectiveOperationException | RuntimeException e) {
          continue;
        }
        if (value instanceof Method method) {
          methods.add(method);
        } else if (value instanceof Collection<?> values) {
          for (Object element : values) {
            if (element instanceof Method method) {
              methods.add(method);
            }
          }
        }
      }
    }
    return methods;
  }
}
