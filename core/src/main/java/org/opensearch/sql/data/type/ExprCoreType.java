/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.data.type;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** Expression Type. */
public enum ExprCoreType implements ExprType {
  /** Unknown due to unsupported data type. */
  UNKNOWN,

  /**
   * Undefined type for special literal such as NULL. As the root of data type tree, it is
   * compatible with any other type. In other word, undefined type is the "narrowest" type.
   */
  UNDEFINED,

  /** Numbers. */
  BYTE(UNDEFINED),
  SHORT(BYTE),
  INTEGER(SHORT),
  LONG(INTEGER),
  FLOAT(LONG),
  DOUBLE(FLOAT),

  /** String. */
  STRING(UNDEFINED),

  /** Boolean. */
  BOOLEAN(STRING),

  /** Date. */
  DATE(STRING),
  TIME(STRING),
  TIMESTAMP(STRING, DATE, TIME),
  INTERVAL(UNDEFINED),

  /** IP Address. */
  IP(STRING),

  /** Geometry. Only support point now. */
  GEO_POINT(UNDEFINED),

  BINARY(UNDEFINED),

  /** Struct. */
  STRUCT(UNDEFINED),

  /** Array. */
  ARRAY(UNDEFINED),

  /**
   * Dense vector, e.g. an OpenSearch {@code knn_vector} field. A sequence of numbers with a fixed
   * dimension; it has no order or equality semantics, so it cannot be compared, sorted or grouped.
   * Storage types that carry the dimension and element encoding implement {@link VectorFieldType}.
   */
  VECTOR(UNDEFINED);

  /** Parents (wider/compatible types) of current base type. */
  private final List<ExprType> parents = new ArrayList<>();

  /** The mapping between Type and legacy JDBC type name. */
  private static final Map<ExprCoreType, String> LEGACY_TYPE_NAME_MAPPING =
      new ImmutableMap.Builder<ExprCoreType, String>()
          .put(STRUCT, "OBJECT")
          .put(ARRAY, "NESTED")
          // SQL and JDBC-format clients keep the name a knn_vector column had as ARRAY.
          .put(VECTOR, "NESTED")
          .put(STRING, "KEYWORD")
          .build();

  private static final Set<ExprType> NUMBER_TYPES =
      new ImmutableSet.Builder<ExprType>()
          .add(BYTE)
          .add(SHORT)
          .add(INTEGER)
          .add(LONG)
          .add(FLOAT)
          .add(DOUBLE)
          .build();

  ExprCoreType(ExprCoreType... compatibleTypes) {
    for (ExprCoreType subType : compatibleTypes) {
      // for example: TIMESTAMP(STRING, DATE, TIME) and DATE(STRING) means
      // STRING's parents is TIMESTAMP and DATE
      // DATE's parent is TIMESTAMP
      // TIME's parent is TIMESTAMP
      subType.parents.add(this);
    }
  }

  @Override
  public List<ExprType> getParent() {
    return parents.isEmpty() ? ExprType.super.getParent() : parents;
  }

  @Override
  public String typeName() {
    return this.name();
  }

  @Override
  public String legacyTypeName() {
    return LEGACY_TYPE_NAME_MAPPING.getOrDefault(this, this.name());
  }

  /**
   * Return all the valid ExprCoreType of the V2 engine, which registers its functions over this
   * list. {@link #VECTOR} is not in it: it exists for the Calcite engine only, and the V2 engine
   * types a knn_vector field as {@link #ARRAY}, as it did before VECTOR was added. Keeping VECTOR
   * out keeps V2 signatures, and the signature lists in its error messages, unchanged.
   */
  public static List<ExprCoreType> coreTypes() {
    return v2Types().stream()
        .filter(type -> type != UNKNOWN)
        .filter(type -> type != UNDEFINED)
        .collect(Collectors.toList());
  }

  /**
   * All values the V2 engine knows, {@link #UNKNOWN} and {@link #UNDEFINED} included: {@link
   * #values()} without the Calcite-only {@link #VECTOR}. Use it instead of {@code values()} to
   * register V2 functions over every type.
   */
  public static List<ExprCoreType> v2Types() {
    return Arrays.stream(ExprCoreType.values())
        .filter(type -> type != VECTOR)
        .collect(Collectors.toList());
  }

  public static Set<ExprType> numberTypes() {
    return NUMBER_TYPES;
  }
}
