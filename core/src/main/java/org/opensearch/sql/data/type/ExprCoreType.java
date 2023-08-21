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

  /** Struct. */
  STRUCT(UNDEFINED),

  /** Array. */
  ARRAY(UNDEFINED);

  /** Parents (wider/compatible types) of current base type. */
  private final List<ExprType> parents = new ArrayList<>();

  /** The mapping between Type and legacy JDBC type name. */
  private static final Map<ExprCoreType, String> LEGACY_TYPE_NAME_MAPPING =
      new ImmutableMap.Builder<ExprCoreType, String>()
          .put(STRUCT, "OBJECT")
          .put(ARRAY, "NESTED")
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

  /** Return all the valid ExprCoreType. */
  public static List<ExprCoreType> coreTypes() {
    return Arrays.stream(ExprCoreType.values())
        .filter(type -> type != UNKNOWN)
        .filter(type -> type != UNDEFINED)
        .collect(Collectors.toList());
  }

  public static Set<ExprType> numberTypes() {
    return NUMBER_TYPES;
  }
}
