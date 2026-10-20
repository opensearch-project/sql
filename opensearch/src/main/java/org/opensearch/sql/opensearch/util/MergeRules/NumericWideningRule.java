/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.util.MergeRules;

import java.util.List;
import java.util.Map;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType.MappingType;

/**
 * Merge rule for a field mapped as two different numeric types across indices. Two integer types
 * merge to the wider one, half_float and float merge to float, and any other pair merges to double.
 * When every index maps the field to a numeric type, the merged type does not depend on the order
 * the indices merge in. Unlike WideningTypeRule in core, which picks float, an integer paired with
 * half_float or float widens to double. Only a long beyond 2^53 next to a floating point type loses
 * precision.
 *
 * <p>See GitHub issue #5901.
 */
public class NumericWideningRule implements MergeRule {

  /** Integer types from narrowest to widest. */
  private static final List<MappingType> INTEGER_TYPES_BY_WIDTH =
      List.of(MappingType.Byte, MappingType.Short, MappingType.Integer, MappingType.Long);

  /** Mapping types whose ExprCoreType is FLOAT. */
  private static final List<MappingType> FLOAT_TYPES =
      List.of(MappingType.HalfFloat, MappingType.Float);

  /** Mapping types whose ExprCoreType is DOUBLE. */
  private static final List<MappingType> DOUBLE_TYPES =
      List.of(MappingType.ScaledFloat, MappingType.Double);

  @Override
  public boolean isMatch(OpenSearchDataType source, OpenSearchDataType target) {
    if (source == null || target == null) {
      return false;
    }
    MappingType sourceMapping = source.getMappingType();
    MappingType targetMapping = target.getMappingType();
    // A type built from an ExprCoreType, such as an alias, has no mapping type.
    if (sourceMapping == null || targetMapping == null) {
      return false;
    }
    return isNumeric(sourceMapping) && isNumeric(targetMapping) && sourceMapping != targetMapping;
  }

  @Override
  public void mergeInto(
      String key, OpenSearchDataType source, Map<String, OpenSearchDataType> target) {
    MappingType widened = widenedType(source.getMappingType(), target.get(key).getMappingType());
    target.put(key, OpenSearchDataType.of(widened));
  }

  private static MappingType widenedType(MappingType a, MappingType b) {
    if (INTEGER_TYPES_BY_WIDTH.contains(a) && INTEGER_TYPES_BY_WIDTH.contains(b)) {
      return INTEGER_TYPES_BY_WIDTH.indexOf(a) > INTEGER_TYPES_BY_WIDTH.indexOf(b) ? a : b;
    }
    if (FLOAT_TYPES.contains(a) && FLOAT_TYPES.contains(b)) {
      return MappingType.Float;
    }
    // Every other pair merges to double. A 32-bit float holds integers exactly only up to 2^24,
    // so an integer next to a half_float or float lands here too.
    return MappingType.Double;
  }

  private static boolean isNumeric(MappingType mappingType) {
    return INTEGER_TYPES_BY_WIDTH.contains(mappingType)
        || FLOAT_TYPES.contains(mappingType)
        || DOUBLE_TYPES.contains(mappingType);
  }
}
