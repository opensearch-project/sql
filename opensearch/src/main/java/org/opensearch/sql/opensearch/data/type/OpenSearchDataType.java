/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.data.type;

import com.google.common.collect.ImmutableMap;
import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import lombok.Getter;
import org.apache.commons.lang3.EnumUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.data.type.WideningTypeRule;

/** The extension of ExprType in OpenSearch. */
public class OpenSearchDataType implements ExprType, Serializable {

  /**
   * Redefine comparison operation: class (or derived) could be compared with ExprCoreType too. Used
   * in {@link WideningTypeRule#distance(ExprType, ExprType)}.
   */
  @Override
  public boolean equals(final Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof ExprCoreType) {
      return exprCoreType.equals(o);
    }
    if (!(o instanceof OpenSearchDataType)) {
      return false;
    }
    OpenSearchDataType other = (OpenSearchDataType) o;
    if (mappingType != null && other.mappingType != null) {
      return mappingType.equals(other.mappingType) && exprCoreType.equals(other.exprCoreType);
    }
    return exprCoreType.equals(other.exprCoreType);
  }

  /** Redefine hash calculation to enforce comparing using {@link #equals(Object)}. */
  @Override
  public int hashCode() {
    return 42 + exprCoreType.hashCode();
  }

  @Override
  public List<ExprType> getParent() {
    return exprCoreType.getParent();
  }

  @Override
  public boolean shouldCast(ExprType other) {
    ExprCoreType otherCoreType = ExprCoreType.UNKNOWN;
    if (other instanceof ExprCoreType) {
      otherCoreType = (ExprCoreType) other;
    } else if (other instanceof OpenSearchDataType) {
      otherCoreType = ((OpenSearchDataType) other).exprCoreType;
    }
    if (ExprCoreType.numberTypes().contains(exprCoreType)
        && ExprCoreType.numberTypes().contains(otherCoreType)) {
      return false;
    }
    return exprCoreType == ExprCoreType.UNKNOWN || exprCoreType.shouldCast(otherCoreType);
  }

  /** The mapping (OpenSearch engine) type. */
  public enum MappingType {
    Invalid(null, ExprCoreType.UNKNOWN),
    Text("text", ExprCoreType.STRING),
    Keyword("keyword", ExprCoreType.STRING),
    Ip("ip", ExprCoreType.UNKNOWN),
    GeoPoint("geo_point", ExprCoreType.UNKNOWN),
    Binary("binary", ExprCoreType.UNKNOWN),
    Date("date", ExprCoreType.TIMESTAMP),
    DateNanos("date_nanos", ExprCoreType.TIMESTAMP),
    Object("object", ExprCoreType.STRUCT),
    Nested("nested", ExprCoreType.ARRAY),
    Byte("byte", ExprCoreType.BYTE),
    Short("short", ExprCoreType.SHORT),
    Integer("integer", ExprCoreType.INTEGER),
    Long("long", ExprCoreType.LONG),
    Float("float", ExprCoreType.FLOAT),
    HalfFloat("half_float", ExprCoreType.FLOAT),
    ScaledFloat("scaled_float", ExprCoreType.DOUBLE),
    Double("double", ExprCoreType.DOUBLE),
    Boolean("boolean", ExprCoreType.BOOLEAN);
    // TODO: ranges, geo shape, point, shape

    private final String name;

    // Associated `ExprCoreType`
    @Getter private final ExprCoreType exprCoreType;

    MappingType(String name, ExprCoreType exprCoreType) {
      this.name = name;
      this.exprCoreType = exprCoreType;
    }

    public String toString() {
      return name;
    }
  }

  @Getter protected MappingType mappingType;

  // resolved ExprCoreType
  protected ExprCoreType exprCoreType;

  /**
   * Get a simplified type {@link ExprCoreType} if possible. To avoid returning `UNKNOWN` for
   * `OpenSearch*Type`s, e.g. for IP, returns itself.
   *
   * @return An {@link ExprType}.
   */
  @Override
  public ExprType getExprType() {
    if (exprCoreType != ExprCoreType.UNKNOWN) {
      return exprCoreType;
    }
    return this;
  }

  /**
   * Simple instances of OpenSearchDataType are created once during entire SQL engine lifetime and
   * cached there. This reduces memory usage and increases type comparison. Note: Types with
   * non-empty fields and properties are not cached.
   */
  private static final Map<String, OpenSearchDataType> instances = new HashMap<>();

  static {
    EnumUtils.getEnumList(MappingType.class).stream()
        .filter(t -> t != MappingType.Invalid)
        .forEach(t -> instances.put(t.toString(), OpenSearchDataType.of(t)));
    EnumUtils.getEnumList(ExprCoreType.class)
        .forEach(t -> instances.put(t.toString(), OpenSearchDataType.of(t)));
  }

  /**
   * Parses index mapping and maps it to a Data type in the SQL plugin.
   *
   * @param indexMapping An input with keys and objects that need to be mapped to a data type.
   * @return The mapping.
   */
  public static Map<String, OpenSearchDataType> parseMapping(Map<String, Object> indexMapping) {
    Map<String, OpenSearchDataType> result = new LinkedHashMap<>();

    if (indexMapping == null) {
      return result;
    }

    indexMapping.forEach(
        (k, v) -> {
          var innerMap = (Map<String, Object>) v;
          // by default, the type is treated as an Object if "type" is not provided
          var type = ((String) innerMap.getOrDefault("type", "object")).replace("_", "");
          if (!EnumUtils.isValidEnumIgnoreCase(OpenSearchDataType.MappingType.class, type)) {
            // unknown type, e.g. `alias`
            // TODO resolve alias reference
            return;
          }
          // create OpenSearchDataType
          result.put(
              k,
              OpenSearchDataType.of(
                  EnumUtils.getEnumIgnoreCase(OpenSearchDataType.MappingType.class, type),
                  innerMap));
        });
    return result;
  }

  /**
   * A constructor function which builds proper `OpenSearchDataType` for given mapping `Type`.
   *
   * @param mappingType A mapping type.
   * @return An instance or inheritor of `OpenSearchDataType`.
   */
  @SuppressWarnings("unchecked")
  public static OpenSearchDataType of(MappingType mappingType, Map<String, Object> innerMap) {
    OpenSearchDataType res =
        instances.getOrDefault(mappingType.toString(), new OpenSearchDataType(mappingType));
    switch (mappingType) {
      case Object:
        // TODO: use Object type once it has been added
      case Nested:
        if (innerMap.isEmpty()) {
          return res;
        }
        Map<String, OpenSearchDataType> properties =
            parseMapping((Map<String, Object>) innerMap.getOrDefault("properties", Map.of()));
        OpenSearchDataType objectDataType = res.cloneEmpty();
        objectDataType.properties = properties;
        return objectDataType;
      case Text:
        // don't parse `fielddata`, because it does not contain any info which could be used by SQL
        Map<String, OpenSearchDataType> fields =
            parseMapping((Map<String, Object>) innerMap.getOrDefault("fields", Map.of()));
        return (!fields.isEmpty()) ? OpenSearchTextType.of(fields) : OpenSearchTextType.of();
      case GeoPoint:
        return OpenSearchGeoPointType.of();
      case Binary:
        return OpenSearchBinaryType.of();
      case Ip:
        return OpenSearchIpType.of();
      case Date:
      case DateNanos:
        // Default date formatter is used when "" is passed as the second parameter
        return innerMap.isEmpty()
            ? OpenSearchDateType.of()
            : OpenSearchDateType.of((String) innerMap.getOrDefault("format", ""));
      default:
        return res;
    }
  }

  /**
   * A constructor function which builds proper `OpenSearchDataType` for given mapping `Type`.
   * Designed to be called by the mapping parser only (and tests).
   *
   * @param mappingType A mapping type.
   * @return An instance or inheritor of `OpenSearchDataType`.
   */
  public static OpenSearchDataType of(MappingType mappingType) {
    return of(mappingType, Map.of());
  }

  /**
   * A constructor function which builds proper `OpenSearchDataType` for given {@link ExprType}.
   *
   * @param type A type.
   * @return An instance of `OpenSearchDataType`.
   */
  public static OpenSearchDataType of(ExprType type) {
    if (type instanceof OpenSearchDataType) {
      return (OpenSearchDataType) type;
    }
    var res = instances.getOrDefault(type.toString(), null);
    if (res != null) {
      return res;
    }
    if (OpenSearchDateType.isDateTypeCompatible(type)) {
      return OpenSearchDateType.of(type);
    }

    return new OpenSearchDataType((ExprCoreType) type);
  }

  protected OpenSearchDataType(MappingType mappingType) {
    this.mappingType = mappingType;
    this.exprCoreType = mappingType.getExprCoreType();
  }

  protected OpenSearchDataType(ExprCoreType type) {
    this.exprCoreType = type;
  }

  // For datatypes with properties (example: object and nested types)
  // a read-only collection
  @Getter Map<String, OpenSearchDataType> properties = ImmutableMap.of();

  @Override
  // Called when building TypeEnvironment and when serializing PPL response
  public String typeName() {
    // To avoid breaking changes return `string` for `typeName` call (PPL) and `text` for
    // `legacyTypeName` call (SQL). See more: https://github.com/opensearch-project/sql/issues/1296
    if (legacyTypeName().equals("TEXT") || legacyTypeName().equals("KEYWORD")) {
      return "STRING";
    }
    return legacyTypeName();
  }

  @Override
  // Called when serializing SQL response
  public String legacyTypeName() {
    if (mappingType == null) {
      return exprCoreType.typeName();
    }
    return mappingType.toString().toUpperCase();
  }

  /**
   * Clone type object without {@link #properties} - without info about nested object types. Note:
   * Should be overriden by all derived classes for proper work.
   *
   * @return A cloned object.
   */
  protected OpenSearchDataType cloneEmpty() {
    return this.mappingType == null
        ? new OpenSearchDataType(this.exprCoreType)
        : new OpenSearchDataType(this.mappingType);
  }

  /**
   * Flattens mapping tree into a single layer list of objects (pairs of name-types actually), which
   * don't have nested types. See <em>OpenSearchDataTypeTest::traverseAndFlatten()</em> test for
   * example.
   *
   * @param tree A list of `OpenSearchDataType`s - map between field name and its type.
   * @return A list of all `OpenSearchDataType`s from given map on the same nesting level (1).
   *     Nested object names are prefixed by names of their host.
   */
  public static Map<String, OpenSearchDataType> traverseAndFlatten(
      Map<String, OpenSearchDataType> tree) {
    final Map<String, OpenSearchDataType> result = new LinkedHashMap<>();
    BiConsumer<Map<String, OpenSearchDataType>, String> visitLevel =
        new BiConsumer<>() {
          @Override
          public void accept(Map<String, OpenSearchDataType> subtree, String prefix) {
            for (var entry : subtree.entrySet()) {
              String entryKey = entry.getKey();
              var nextPrefix =
                  prefix.isEmpty() ? entryKey : String.format("%s.%s", prefix, entryKey);
              result.put(nextPrefix, entry.getValue().cloneEmpty());
              var nextSubtree = entry.getValue().getProperties();
              if (!nextSubtree.isEmpty()) {
                accept(nextSubtree, nextPrefix);
              }
            }
          }
        };
    visitLevel.accept(tree, "");
    return result;
  }

  /**
   * Resolve type of identified from parsed mapping tree.
   *
   * @param tree Parsed mapping tree (not flattened).
   * @param id An identifier.
   * @return Resolved OpenSearchDataType or null if not found.
   */
  public static OpenSearchDataType resolve(Map<String, OpenSearchDataType> tree, String id) {
    for (var item : tree.entrySet()) {
      if (item.getKey().equals(id)) {
        return item.getValue();
      }
      OpenSearchDataType result = resolve(item.getValue().getProperties(), id);
      if (result != null) {
        return result;
      }
    }
    return null;
  }
}
