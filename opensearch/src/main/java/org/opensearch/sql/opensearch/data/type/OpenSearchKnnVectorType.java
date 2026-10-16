/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.data.type;

import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.data.type.VectorDataType;
import org.opensearch.sql.data.type.VectorFieldType;
import org.opensearch.sql.executor.QueryType;
import org.opensearch.sql.lang.LangSpec;

/**
 * The type of a k-NN plugin {@code knn_vector} field. See <a
 * href="https://docs.opensearch.org/latest/field-types/supported-field-types/knn-vector/">doc</a>.
 *
 * <p>Its core type is {@link ExprCoreType#VECTOR}. It keeps the mapping attributes the planner
 * needs, parsed by {@link #of(Map)}:
 *
 * <ul>
 *   <li>{@code dimension} — {@code dimension}; {@link #UNKNOWN_DIMENSION} when the mapping has
 *       none, e.g. a model-based field ({@code model_id}), which stays readable.
 *   <li>{@code dataType} — {@code data_type}: {@code float} (the default), {@code half_float},
 *       {@code byte} or {@code binary}. A value this plugin does not know reads as {@code float}.
 *   <li>{@code spaceType} — {@code space_type} at the field level, else inside {@code method};
 *       {@code null} when neither is set, in which case callers apply the k-NN defaults.
 *   <li>{@code engine} — {@code method.engine}, else the field-level {@code engine}; nullable.
 *   <li>{@code methodName} — {@code method.name}, e.g. {@code hnsw} or {@code ivf}; nullable.
 *   <li>{@code modelId} — {@code model_id}; nullable.
 * </ul>
 *
 * <p>String attributes are lower-cased. Unlike most {@link OpenSearchDataType}s, {@link
 * #getExprType()} returns this instance rather than the bare core type, so the attributes reach the
 * Calcite schema ({@code OpenSearchTypeFactory.convertExprTypeToRelDataType} turns it into an
 * {@code ExprVectorType}).
 *
 * <p>Type names. Only PPL on the Calcite engine reports the {@code vector} type: there a vector
 * column's type is {@link ExprCoreType#VECTOR}, and PPL {@code describe} names the field {@code
 * vector} when Calcite is enabled (see {@link #describeTypeName}). The V2 engine, which runs all
 * SQL and PPL with Calcite off, sees this type in its type environment and treats it as the {@link
 * ExprCoreType#ARRAY} that {@code knn_vector} mapped to before: {@link #typeName()}, {@link
 * #legacyTypeName()} and {@link #getParent()} are those of ARRAY, so its responses, {@code typeof}
 * and function resolution are unchanged. SQL {@code describe} keeps the mapping type name {@code
 * knn_vector}.
 */
@Getter
@EqualsAndHashCode(callSuper = true)
public class OpenSearchKnnVectorType extends OpenSearchDataType implements VectorFieldType {

  private static final String DIMENSION = "dimension";
  private static final String DATA_TYPE = "data_type";
  private static final String SPACE_TYPE = "space_type";
  private static final String METHOD = "method";
  private static final String METHOD_NAME = "name";
  private static final String ENGINE = "engine";
  private static final String MODEL_ID = "model_id";

  /** The k-NN plugin's {@code SpaceType.UNDEFINED}, which stands for no declared space type. */
  private static final String UNDEFINED_SPACE_TYPE = "undefined";

  /** k-NN {@code SpaceType.DEFAULT} and {@code SpaceType.DEFAULT_BINARY}. */
  private static final String DEFAULT_SPACE_TYPE = "l2";

  private static final String DEFAULT_BINARY_SPACE_TYPE = "hamming";

  private final int dimension;
  private final VectorDataType dataType;
  private final @Nullable String spaceType;
  private final @Nullable String engine;
  private final @Nullable String methodName;
  private final @Nullable String modelId;

  /**
   * Whether this type merges indices that disagree on what a k-NN search needs to be identical:
   * dimension, data type and space type. See {@link #mergedWith}.
   */
  @Getter(AccessLevel.NONE)
  private final boolean mappingMismatch;

  /**
   * The field's own type in each index of a multi-index query, by index name; empty for a single
   * index. It tells how a value of each index is encoded and names the indices of a mismatch. Not
   * part of the type's identity.
   */
  @Getter(AccessLevel.NONE)
  @EqualsAndHashCode.Exclude
  private final Map<String, OpenSearchKnnVectorType> indexTypes;

  /** Whether {@link #indexTypes} disagree on whether elements are bytes; derived from them. */
  @Getter(AccessLevel.NONE)
  @EqualsAndHashCode.Exclude
  private final boolean mixedElementEncodings;

  private OpenSearchKnnVectorType(
      int dimension,
      VectorDataType dataType,
      @Nullable String spaceType,
      @Nullable String engine,
      @Nullable String methodName,
      @Nullable String modelId) {
    this(dimension, dataType, spaceType, engine, methodName, modelId, false, Map.of());
  }

  private OpenSearchKnnVectorType(
      int dimension,
      VectorDataType dataType,
      @Nullable String spaceType,
      @Nullable String engine,
      @Nullable String methodName,
      @Nullable String modelId,
      boolean mappingMismatch,
      Map<String, OpenSearchKnnVectorType> indexTypes) {
    super(MappingType.KnnVector);
    this.dimension = dimension < 0 ? UNKNOWN_DIMENSION : dimension;
    this.dataType = dataType;
    this.spaceType = spaceType;
    this.engine = engine;
    this.methodName = methodName;
    this.modelId = modelId;
    this.mappingMismatch = mappingMismatch;
    this.indexTypes = indexTypes;
    this.mixedElementEncodings =
        indexTypes.values().stream().map(t -> t.dataType.hasByteElements()).distinct().count() > 1;
  }

  /**
   * This type as the mapping of the given index, before the mappings of a multi-index query are
   * merged with {@link #mergedWith}. It keeps this type's sub-field properties.
   *
   * @param index the name of the index whose mapping this type is
   */
  public OpenSearchKnnVectorType inIndex(String index) {
    OpenSearchKnnVectorType annotated =
        new OpenSearchKnnVectorType(
            dimension,
            dataType,
            spaceType,
            engine,
            methodName,
            modelId,
            mappingMismatch,
            Map.of(index, withoutIndices()));
    annotated.properties = properties;
    return annotated;
  }

  /**
   * This type in an index whose settings declare the space type of k-NN 2.x, {@code
   * index.knn.space_type}: k-NN 2.x uses it for a mapping that declares none (2.x {@code
   * SpaceTypeResolver.resolveSpaceType}); the setting was removed in k-NN 3.0, but a remote cluster
   * of a cross-cluster query can still be 2.x. A declared space type and a model-based field are
   * kept.
   *
   * <p>k-NN 2.17 and 2.18 ignored the setting (fixed in 2.19 by k-NN #2374, backported in #2389):
   * they wrote the default into a method mapping, which then declares it, and used the default for
   * a legacy mapping without a method, which declares none. For such an index on a 2.17 or 2.18
   * cluster this type takes the setting while k-NN used the default, so a merge with an index of
   * the default space type can report a mismatch k-NN would not have.
   *
   * @param indexSpaceType the value of {@code index.knn.space_type}
   */
  public OpenSearchKnnVectorType withIndexSpaceType(String indexSpaceType) {
    if (spaceType != null && !UNDEFINED_SPACE_TYPE.equals(spaceType) || modelId != null) {
      return this;
    }
    OpenSearchKnnVectorType resolved =
        new OpenSearchKnnVectorType(
            dimension,
            dataType,
            lower(indexSpaceType),
            engine,
            methodName,
            modelId,
            mappingMismatch,
            indexTypes);
    resolved.properties = properties;
    return resolved;
  }

  private OpenSearchKnnVectorType withoutIndices() {
    return new OpenSearchKnnVectorType(dimension, dataType, spaceType, engine, methodName, modelId);
  }

  /**
   * The type of the field across two indices, for a multi-index query: this type, which is the
   * accumulated one, merged with {@code other}. It claims nothing the mappings disagree on:
   *
   * <ul>
   *   <li>dimension: kept if both have the same dimension and read their elements the same way
   *       (float and half_float do; byte does not read like either, and only binary counts the
   *       dimension in bits), else {@link #UNKNOWN_DIMENSION};
   *   <li>data type: the shared one; else {@link VectorDataType#BYTE} if both have byte elements,
   *       {@link VectorDataType#FLOAT} otherwise, which can hold either kind of element;
   *   <li>space type: the shared declared value; else the space type the k-NN plugin resolves for
   *       both (see {@link #sameSpaceType}), as an undeclared one is the default; else {@code
   *       null};
   *   <li>engine, method name, model id: the shared value, else {@code null}.
   * </ul>
   *
   * <p>The merged type keeps the indices' own types (see {@link #inIndex}) and {@link
   * #hasMappingMismatch()} tells whether they differ in dimension, data type or the space type the
   * k-NN plugin uses, on which a k-NN step reports <i>vector mapping mismatch</i>. It carries this
   * type's sub-field properties, as merging kept the first index's type when knn_vector mapped to
   * ARRAY.
   *
   * @param other the field's type in another index
   */
  public OpenSearchKnnVectorType mergedWith(OpenSearchKnnVectorType other) {
    Map<String, OpenSearchKnnVectorType> indices = new TreeMap<>(indexTypes);
    indices.putAll(other.indexTypes);
    boolean sameDataType = dataType == other.dataType;
    VectorDataType mergedDataType =
        sameDataType
            ? dataType
            : dataType.hasByteElements() && other.dataType.hasByteElements()
                ? VectorDataType.BYTE
                : VectorDataType.FLOAT;
    boolean sameDimension =
        dimension == other.dimension && elementKind(dataType) == elementKind(other.dataType);
    boolean sameSpaceType = sameSpaceType(other);
    OpenSearchKnnVectorType merged =
        new OpenSearchKnnVectorType(
            sameDimension ? dimension : UNKNOWN_DIMENSION,
            mergedDataType,
            Objects.equals(spaceType, other.spaceType)
                ? spaceType
                : sameSpaceType ? resolvedSpaceType() : null,
            shared(engine, other.engine),
            shared(methodName, other.methodName),
            shared(modelId, other.modelId),
            mappingMismatch
                || other.mappingMismatch
                || dimension != other.dimension
                || !sameDataType
                || !sameSpaceType,
            Collections.unmodifiableMap(indices));
    merged.properties = properties;
    return merged;
  }

  /**
   * The space type the k-NN plugin uses for this mapping: the declared one ({@code
   * method.space_type} or the field-level {@code space_type}, which k-NN requires to agree), else
   * its default for the data type, {@code hamming} for binary and {@code l2} otherwise (k-NN {@code
   * SpaceTypeResolver.resolveSpaceType} and {@code getDefaultSpaceType}). The legacy index setting
   * {@code index.knn.space_type} was removed in k-NN 3.0; for an index of a 2.x remote cluster it
   * is already the declared space type here (see {@link #withIndexSpaceType}). {@code null} for a
   * model-based field, whose space type is the model's and not in the mapping.
   */
  private @Nullable String resolvedSpaceType() {
    if (spaceType != null && !UNDEFINED_SPACE_TYPE.equals(spaceType)) {
      return spaceType;
    }
    if (modelId != null) {
      return null;
    }
    return dataType == VectorDataType.BINARY ? DEFAULT_BINARY_SPACE_TYPE : DEFAULT_SPACE_TYPE;
  }

  /**
   * Whether the k-NN plugin uses the same space type for both mappings. A space type that the
   * mappings do not tell, that of a model-based field, is the same only for the same model.
   */
  private boolean sameSpaceType(OpenSearchKnnVectorType other) {
    String resolved = resolvedSpaceType();
    String otherResolved = other.resolvedSpaceType();
    if (resolved != null || otherResolved != null) {
      return Objects.equals(resolved, otherResolved);
    }
    return modelId != null && modelId.equals(other.modelId);
  }

  /** How elements of a data type are read: 0 float, 1 byte, 2 bits. */
  private static int elementKind(VectorDataType dataType) {
    return switch (dataType) {
      case FLOAT, HALF_FLOAT -> 0;
      case BYTE -> 1;
      case BINARY -> 2;
    };
  }

  @Override
  public boolean hasMappingMismatch() {
    return mappingMismatch;
  }

  /** The indices of the merge, sorted, if their mappings mismatch; else empty. */
  @Override
  public List<String> getMappingMismatchIndices() {
    return mappingMismatch ? List.copyOf(indexTypes.keySet()) : List.of();
  }

  /**
   * The data type a value from {@code index} was indexed with. The merged data type, unless the
   * indices disagree on whether elements are bytes; then the index's own data type, or {@code null}
   * if the index is not known or not one of the merge.
   */
  @Override
  public @Nullable VectorDataType getStoredDataType(@Nullable String index) {
    if (!mixedElementEncodings) {
      return dataType;
    }
    OpenSearchKnnVectorType own = index == null ? null : indexTypes.get(index);
    return own == null ? null : own.dataType;
  }

  private static @Nullable String shared(@Nullable String a, @Nullable String b) {
    return Objects.equals(a, b) ? a : null;
  }

  /**
   * Create a vector type with the given attributes.
   *
   * @param dimension the mapping dimension, or {@link #UNKNOWN_DIMENSION}
   * @param dataType the element encoding
   * @param spaceType the space type, or {@code null} if not declared
   * @param engine the engine, or {@code null} if not declared
   * @param methodName the method name, or {@code null} if not declared
   */
  public static OpenSearchKnnVectorType of(
      int dimension,
      VectorDataType dataType,
      @Nullable String spaceType,
      @Nullable String engine,
      @Nullable String methodName) {
    return new OpenSearchKnnVectorType(
        dimension, dataType, lower(spaceType), lower(engine), lower(methodName), null);
  }

  /**
   * Parse the attributes of a {@code knn_vector} field mapping.
   *
   * @param mapping the field mapping, e.g. {@code {"type": "knn_vector", "dimension": 768,
   *     "method": {"name": "hnsw", "engine": "faiss", "space_type": "l2"}}}
   */
  public static OpenSearchKnnVectorType of(Map<String, Object> mapping) {
    Map<?, ?> method = mapping.get(METHOD) instanceof Map<?, ?> m ? m : Map.of();
    String spaceType = string(mapping.get(SPACE_TYPE));
    if (spaceType == null) {
      spaceType = string(method.get(SPACE_TYPE));
    }
    String engine = string(method.get(ENGINE));
    if (engine == null) {
      engine = string(mapping.get(ENGINE));
    }
    return new OpenSearchKnnVectorType(
        dimension(mapping.get(DIMENSION)),
        dataType(string(mapping.get(DATA_TYPE))),
        spaceType,
        engine,
        string(method.get(METHOD_NAME)),
        string(mapping.get(MODEL_ID)));
  }

  /** A vector reads as itself, not as the bare core type, so its attributes are kept. */
  @Override
  public ExprType getExprType() {
    return this;
  }

  /** The V2 engine resolves functions on a vector field as it did for ARRAY. */
  @Override
  public List<ExprType> getParent() {
    return List.of(ExprCoreType.ARRAY);
  }

  @Override
  public boolean shouldCast(ExprType other) {
    return false;
  }

  /** {@code ARRAY}: the name V2 PPL responses and {@code typeof} reported for knn_vector. */
  @Override
  public String typeName() {
    return ExprCoreType.ARRAY.typeName();
  }

  /** {@code NESTED}: the name SQL responses (JDBC) and SQL {@code typeof} reported for it. */
  @Override
  public String legacyTypeName() {
    return ExprCoreType.ARRAY.legacyTypeName();
  }

  /**
   * knn_vector as an alias to it saw it when knn_vector mapped to ARRAY: an {@link
   * OpenSearchDataType} of mapping type knn_vector and core type ARRAY, named {@code KNN_VECTOR} by
   * its mapping type, with the default parent. An alias reports these names in SQL, in the V2
   * engine and in describe, as before.
   */
  private static final OpenSearchDataType ALIAS_TARGET = new BaseKnnVectorType();

  private static final class BaseKnnVectorType extends OpenSearchDataType {
    private BaseKnnVectorType() {
      super(MappingType.KnnVector);
      exprCoreType = ExprCoreType.ARRAY;
    }
  }

  @Override
  public OpenSearchDataType asAliasTarget() {
    return ALIAS_TARGET;
  }

  /**
   * PPL {@code describe} reports {@code VECTOR} when Calcite is enabled and {@code ARRAY}, as
   * before, when it is not; SQL {@code describe} keeps {@code KNN_VECTOR}.
   */
  @Override
  public String describeTypeName(LangSpec langSpec, boolean calciteEngine) {
    if (langSpec.language() != QueryType.PPL) {
      return getMappingType().toString().toUpperCase(Locale.ROOT);
    }
    return calciteEngine ? ExprCoreType.VECTOR.typeName() : langSpec.typeName(this);
  }

  @Override
  protected OpenSearchDataType cloneEmpty() {
    return new OpenSearchKnnVectorType(
        dimension, dataType, spaceType, engine, methodName, modelId, mappingMismatch, indexTypes);
  }

  private static int dimension(@Nullable Object value) {
    if (value instanceof Number number) {
      return number.intValue();
    }
    if (value instanceof String text) {
      try {
        return Integer.parseInt(text.trim());
      } catch (NumberFormatException e) {
        return UNKNOWN_DIMENSION;
      }
    }
    return UNKNOWN_DIMENSION;
  }

  private static VectorDataType dataType(@Nullable String value) {
    try {
      return VectorDataType.fromMappingName(value);
    } catch (IllegalArgumentException e) {
      // A data type added by a later k-NN version: keep the field readable as float elements.
      return VectorDataType.DEFAULT;
    }
  }

  private static @Nullable String string(@Nullable Object value) {
    return value == null ? null : lower(value.toString());
  }

  private static @Nullable String lower(@Nullable String value) {
    return value == null ? null : value.toLowerCase(Locale.ROOT);
  }
}
