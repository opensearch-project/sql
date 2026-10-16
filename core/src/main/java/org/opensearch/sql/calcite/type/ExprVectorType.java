/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.type;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.FloatBuffer;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeComparability;
import org.apache.calcite.sql.type.ArraySqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.ExprUDT;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.VectorDataType;
import org.opensearch.sql.data.type.VectorFieldType;

/**
 * Calcite type of a dense vector column ({@link ExprCoreType#VECTOR}, {@link ExprUDT#EXPR_VECTOR}):
 * an {@code ARRAY<REAL>} for {@code float} / {@code half_float} vectors or an {@code
 * ARRAY<TINYINT>} for {@code byte} / {@code binary} vectors, which also carries the vector's
 * dimension, element encoding and space type. Calcite has no vector type, and its {@code ARRAY}
 * cannot carry a length, so the dimension lives here.
 *
 * <p>This is a subclass of {@link ArraySqlType}, not a wrapper built on {@link
 * AbstractExprRelDataType} like the other UDTs. Calcite casts every type whose {@link
 * #getSqlTypeName()} is {@code ARRAY} to {@code ArraySqlType}, for example in {@code
 * RexBuilder.makeLiteral} and {@code RelToSqlConverterUtil.getCastSpecAngleBracketArrayType}; a
 * wrapper reporting {@code ARRAY} would throw {@link ClassCastException} there. As a subclass, a
 * vector column also keeps working with every consumer that understands Calcite arrays: enumerable
 * code generation (runtime value {@code java.util.List<Float>} or {@code List<Byte>}), {@code
 * RelToSql} and the PPL array functions.
 *
 * <p>Two vector types are equal when dimension, data type, space type and nullability are equal
 * (Calcite compares types by digest). {@link #toString()} prints the user-facing form, e.g. {@code
 * VECTOR(768, FLOAT)}; the digest adds the space type, e.g. {@code VECTOR(768, FLOAT, l2)}, so that
 * type canonization never merges fields with different similarities.
 *
 * <p>Vector values have no order or equality semantics: {@link #getComparability()} is {@link
 * RelDataTypeComparability#NONE}, and the Calcite lowering of PPL fails with {@link
 * org.opensearch.sql.calcite.utils.VectorErrors#vectorNotComparable()} when a vector is
 *
 * <ul>
 *   <li>an operand of a comparison: {@code =}, {@code !=}, {@code <}, ..., {@code in}, {@code
 *       between}, {@code in [subsearch]}, a join condition, a named join key, a column both inputs
 *       of a join without keys share (it would be an implicit key), or a lookup key;
 *   <li>a sort key: {@code sort}, {@code trendline sort}, the order of a window, the {@code
 *       xyseries} x-field;
 *   <li>a group key: {@code stats}, {@code top}, {@code rare}, {@code chart}, {@code patterns},
 *       {@code eventstats} and {@code streamstats} by-lists, the {@code xyseries} x-field, and the
 *       columns {@code mvcombine} groups by;
 *   <li>a {@code dedup} field;
 *   <li>the argument of {@code min}, {@code max}, {@code dc} / {@code distinct_count} or {@code
 *       values}, or an {@code xyseries} y-data field (aggregated with {@code max}).
 * </ul>
 *
 * <p>Create instances through {@link OpenSearchTypeFactory#createVectorType} so they are canonized.
 * The type factory preserves this type in {@code createTypeWithNullability} and {@code
 * enforceTypeWithNullability} (Calcite's defaults would rebuild a plain {@code ArraySqlType} and
 * drop the dimension) and in {@code leastRestrictive}.
 */
public final class ExprVectorType extends ArraySqlType {

  /** Same as {@link VectorFieldType#UNKNOWN_DIMENSION}. */
  public static final int UNKNOWN_DIMENSION = VectorFieldType.UNKNOWN_DIMENSION;

  private final int dimension;
  private final VectorDataType dataType;
  private final @Nullable String spaceType;
  private final IndexMappings indexMappings;

  /**
   * What a vector column whose type merges the mappings of several indices knows about them; {@link
   * #SINGLE} for a column of one mapping, or of indices that map it alike. The sets are sorted and
   * unmodifiable.
   *
   * @param mismatch whether the indices map the field with a different dimension, data type or
   *     space type, on which a k-NN step reports <i>vector mapping mismatch</i>
   * @param indices the indices, if {@code mismatch} and they are known; else empty
   * @param byteElementIndices the indices whose data type has byte elements, if the indices
   *     disagree on whether elements are bytes (e.g. float in one, byte in another); else empty. A
   *     base64 value is decoded with its own index's encoding.
   * @throws IllegalArgumentException for facts no merge gives: indices without a mismatch (the data
   *     types of a disagreement on encoding differ, so it is a mismatch), byte element indices that
   *     are not among the indices, or every index having byte elements (then they agree on the
   *     encoding)
   */
  public record IndexMappings(
      boolean mismatch, SortedSet<String> indices, SortedSet<String> byteElementIndices) {

    /** One mapping. */
    public static final IndexMappings SINGLE = new IndexMappings(false, List.of(), List.of());

    public IndexMappings {
      indices = Collections.unmodifiableSortedSet(new TreeSet<>(indices));
      byteElementIndices = Collections.unmodifiableSortedSet(new TreeSet<>(byteElementIndices));
      if (!mismatch && !(indices.isEmpty() && byteElementIndices.isEmpty())) {
        throw new IllegalArgumentException("indices are recorded only for a mapping mismatch");
      }
      if (!indices.containsAll(byteElementIndices)) {
        throw new IllegalArgumentException("byte element indices must be among the indices");
      }
      if (!byteElementIndices.isEmpty() && byteElementIndices.size() == indices.size()) {
        throw new IllegalArgumentException(
            "byte element indices are recorded only when another index has float elements");
      }
    }

    public IndexMappings(
        boolean mismatch, Collection<String> indices, Collection<String> byteElementIndices) {
      this(mismatch, new TreeSet<>(indices), new TreeSet<>(byteElementIndices));
    }

    /** Whether the indices disagree on whether elements are bytes. */
    public boolean mixedElementEncodings() {
      return !byteElementIndices.isEmpty();
    }

    /** What a column that combines values of both knows, whichever comes first. */
    public IndexMappings union(IndexMappings other) {
      if (equals(other)) {
        return this;
      }
      TreeSet<String> allIndices = new TreeSet<>(indices);
      allIndices.addAll(other.indices);
      TreeSet<String> byteIndices = new TreeSet<>(byteElementIndices);
      byteIndices.addAll(other.byteElementIndices);
      if (byteIndices.size() == allIndices.size()) {
        // Only inputs that contradict each other give this; keep what they agree on.
        byteIndices.clear();
      }
      return new IndexMappings(mismatch || other.mismatch, allIndices, byteIndices);
    }
  }

  /**
   * Prefer {@link OpenSearchTypeFactory#createVectorType}, which builds the element type and
   * canonizes the result.
   *
   * @param elementType {@code REAL} for float data types, {@code TINYINT} for byte data types
   * @param nullable whether the vector itself is nullable
   * @param dimension the mapping dimension, or {@link #UNKNOWN_DIMENSION}
   * @param dataType the element encoding
   * @param spaceType the declared space type, or {@code null} if not declared
   */
  public ExprVectorType(
      RelDataType elementType,
      boolean nullable,
      int dimension,
      VectorDataType dataType,
      @Nullable String spaceType) {
    this(elementType, nullable, dimension, dataType, spaceType, IndexMappings.SINGLE);
  }

  /**
   * Prefer {@link OpenSearchTypeFactory#createVectorType}.
   *
   * @param indexMappings what the type knows of the indices whose mappings it merges (see {@link
   *     VectorFieldType#hasMappingMismatch})
   */
  public ExprVectorType(
      RelDataType elementType,
      boolean nullable,
      int dimension,
      VectorDataType dataType,
      @Nullable String spaceType,
      IndexMappings indexMappings) {
    super(elementType, nullable);
    this.dimension = dimension < 0 ? UNKNOWN_DIMENSION : dimension;
    this.dataType = Objects.requireNonNull(dataType, "dataType");
    this.spaceType = spaceType;
    this.indexMappings = Objects.requireNonNull(indexMappings, "indexMappings");
    SqlTypeName expected = dataType.hasByteElements() ? SqlTypeName.TINYINT : SqlTypeName.REAL;
    if (elementType.getSqlTypeName() != expected) {
      throw new IllegalArgumentException(
          "A " + dataType + " vector needs " + expected + " elements, got " + elementType);
    }
    // ArraySqlType computed the digest before the fields above were set; compute it again.
    computeDigest();
  }

  /**
   * The dimension as declared in the mapping. For {@link VectorDataType#BINARY} it counts bits.
   *
   * @return the dimension, or {@link #UNKNOWN_DIMENSION} (e.g. a model-based field)
   */
  public int getDimension() {
    return dimension;
  }

  /** Whether the dimension is known. */
  public boolean hasKnownDimension() {
    return dimension != UNKNOWN_DIMENSION;
  }

  /**
   * The number of elements a value holds: the dimension, or {@code dimension / 8} for {@link
   * VectorDataType#BINARY}.
   *
   * @return the element count, or {@link #UNKNOWN_DIMENSION} if the dimension is unknown
   */
  public int getElementCount() {
    return dataType.elementCount(dimension);
  }

  /** The element encoding; never null. */
  public VectorDataType getDataType() {
    return dataType;
  }

  /** The declared space type (lower-case, e.g. {@code l2}), or {@code null} if not declared. */
  public @Nullable String getSpaceType() {
    return spaceType;
  }

  /**
   * Whether the indices of a multi-index query map the field with a different dimension, data type
   * or space type; a k-NN search over it reports <i>vector mapping mismatch</i>.
   */
  public boolean hasMappingMismatch() {
    return indexMappings.mismatch();
  }

  /** The indices whose mappings mismatch, sorted; empty if they agree or are not known. */
  public List<String> getMappingMismatchIndices() {
    return List.copyOf(indexMappings.indices());
  }

  /** What the type knows of the indices whose mappings it merges. */
  public IndexMappings getIndexMappings() {
    return indexMappings;
  }

  /**
   * Whether the indices whose mappings this type merges disagree on whether elements are bytes, so
   * that a base64 value can only be decoded knowing its index (see {@link #storesByteElements}).
   */
  public boolean hasMixedElementEncodings() {
    return indexMappings.mixedElementEncodings();
  }

  /**
   * Whether a stored value from {@code index} holds one byte per element rather than float32
   * values, which decides how a base64 value is decoded.
   *
   * @param index the index of the value, or {@code null} if not known
   * @return the data type's encoding if the indices agree on it; else that of {@code index}, or
   *     {@code null} if {@code index} is not known or not one of the merged indices
   */
  public @Nullable Boolean storesByteElements(@Nullable String index) {
    if (!indexMappings.mixedElementEncodings()) {
      return dataType.hasByteElements();
    }
    if (index == null || !indexMappings.indices().contains(index)) {
      return null;
    }
    return indexMappings.byteElementIndices().contains(index);
  }

  /** Always {@link ExprUDT#EXPR_VECTOR}. */
  public ExprUDT getUdt() {
    return ExprUDT.EXPR_VECTOR;
  }

  /** Always {@link ExprCoreType#VECTOR}. */
  public ExprCoreType getExprType() {
    return ExprCoreType.VECTOR;
  }

  /** A copy with a different nullability. Canonize it through the type factory. */
  public ExprVectorType withNullability(boolean nullable) {
    return new ExprVectorType(
        getComponentType(), nullable, dimension, dataType, spaceType, indexMappings);
  }

  /** A copy with a different space type. Canonize it through the type factory. */
  public ExprVectorType withSpaceType(@Nullable String newSpaceType) {
    return new ExprVectorType(
        getComponentType(), isNullable(), dimension, dataType, newSpaceType, indexMappings);
  }

  /** A copy with different index mappings. Canonize it through the type factory. */
  public ExprVectorType withIndexMappings(IndexMappings newIndexMappings) {
    return new ExprVectorType(
        getComponentType(), isNullable(), dimension, dataType, spaceType, newIndexMappings);
  }

  /**
   * Whether a value of this type and one of {@code other} can share a column: same dimension and
   * data type. Nullability and space type are ignored.
   */
  public boolean isCombinableWith(ExprVectorType other) {
    return dimension == other.dimension && dataType == other.dataType;
  }

  /**
   * The runtime value of this type for a vector read from storage: a {@code List<Float>} or a
   * {@code List<Byte>}, the Java values of {@code ARRAY<REAL>} / {@code ARRAY<TINYINT>} that
   * generated code expects. See {@link #toRuntimeValue(Object, boolean)}.
   */
  public @Nullable List<Object> toRuntimeValue(@Nullable Object value) {
    return toRuntimeValue(value, dataType.hasByteElements());
  }

  /**
   * Convert a vector as storage returns it to the runtime value of a vector column. SQL and the V2
   * engine keep the stored value; only values that enter Calcite are converted. The k-NN plugin
   * accepts three forms in {@code _source}, and the result is the vector it indexed for each:
   *
   * <ul>
   *   <li>a JSON array of numbers, integers or decimals: each element converted;
   *   <li>a base64 string (k-NN 3.8 and later): for float and half_float vectors the bytes are
   *       float32 values in little-endian order, for byte and binary vectors each byte is an
   *       element;
   *   <li>a single number: a vector of one element.
   * </ul>
   *
   * @param value the stored value
   * @param byteElements {@code true} for {@code Byte} elements, {@code false} for {@code Float}
   * @return the converted list, or {@code null} for anything else: null, an empty array, an array
   *     with an element that is not a number, a string that is not base64 or, for float elements,
   *     whose length is not a multiple of 4. The k-NN plugin rejects such values at index time, so
   *     only null and missing values occur in practice.
   */
  public static @Nullable List<Object> toRuntimeValue(
      @Nullable Object value, boolean byteElements) {
    return toRuntimeValue(value, byteElements, byteElements);
  }

  /**
   * {@link #toRuntimeValue(Object, boolean)} for a value that was indexed with another element
   * encoding than the column has, e.g. under a column whose data type merges a float index and a
   * byte index: a base64 value is decoded as {@code storedAsBytes} says, and its elements are then
   * converted to the column's element type.
   *
   * @param storedAsBytes whether a base64 value holds one byte per element (byte, binary) rather
   *     than little-endian float32 values (float, half_float)
   * @param byteElements {@code true} for {@code Byte} elements, {@code false} for {@code Float}
   */
  public static @Nullable List<Object> toRuntimeValue(
      @Nullable Object value, boolean storedAsBytes, boolean byteElements) {
    if (value instanceof Number number) {
      return List.of(element(number, byteElements));
    }
    if (value instanceof String base64) {
      List<Object> decoded = fromBase64(base64, storedAsBytes);
      return decoded == null || storedAsBytes == byteElements
          ? decoded
          : toRuntimeValue(decoded, byteElements);
    }
    if (!(value instanceof List<?> list) || list.isEmpty()) {
      return null;
    }
    List<Object> elements = new ArrayList<>(list.size());
    for (Object element : list) {
      if (!(element instanceof Number number)) {
        return null;
      }
      elements.add(element(number, byteElements));
    }
    return elements;
  }

  private static Object element(Number number, boolean byteElements) {
    return byteElements ? (Object) number.byteValue() : (Object) number.floatValue();
  }

  /** A base64 vector, encoded as the k-NN plugin decodes it in {@code KNNVectorFieldMapper}. */
  private static @Nullable List<Object> fromBase64(String base64, boolean byteElements) {
    byte[] bytes;
    try {
      bytes = Base64.getDecoder().decode(base64);
    } catch (IllegalArgumentException e) {
      return null;
    }
    if (bytes.length == 0 || (!byteElements && bytes.length % Float.BYTES != 0)) {
      return null;
    }
    List<Object> elements = new ArrayList<>();
    if (byteElements) {
      for (byte b : bytes) {
        elements.add(b);
      }
    } else {
      FloatBuffer floats = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).asFloatBuffer();
      while (floats.hasRemaining()) {
        elements.add(floats.get());
      }
    }
    return elements;
  }

  @Override
  public RelDataTypeComparability getComparability() {
    return RelDataTypeComparability.NONE;
  }

  @Override
  protected void generateTypeString(StringBuilder sb, boolean withDetail) {
    if (dataType == null) {
      // Called from the ArraySqlType constructor, before this class's fields are assigned. The
      // constructor computes the digest again once they are.
      super.generateTypeString(sb, withDetail);
      return;
    }
    sb.append("VECTOR(")
        .append(dimension == UNKNOWN_DIMENSION ? "?" : String.valueOf(dimension))
        .append(", ")
        .append(dataType.name());
    if (withDetail && spaceType != null) {
      sb.append(", ").append(spaceType);
    }
    if (withDetail && indexMappings.mismatch()) {
      sb.append(", mismatch ").append(indexMappings.indices());
    }
    if (withDetail && indexMappings.mixedElementEncodings()) {
      sb.append(", byte elements in ").append(indexMappings.byteElementIndices());
    }
    sb.append(')');
  }

  @Override
  public boolean deepEquals(@Nullable Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof ExprVectorType that)) {
      return false;
    }
    return isNullable() == that.isNullable()
        && dimension == that.dimension
        && dataType == that.dataType
        && Objects.equals(spaceType, that.spaceType)
        && indexMappings.equals(that.indexMappings)
        && getComponentType().equals(that.getComponentType());
  }

  @Override
  public int deepHashCode() {
    return Objects.hash(
        ExprUDT.EXPR_VECTOR,
        isNullable(),
        dimension,
        dataType,
        spaceType,
        indexMappings,
        getComponentType());
  }
}
