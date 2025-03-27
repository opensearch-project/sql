/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

/*
 * This file contains code from the Apache Calcite project (original license below).
 * It contains modifications, which are licensed as above:
 */

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opensearch.sql.calcite.type;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import java.nio.charset.Charset;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.type.AbstractSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.SerializableCharset;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.ExprUDT;
import org.opensearch.sql.data.type.ExprType;

/**
 * ExprBasicSqlUDT represents a standard atomic SQL type (excluding interval types) for a {@link
 * ExprType}.
 *
 * <p>Instances of this class are immutable.
 *
 * <p>Compared to Calcite's BasicSqlType, this class supports flexible inheritance of UDTs.
 *
 * <p>TODO: may no need to be abstract if it can cover all functions of subclass.
 */
public abstract class ExprBasicSqlUDT extends AbstractSqlType implements ExprUserDefinedType {
  // ~ Static fields/initializers ---------------------------------------------

  // ~ Instance fields --------------------------------------------------------

  private final ExprUDT exprUDT;

  private final int precision;
  private final int scale;
  protected final RelDataTypeSystem typeSystem;
  private final @Nullable SqlCollation collation;
  private final @Nullable SerializableCharset wrappedCharset;

  // ~ Constructors -----------------------------------------------------------

  /**
   * Constructs a type with no parameters. This should only be called from a factory method.
   *
   * @param typeSystem Type system
   * @param typeName Type name
   */
  public ExprBasicSqlUDT(RelDataTypeSystem typeSystem, SqlTypeName typeName, ExprUDT udtName) {
    this(typeSystem, typeName, udtName, false);
  }

  protected ExprBasicSqlUDT(
      RelDataTypeSystem typeSystem, SqlTypeName sqlTypeName, ExprUDT udtName, boolean nullable) {
    this(
        typeSystem,
        sqlTypeName,
        udtName,
        nullable,
        PRECISION_NOT_SPECIFIED,
        SCALE_NOT_SPECIFIED,
        null,
        null);
    checkPrecScale(typeName, false, false);
  }

  /**
   * Constructs a type with precision/length but no scale.
   *
   * @param typeSystem Type system
   * @param typeName Type name
   * @param precision Precision (called length for some types)
   */
  public ExprBasicSqlUDT(
      RelDataTypeSystem typeSystem, SqlTypeName typeName, ExprUDT udtName, int precision) {
    this(typeSystem, typeName, udtName, false, precision, SCALE_NOT_SPECIFIED, null, null);
    checkPrecScale(typeName, true, false);
  }

  /**
   * Constructs a type with precision/length and scale.
   *
   * @param typeSystem Type system
   * @param typeName Type name
   * @param precision Precision (called length for some types)
   * @param scale Scale
   */
  public ExprBasicSqlUDT(
      RelDataTypeSystem typeSystem,
      SqlTypeName typeName,
      ExprUDT udtName,
      int precision,
      int scale) {
    this(typeSystem, typeName, udtName, false, precision, scale, null, null);
    checkPrecScale(typeName, true, true);
  }

  /** Internal constructor. */
  public ExprBasicSqlUDT(
      RelDataTypeSystem typeSystem,
      SqlTypeName sqlTypeName,
      ExprUDT udtName,
      boolean nullable,
      int precision,
      int scale,
      @Nullable SqlCollation collation,
      @Nullable SerializableCharset wrappedCharset) {
    super(sqlTypeName, nullable, null);
    this.exprUDT = requireNonNull(udtName, "udtName");
    this.typeSystem = requireNonNull(typeSystem, "typeSystem");
    this.precision = precision;
    this.scale = scale;
    this.collation = collation;
    this.wrappedCharset = wrappedCharset;
    computeDigest();
  }

  protected abstract ExprBasicSqlUDT createInstance(
      RelDataTypeSystem typeSystem,
      SqlTypeName typeName,
      ExprUDT udt,
      boolean nullable,
      int precision,
      int scale,
      @Nullable SqlCollation collation,
      @Nullable SerializableCharset wrappedCharset);

  /** Throws if {@code typeName} does not allow the given combination of precision and scale. */
  protected static void checkPrecScale(
      SqlTypeName typeName, boolean precisionSpecified, boolean scaleSpecified) {
    if (!typeName.allowsPrecScale(precisionSpecified, scaleSpecified)) {
      throw new AssertionError(
          "typeName.allowsPrecScale("
              + precisionSpecified
              + ", "
              + scaleSpecified
              + "): "
              + typeName);
    }
  }

  // ~ Methods ----------------------------------------------------------------

  /** Constructs a type with nullablity. */
  public ExprBasicSqlUDT createWithNullability(boolean nullable) {
    if (nullable == this.isNullable) {
      return this;
    }
    return createInstance(
        this.typeSystem,
        this.typeName,
        this.exprUDT,
        nullable,
        this.precision,
        this.scale,
        this.collation,
        this.wrappedCharset);
  }

  /**
   * Constructs a type with charset and collation.
   *
   * <p>This must be a character type.
   */
  public ExprBasicSqlUDT createWithCharsetAndCollation(Charset charset, SqlCollation collation) {
    checkArgument(SqlTypeUtil.inCharFamily(this));
    return createInstance(
        this.typeSystem,
        this.typeName,
        this.exprUDT,
        this.isNullable,
        this.precision,
        this.scale,
        collation,
        SerializableCharset.forCharset(charset));
  }

  @Override
  public int getPrecision() {
    if (precision == PRECISION_NOT_SPECIFIED) {
      return typeSystem.getDefaultPrecision(typeName);
    }
    return precision;
  }

  @Override
  public int getScale() {
    if (scale == SCALE_NOT_SPECIFIED) {
      switch (typeName) {
        case TINYINT:
        case SMALLINT:
        case INTEGER:
        case BIGINT:
        case DECIMAL:
          return 0;
        default:
          // fall through
      }
    }
    return scale;
  }

  @Override
  public @Nullable Charset getCharset() {
    return wrappedCharset == null ? null : wrappedCharset.getCharset();
  }

  @Override
  public @Nullable SqlCollation getCollation() {
    return collation;
  }

  // implement RelDataTypeImpl
  @Override
  protected void generateTypeString(StringBuilder sb, boolean withDetail) {
    // Called to make the digest, which equals() compares;
    // so equivalent data types must produce identical type strings.
    sb.append(exprUDT.name());
    sb.append(' ');
    sb.append(typeName.name());
    boolean printPrecision = precision != PRECISION_NOT_SPECIFIED;
    boolean printScale = scale != SCALE_NOT_SPECIFIED;

    if (printPrecision) {
      sb.append('(');
      sb.append(getPrecision());
      if (printScale) {
        sb.append(", ");
        sb.append(getScale());
      }
      sb.append(')');
    }
    if (!withDetail) {
      return;
    }
    if (wrappedCharset != null
        && !SqlCollation.IMPLICIT.getCharset().equals(wrappedCharset.getCharset())) {
      sb.append(" CHARACTER SET \"");
      sb.append(wrappedCharset.getCharset().name());
      sb.append("\"");
    }
    if (collation != null
        && collation != SqlCollation.IMPLICIT
        && collation != SqlCollation.COERCIBLE) {
      sb.append(" COLLATE \"");
      sb.append(collation.getCollationName());
      sb.append("\"");
    }
  }

  @Override
  public String toString() {
    return exprUDT.toString();
  }

  /**
   * Returns a value which is a limit for this type.
   *
   * <p>For example,
   *
   * <table border="1">
   * <caption>Limits</caption>
   * <tr>
   * <th>Datatype</th>
   * <th>sign</th>
   * <th>limit</th>
   * <th>beyond</th>
   * <th>precision</th>
   * <th>scale</th>
   * <th>Returns</th>
   * </tr>
   * <tr>
   * <td>Integer</td>
   * <td>true</td>
   * <td>true</td>
   * <td>false</td>
   * <td>-1</td>
   * <td>-1</td>
   * <td>2147483647 (2 ^ 31 -1 = MAXINT)</td>
   * </tr>
   * <tr>
   * <td>Integer</td>
   * <td>true</td>
   * <td>true</td>
   * <td>true</td>
   * <td>-1</td>
   * <td>-1</td>
   * <td>2147483648 (2 ^ 31 = MAXINT + 1)</td>
   * </tr>
   * <tr>
   * <td>Integer</td>
   * <td>false</td>
   * <td>true</td>
   * <td>false</td>
   * <td>-1</td>
   * <td>-1</td>
   * <td>-2147483648 (-2 ^ 31 = MININT)</td>
   * </tr>
   * <tr>
   * <td>Boolean</td>
   * <td>true</td>
   * <td>true</td>
   * <td>false</td>
   * <td>-1</td>
   * <td>-1</td>
   * <td>TRUE</td>
   * </tr>
   * <tr>
   * <td>Varchar</td>
   * <td>true</td>
   * <td>true</td>
   * <td>false</td>
   * <td>10</td>
   * <td>-1</td>
   * <td>'ZZZZZZZZZZ'</td>
   * </tr>
   * </table>
   *
   * @param sign If true, returns upper limit, otherwise lower limit
   * @param limit If true, returns value at or near to overflow; otherwise value at or near to
   *     underflow
   * @param beyond If true, returns the value just beyond the limit, otherwise the value at the
   *     limit
   * @return Limit value
   */
  public @Nullable Object getLimit(boolean sign, SqlTypeName.Limit limit, boolean beyond) {
    int precision = typeName.allowsPrec() ? this.getPrecision() : -1;
    int scale = typeName.allowsScale() ? this.getScale() : -1;
    return typeName.getLimit(sign, limit, beyond, precision, scale);
  }

  @Override
  public ExprType getExprType() {
    return exprUDT.getExprCoreType();
  }
}
