/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.type;

import java.lang.reflect.Type;
import java.nio.charset.Charset;
import java.util.Map;
import lombok.Getter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactoryImpl.JavaType;
import org.apache.calcite.rel.type.RelDataTypeFamily;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.type.SqlTypeName;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.ExprUDT;
import org.opensearch.sql.data.type.ExprType;

@Getter
public abstract class ExprRelDataType<T extends RelDataType> extends RelDataTypeImpl {

  protected final ExprUDT udt;

  protected final T relType;

  protected ExprRelDataType(ExprUDT udt, T relType) {
    this.udt = udt;
    this.relType = relType;
    computeDigest();
  }

  public ExprType getExprType() {
    return udt.getExprCoreType();
  }

  public abstract Type getJavaType();

  public abstract RelDataType createWithNullability(
      OpenSearchTypeFactory typeFactory, boolean nullable);

  public abstract RelDataType createWithCharsetAndCollation(
      OpenSearchTypeFactory typeFactory, Charset charset, SqlCollation collation);

  /** Show udt name and hide the details */
  @Override
  public String toString() {
    return udt.toString();
  }

  @Override
  public boolean isNullable() {
    return relType.isNullable();
  }

  @Override
  protected abstract void generateTypeString(StringBuilder sb, boolean withDetail);

  @Override
  public RelDataTypeFamily getFamily() {
    return relType.getFamily();
  }

  @Override
  public @Nullable RelDataType getComponentType() {
    return relType.getComponentType();
  }

  /**
   * For {@link JavaType} created with {@link Map} class, we cannot get the key type. Use ANY as key
   * type.
   */
  @Override
  public @Nullable RelDataType getKeyType() {
    return relType.getKeyType();
  }

  /**
   * For {@link JavaType} created with {@link Map} class, we cannot get the value type. Use ANY as
   * value type.
   */
  @Override
  public @Nullable RelDataType getValueType() {
    return relType.getValueType();
  }

  @Override
  public @Nullable Charset getCharset() {
    return relType.getCharset();
  }

  @Override
  public @Nullable SqlCollation getCollation() {
    return relType.getCollation();
  }

  @Override
  public SqlTypeName getSqlTypeName() {
    return relType.getSqlTypeName();
  }
}
