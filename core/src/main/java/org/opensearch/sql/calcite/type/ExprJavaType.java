/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.type;

import java.lang.reflect.Type;
import java.nio.charset.Charset;
import org.apache.calcite.rel.type.RelDataTypeFactoryImpl.JavaType;
import org.apache.calcite.sql.SqlCollation;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.ExprUDT;

/**
 * The JavaType for an OpenSearch UDT backed by a {@link JavaType}. The UDT which needs to use
 * self-implemented java class should extend this. Its javaClazz should override equals() and
 * hashCode() methods. For example, {@link org.opensearch.sql.data.model.ExprIpValue} (javaClazz of
 * {@link ExprIPType}) overrides the equals() and hashCode().
 */
public abstract class ExprJavaType extends AbstractExprRelDataType<JavaType> {
  protected ExprJavaType(OpenSearchTypeFactory typeFactory, ExprUDT exprUDT, Class<?> javaClazz) {
    super(exprUDT, (JavaType) typeFactory.createJavaType(javaClazz));
  }

  protected ExprJavaType(ExprUDT exprUDT, JavaType type) {
    super(exprUDT, type);
  }

  @Override
  protected void generateTypeString(StringBuilder sb, boolean withDetail) {
    sb.append(udt.name());
    sb.append("JavaType(");
    sb.append(super.relType.getJavaClass());
    sb.append(")");
  }

  @Override
  public Type getJavaType() {
    return super.relType.getJavaClass();
  }

  /** Subclasses must construct their own variant when re-wrapped. */
  protected abstract ExprJavaType cloneWith(JavaType inner);

  @Override
  public ExprJavaType createWithNullability(OpenSearchTypeFactory typeFactory, boolean nullable) {
    if (isNullable() == nullable) {
      return this;
    }
    return cloneWith((JavaType) typeFactory.createTypeWithNullability(super.relType, nullable));
  }

  @Override
  public ExprJavaType createWithCharsetAndCollation(
      OpenSearchTypeFactory typeFactory, Charset charset, SqlCollation collation) {
    return cloneWith(
        (JavaType)
            typeFactory.createTypeWithCharsetAndCollation(super.relType, charset, collation));
  }
}
