/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf.crypto;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeTransforms;
import org.opensearch.sql.expression.function.ImplementorUDF;

public class CryptographicFunction extends ImplementorUDF {
  private CryptographicFunction(String algorithm, NullPolicy nullPolicy) {
    super(new CryptographicImplementor(algorithm), nullPolicy);
  }

  public static CryptographicFunction md5() {
    return new CryptographicFunction("MD5", NullPolicy.ARG0);
  }

  public static CryptographicFunction sha1() {
    return new CryptographicFunction("SHA-1", NullPolicy.ARG0);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return ReturnTypes.VARCHAR.andThen(SqlTypeTransforms.FORCE_NULLABLE);
  }

  public static class CryptographicImplementor implements NotNullImplementor {
    private final MessageDigest digest;

    public CryptographicImplementor(String algorithm) {
      try {
        this.digest = MessageDigest.getInstance(algorithm);
      } catch (NoSuchAlgorithmException e) {
        throw new IllegalArgumentException(algorithm + " is not supported");
      }
    }

    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      return Expressions.call(
          Expressions.parameter(this.getClass(), "this"), "getDigest", translatedOperands);
    }

    public String getDigest(String input) {
      byte[] hash = digest.digest(input.getBytes());
      StringBuilder hexString = new StringBuilder();
      for (byte b : hash) {
        String hex = Integer.toHexString(0xff & b);
        if (hex.length() == 1) {
          hexString.append('0');
        }
        hexString.append(hex);
      }
      return hexString.toString();
    }
  }
}
