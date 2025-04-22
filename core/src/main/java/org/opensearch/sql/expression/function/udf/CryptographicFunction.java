/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Map;
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
  private CryptographicFunction(NotNullImplementor implementor, NullPolicy nullPolicy) {
    super(implementor, nullPolicy);
  }

  public static CryptographicFunction md5() {
    return new CryptographicFunction(new Md5Implementor(), NullPolicy.ARG0);
  }

  public static CryptographicFunction sha1() {
    return new CryptographicFunction(new Sha1Implementor(), NullPolicy.ARG0);
  }

  public static CryptographicFunction sha2() {
    return new CryptographicFunction(new Sha2Implementor(), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return ReturnTypes.VARCHAR.andThen(SqlTypeTransforms.FORCE_NULLABLE);
  }

  private static String getDigest(MessageDigest digest, String input) {
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

  public static class Md5Implementor implements NotNullImplementor {
    private static final MessageDigest MD5_DIGEST;

    static {
      try {
        MD5_DIGEST = MessageDigest.getInstance("MD5");
      } catch (NoSuchAlgorithmException e) {
        throw new RuntimeException("MD5 algorithm not found", e);
      }
    }

    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      return Expressions.call(Md5Implementor.class, "getDigest", translatedOperands);
    }

    public static String getDigest(String input) {
      return CryptographicFunction.getDigest(MD5_DIGEST, input);
    }
  }

  public static class Sha1Implementor implements NotNullImplementor {
    private static final MessageDigest SHA1_DIGEST;

    static {
      try {
        SHA1_DIGEST = MessageDigest.getInstance("SHA-1");
      } catch (NoSuchAlgorithmException e) {
        throw new RuntimeException("SHA-1 algorithm not found", e);
      }
    }

    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      return Expressions.call(Sha1Implementor.class, "getDigest", translatedOperands);
    }

    public static String getDigest(String input) {
      return CryptographicFunction.getDigest(SHA1_DIGEST, input);
    }
  }

  public static class Sha2Implementor implements NotNullImplementor {
    private static final Map<Integer, MessageDigest> digests;

    static {
      try {
        digests =
            Map.of(
                224,
                MessageDigest.getInstance("SHA-224"),
                256,
                MessageDigest.getInstance("SHA-256"),
                384,
                MessageDigest.getInstance("SHA-384"),
                512,
                MessageDigest.getInstance("SHA-512"));
      } catch (NoSuchAlgorithmException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      return Expressions.call(Sha2Implementor.class, "getDigest", translatedOperands);
    }

    public static String getDigest(String input, int algorithm) {
      if (!digests.containsKey(algorithm)) {
        throw new IllegalArgumentException("Unsupported SHA2 algorithm: " + algorithm);
      }
      return CryptographicFunction.getDigest(digests.get(algorithm), input);
    }
  }
}
