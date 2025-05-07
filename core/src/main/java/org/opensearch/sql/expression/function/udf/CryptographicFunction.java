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
import org.apache.commons.codec.digest.DigestUtils;
import org.opensearch.sql.expression.function.ImplementorUDF;

public class CryptographicFunction extends ImplementorUDF {
  private CryptographicFunction(NotNullImplementor implementor, NullPolicy nullPolicy) {
    super(implementor, nullPolicy);
  }

  public static CryptographicFunction sha2() {
    return new CryptographicFunction(new Sha2Implementor(), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return ReturnTypes.VARCHAR.andThen(SqlTypeTransforms.FORCE_NULLABLE);
  }

  public static class Sha2Implementor implements NotNullImplementor {
    private static final ThreadLocal<Map<Integer, MessageDigest>> digests;

    static {
      digests =
          ThreadLocal.withInitial(
              () -> {
                try {
                  return Map.of(
                      224,
                      MessageDigest.getInstance("SHA-224"),
                      256,
                      DigestUtils.getSha256Digest(),
                      384,
                      DigestUtils.getSha384Digest(),
                      512,
                      DigestUtils.getSha512Digest());
                } catch (NoSuchAlgorithmException e) {
                  throw new RuntimeException(e);
                }
              });
    }

    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      return Expressions.call(Sha2Implementor.class, "getDigest", translatedOperands);
    }

    public static String getDigest(String input, int algorithm) {
      if (!digests.get().containsKey(algorithm)) {
        throw new IllegalArgumentException("Unsupported SHA2 algorithm: " + algorithm);
      }
      return getDigest(digests.get().get(algorithm), input);
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
  }
}
