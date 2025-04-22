/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf.crypto;

import java.security.MessageDigest;
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

public class Sha2Function extends ImplementorUDF {
  public Sha2Function() {
    super(new Sha2Implementor(), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return ReturnTypes.VARCHAR.andThen(SqlTypeTransforms.FORCE_NULLABLE);
  }

  public static class Sha2Implementor implements NotNullImplementor {
    private final Map<Integer, MessageDigest> digests;

    public Sha2Implementor() {
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
      } catch (Exception e) {
        throw new IllegalArgumentException("SHA-2 algorithms are not supported", e);
      }
    }

    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      return Expressions.call(
          Expressions.parameter(this.getClass(), "this"), "getDigest", translatedOperands);
    }

    public String getDigest(String input, int algorithm) {
      MessageDigest digest = digests.get(algorithm);
      if (digest == null) {
        throw new IllegalArgumentException("Unsupported SHA-2 algorithm: " + algorithm);
      }
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
