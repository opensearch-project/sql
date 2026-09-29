/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.serde;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.expression.DSL.literal;
import static org.opensearch.sql.expression.DSL.ref;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ExpressionNodeVisitor;
import org.opensearch.sql.expression.env.Environment;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class DefaultExpressionSerializerTest {

  private final ExpressionSerializer serializer = new DefaultExpressionSerializer();

  @Test
  public void can_serialize_and_deserialize_literals() {
    Expression original = literal(10);
    Expression actual = serializer.deserialize(serializer.serialize(original));
    assertEquals(original, actual);
  }

  @Test
  public void can_serialize_and_deserialize_references() {
    Expression original = ref("name", STRING);
    Expression actual = serializer.deserialize(serializer.serialize(original));
    assertEquals(original, actual);
  }

  @Test
  public void can_serialize_and_deserialize_predicates() {
    Expression original = DSL.or(literal(true), DSL.less(literal(1), literal(2)));
    Expression actual = serializer.deserialize(serializer.serialize(original));
    assertEquals(original, actual);
  }

  @Test
  public void can_serialize_and_deserialize_functions() {
    Expression original = DSL.abs(literal(30.0));
    Expression actual = serializer.deserialize(serializer.serialize(original));
    assertEquals(original, actual);
  }

  @Test
  public void cannot_serialize_illegal_expression() {
    Expression illegalExpr =
        new Expression() {
          private final Object object = new Object(); // non-serializable

          @Override
          public ExprValue valueOf(Environment<Expression, ExprValue> valueEnv) {
            return null;
          }

          @Override
          public ExprType type() {
            return null;
          }

          @Override
          public <T, C> T accept(ExpressionNodeVisitor<T, C> visitor, C context) {
            return null;
          }
        };
    assertThrows(IllegalStateException.class, () -> serializer.serialize(illegalExpr));
  }

  @Test
  public void cannot_deserialize_illegal_expression_code() {
    assertThrows(IllegalStateException.class, () -> serializer.deserialize("hello world"));
  }

  @Test
  public void deserialize_honors_configured_structural_limits() {
    // A serializer wired with a very tight refs limit must reject an otherwise-valid expression,
    // proving the injected Settings supplier actually drives the deserialization filter.
    Settings tightLimits = settingsWith(/*depth*/ 20, /*refs*/ 1, /*bytes*/ 15000);
    ExpressionSerializer limited = new DefaultExpressionSerializer(() -> tightLimits);

    Expression original = DSL.or(literal(true), DSL.less(literal(1), literal(2)));
    String code = serializer.serialize(original);

    // Default limits (no override) round-trip the same payload fine.
    assertEquals(original, serializer.deserialize(code));

    // maxrefs=1 rejects the multi-object graph.
    var exception = assertThrows(IllegalStateException.class, () -> limited.deserialize(code));
    assertTrue(exception.getMessage().contains("Failed to deserialize"));
  }

  private static Settings settingsWith(int depth, int refs, int bytes) {
    Map<Settings.Key, Object> values =
        Map.of(
            Settings.Key.DESERIALIZATION_MAX_DEPTH, depth,
            Settings.Key.DESERIALIZATION_MAX_REFS, refs,
            Settings.Key.DESERIALIZATION_MAX_BYTES, bytes);
    return new Settings() {
      @Override
      @SuppressWarnings("unchecked")
      public <T> T getSettingValue(Settings.Key key) {
        return (T) values.get(key);
      }

      @Override
      public List<?> getSettings() {
        return List.of();
      }
    };
  }

  @Test
  public void deserialize_rejects_disallowed_class() throws Exception {
    java.io.ByteArrayOutputStream output = new java.io.ByteArrayOutputStream();
    java.io.ObjectOutputStream objectOutput = new java.io.ObjectOutputStream(output);
    objectOutput.writeObject(new java.net.URL("http://example.com"));
    objectOutput.flush();
    String encoded = java.util.Base64.getEncoder().encodeToString(output.toByteArray());
    var exception =
        assertThrows(IllegalStateException.class, () -> serializer.deserialize(encoded));
    assertTrue(exception.getMessage().contains("Failed to deserialize"));
  }
}
