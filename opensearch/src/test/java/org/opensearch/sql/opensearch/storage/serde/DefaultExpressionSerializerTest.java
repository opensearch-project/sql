/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.serde;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.data.type.ExprCoreType.BOOLEAN;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.LONG;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.expression.DSL.literal;
import static org.opensearch.sql.expression.DSL.ref;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ExpressionNodeVisitor;
import org.opensearch.sql.expression.conditional.cases.WhenClause;
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

  @Test
  public void default_limits_admit_conditional_count_with_not_in_list() {
    // CASE WHEN channelNo NOT IN (2,3,4,5,6,8,10,11) AND abandoned = FALSE THEN contactNo END,
    // the argument of COUNT(DISTINCT ...). Its graph reaches serialization depth 28 and was
    // rejected by the previous default max_depth of 20.
    Expression original =
        DSL.cases(
            null,
            DSL.when(
                DSL.and(
                    DSL.not(inList(() -> ref("channelNo", INTEGER), 2, 3, 4, 5, 6, 8, 10, 11)),
                    DSL.equal(ref("abandoned", BOOLEAN), literal(false))),
                ref("contactNo", LONG)));
    String code = serializer.serialize(original);

    assertEquals(original, serializer.deserialize(code));

    ExpressionSerializer previousDefaults =
        new DefaultExpressionSerializer(() -> settingsWith(20, 1000, 15000));
    assertThrows(IllegalStateException.class, () -> previousDefaults.deserialize(code));
  }

  @Test
  public void default_limits_admit_largest_in_list_within_script_size_limit() {
    // IN-lists grow references and bytes linearly: 250 values need about 7000 references and 46 KB.
    // That is close to the largest list whose encoded script still fits the default
    // script.max_size_in_bytes (65535), so the default limits never reject it first.
    int[] values = IntStream.range(0, 250).toArray();
    Expression original = inList(() -> ref("channelNo", INTEGER), values);
    String code = serializer.serialize(original);

    assertTrue(code.length() <= 65535);
    assertEquals(original, serializer.deserialize(code));

    ExpressionSerializer previousDefaults =
        new DefaultExpressionSerializer(() -> settingsWith(20, 1000, 15000));
    assertThrows(IllegalStateException.class, () -> previousDefaults.deserialize(code));
  }

  @Test
  public void default_limits_admit_deeply_nested_conditions() {
    // 20 alternating levels of AND/OR inside a CASE reach serialization depth 72, the deepest
    // legitimate expression measured; it was rejected by the previous default max_depth of 20.
    Expression condition = DSL.greater(ref("balance", LONG), literal(0L));
    for (int i = 20; i >= 1; i--) {
      condition =
          i % 2 == 0
              ? DSL.and(DSL.greater(ref("age", INTEGER), literal(i)), condition)
              : DSL.or(DSL.equal(ref("male", BOOLEAN), literal(true)), condition);
    }
    Expression original = DSL.cases(null, DSL.when(condition, literal(1)));
    String code = serializer.serialize(original);

    assertEquals(original, serializer.deserialize(code));

    ExpressionSerializer previousDefaults =
        new DefaultExpressionSerializer(() -> settingsWith(20, 1000, 15000));
    assertThrows(IllegalStateException.class, () -> previousDefaults.deserialize(code));
  }

  @Test
  public void default_limits_admit_case_with_many_when_clauses() {
    // 25 WHEN clauses with three conditions each need about 2200 references and 18 KB, which the
    // previous default max_refs of 1000 and max_bytes of 15000 rejected.
    List<WhenClause> whens = new ArrayList<>();
    for (int i = 0; i < 25; i++) {
      whens.add(
          DSL.when(
              DSL.and(
                  DSL.and(
                      DSL.equal(ref("age", INTEGER), literal(20 + i)),
                      DSL.equal(ref("male", BOOLEAN), literal(true))),
                  DSL.greater(ref("balance", LONG), literal(i * 1000L))),
              literal(i)));
    }
    Expression original = DSL.cases(literal(-1), whens.toArray(new WhenClause[0]));
    String code = serializer.serialize(original);

    assertEquals(original, serializer.deserialize(code));

    ExpressionSerializer previousDefaults =
        new DefaultExpressionSerializer(() -> settingsWith(20, 1000, 15000));
    assertThrows(IllegalStateException.class, () -> previousDefaults.deserialize(code));
  }

  @Test
  public void default_limits_reject_excessive_nesting_before_stack_overflow() {
    // 60 nested NOTs reach a serialization depth of about 185: over the default max_depth, and
    // rejected by the filter with a regular exception rather than exhausting the thread stack.
    Expression nested = ref("abandoned", BOOLEAN);
    for (int i = 0; i < 60; i++) {
      nested = DSL.not(nested);
    }
    String code = serializer.serialize(nested);

    var exception = assertThrows(IllegalStateException.class, () -> serializer.deserialize(code));
    assertTrue(exception.getMessage().contains("Failed to deserialize"));
  }

  /**
   * Builds {@code field IN (values)} the way the analyzer does: a balanced tree of ORs, with a
   * fresh field reference in each comparison.
   */
  private static Expression inList(Supplier<Expression> field, int... values) {
    return orTree(field, values, 0, values.length);
  }

  private static Expression orTree(Supplier<Expression> field, int[] values, int start, int end) {
    if (end - start == 1) {
      return DSL.equal(field.get(), literal(values[start]));
    }
    int mid = (start + end) / 2;
    return DSL.or(orTree(field, values, start, mid), orTree(field, values, mid, end));
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
