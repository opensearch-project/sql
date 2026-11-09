/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.serde;

import com.google.common.collect.ImmutableSet;
import java.util.Set;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Validates a {@link RexNode} rebuilt from an untrusted, client-supplied script payload before it
 * reaches code generation and Janino compilation.
 *
 * <p>An inlined {@link RexLiteral} is the only channel through which attacker-controlled text can
 * reach the generated Java source: operators resolve against a fixed operator table and parameters
 * travel as runtime data. {@code RexStandardizer} replaces every string/collection literal with a
 * {@link org.apache.calcite.rex.RexDynamicParam} when serializing, so a legitimate script never
 * inlines one. This enforces that invariant on deserialize: only the literal type families the
 * serializer may leave inlined are allowed; the rest are rejected.
 */
public final class RexLiteralSafetyValidator {

  private RexLiteralSafetyValidator() {}

  /**
   * Literal type families the serializer may leave inlined — each emitted by Calcite as a
   * numeric/boolean/temporal constant, never as raw quoted text. {@code SARG} is kept inlined by
   * {@code RexStandardizer} and its string bounds go through the safe (backslash-escaping) writer.
   */
  private static final Set<SqlTypeName> ALLOWED_INLINED_TYPES =
      ImmutableSet.<SqlTypeName>builder()
          .add(
              SqlTypeName.BOOLEAN,
              SqlTypeName.TINYINT,
              SqlTypeName.SMALLINT,
              SqlTypeName.INTEGER,
              SqlTypeName.BIGINT,
              SqlTypeName.DECIMAL,
              SqlTypeName.FLOAT,
              SqlTypeName.REAL,
              SqlTypeName.DOUBLE,
              SqlTypeName.DATE,
              SqlTypeName.TIME,
              SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE,
              SqlTypeName.TIMESTAMP,
              SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
              SqlTypeName.SYMBOL,
              SqlTypeName.NULL,
              SqlTypeName.SARG)
          .addAll(SqlTypeName.INTERVAL_TYPES)
          .build();

  /**
   * Rejects the node if it contains any inlined literal whose type is not in the allowlist.
   *
   * @param rexNode the deserialized expression to validate
   * @throws IllegalStateException if an unsafe inlined literal is found
   */
  public static void validate(RexNode rexNode) {
    rexNode.accept(
        new RexVisitorImpl<Void>(true) {
          @Override
          public Void visitLiteral(RexLiteral literal) {
            // A Sarg literal's getType() is its operand type (e.g. VARCHAR); identify it by its
            // own type name so string Sargs produced by RexStandardizer are not rejected.
            if (literal.getTypeName() != SqlTypeName.SARG) {
              checkInlinedType(literal.getType());
            }
            return null;
          }
        });
  }

  private static void checkInlinedType(RelDataType type) {
    SqlTypeName typeName = type.getSqlTypeName();
    if (typeName == null || !ALLOWED_INLINED_TYPES.contains(typeName)) {
      throw new IllegalStateException(
          "Rejected unsafe inlined literal of type '"
              + typeName
              + "' in deserialized script expression");
    }
  }
}
