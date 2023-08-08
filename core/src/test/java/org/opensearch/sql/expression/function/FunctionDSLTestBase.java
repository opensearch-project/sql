/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import java.util.List;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.data.model.ExprMissingValue;
import org.opensearch.sql.data.model.ExprNullValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;

@ExtendWith(MockitoExtension.class)
public class FunctionDSLTestBase {
  @Mock FunctionProperties functionProperties;

  public static final ExprNullValue NULL = ExprNullValue.of();
  public static final ExprMissingValue MISSING = ExprMissingValue.of();
  protected static final ExprType ANY_TYPE = () -> "ANY";
  protected static final ExprValue ANY =
      new ExprValue() {
        @Override
        public Object value() {
          throw new RuntimeException();
        }

        @Override
        public ExprType type() {
          return ANY_TYPE;
        }

        @Override
        public String toString() {
          return "ANY";
        }

        @Override
        public int compareTo(ExprValue o) {
          throw new RuntimeException();
        }
      };
  static final FunctionName SAMPLE_NAME = FunctionName.of("sample");
  static final FunctionSignature SAMPLE_SIGNATURE_A =
      new FunctionSignature(SAMPLE_NAME, List.of(ExprCoreType.UNDEFINED));
  static final SerializableNoArgFunction<ExprValue> noArg = () -> ANY;
  static final SerializableFunction<ExprValue, ExprValue> oneArg = v -> ANY;
  static final SerializableBiFunction<FunctionProperties, ExprValue, ExprValue>
      oneArgWithProperties = (functionProperties, v) -> ANY;
  static final SerializableTriFunction<FunctionProperties, ExprValue, ExprValue, ExprValue>
      twoArgWithProperties = (functionProperties, v1, v2) -> ANY;

  static final SerializableQuadFunction<
          FunctionProperties, ExprValue, ExprValue, ExprValue, ExprValue>
      threeArgsWithProperties = (functionProperties, v1, v2, v3) -> ANY;

  static final SerializableBiFunction<ExprValue, ExprValue, ExprValue> twoArgs = (v1, v2) -> ANY;
  static final SerializableTriFunction<ExprValue, ExprValue, ExprValue, ExprValue> threeArgs =
      (v1, v2, v3) -> ANY;

  static final SerializableQuadFunction<ExprValue, ExprValue, ExprValue, ExprValue, ExprValue>
      fourArgs = (v1, v2, v3, v4) -> ANY;

  @Mock FunctionProperties mockProperties;
}
