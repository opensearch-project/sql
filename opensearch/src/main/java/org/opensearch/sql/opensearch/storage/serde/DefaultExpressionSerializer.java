/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.serde;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Base64;
import java.util.function.Supplier;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.utils.DeserializationFilterUtil;

/** Default serializer that (de-)serialize expressions by JDK serialization. */
public class DefaultExpressionSerializer implements ExpressionSerializer {

  /**
   * Supplies cluster settings for deserialization structural limits, resolved lazily because the
   * script engine is created before plugin settings are initialized. Null falls back to defaults.
   */
  private final Supplier<Settings> settingsSupplier;

  public DefaultExpressionSerializer() {
    this(null);
  }

  public DefaultExpressionSerializer(Supplier<Settings> settingsSupplier) {
    this.settingsSupplier = settingsSupplier;
  }

  @Override
  public String serialize(Expression expr) {
    try {
      ByteArrayOutputStream output = new ByteArrayOutputStream();
      ObjectOutputStream objectOutput = new ObjectOutputStream(output);
      objectOutput.writeObject(expr);
      objectOutput.flush();
      return Base64.getEncoder().encodeToString(output.toByteArray());
    } catch (IOException e) {
      throw new IllegalStateException("Failed to serialize expression: " + expr, e);
    }
  }

  @Override
  public Expression deserialize(String code) {
    try {
      ByteArrayInputStream input = new ByteArrayInputStream(Base64.getDecoder().decode(code));
      ObjectInputStream objectInput = new ObjectInputStream(input);
      Settings settings = settingsSupplier == null ? null : settingsSupplier.get();
      objectInput.setObjectInputFilter(DeserializationFilterUtil.createFilter(settings, ""));
      return (Expression) objectInput.readObject();
    } catch (Exception e) {
      throw new IllegalStateException("Failed to deserialize expression code: " + code, e);
    }
  }
}
