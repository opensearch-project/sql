/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.serialization;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Base64;
import org.opensearch.sql.expression.Expression;

/** Default serializer that (de-)serialize expressions by JDK serialization. */
public class DefaultExpressionSerializer implements ExpressionSerializer {

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
      return (Expression) objectInput.readObject();
    } catch (Exception e) {
      throw new IllegalStateException("Failed to deserialize expression code: " + code, e);
    }
  }
}
