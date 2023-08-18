/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.serialization;

import org.opensearch.sql.expression.Expression;

/** Expression serializer that (de-)serializes expression object. */
public interface ExpressionSerializer {

  /**
   * Serialize an expression.
   *
   * @param expr expression
   * @return serialized string
   */
  String serialize(Expression expr);

  /**
   * Deserialize an expression.
   *
   * @param code serialized code
   * @return original expression object
   */
  Expression deserialize(String code);
}
