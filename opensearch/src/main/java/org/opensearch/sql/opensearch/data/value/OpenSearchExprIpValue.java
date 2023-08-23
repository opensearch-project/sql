/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.data.value;

import java.util.Objects;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.data.model.AbstractExprValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.opensearch.data.type.OpenSearchIpType;

/**
 * OpenSearch IP ExprValue<br>
 * Todo, add this to avoid the unknown value type exception, the implementation will be changed.
 */
@RequiredArgsConstructor
public class OpenSearchExprIpValue extends AbstractExprValue {

  private final String ip;

  @Override
  public Object value() {
    return ip;
  }

  @Override
  public ExprType type() {
    return OpenSearchIpType.of();
  }

  @Override
  public int compare(ExprValue other) {
    return ip.compareTo(((OpenSearchExprIpValue) other).ip);
  }

  @Override
  public boolean equal(ExprValue other) {
    return ip.equals(((OpenSearchExprIpValue) other).ip);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(ip);
  }
}
