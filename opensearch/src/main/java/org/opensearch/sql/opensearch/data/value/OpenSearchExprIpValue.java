/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

/*
 *
 *    Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License").
 *    You may not use this file except in compliance with the License.
 *    A copy of the License is located at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    or in the "license" file accompanying this file. This file is distributed
 *    on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *    express or implied. See the License for the specific language governing
 *    permissions and limitations under the License.
 *
 */

package org.opensearch.sql.opensearch.data.value;

import static org.opensearch.sql.opensearch.data.type.OpenSearchDataType.OPENSEARCH_IP;

import java.util.Objects;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.data.model.AbstractExprValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprType;

/**
 * OpenSearch IP ExprValue.
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
    return OPENSEARCH_IP;
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
