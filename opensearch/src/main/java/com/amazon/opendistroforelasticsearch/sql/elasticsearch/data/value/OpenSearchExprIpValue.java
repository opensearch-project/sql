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

package com.amazon.opendistroforelasticsearch.sql.elasticsearch.data.value;

import static com.amazon.opendistroforelasticsearch.sql.elasticsearch.data.type.OpenSearchDataType.ES_IP;

import com.amazon.opendistroforelasticsearch.sql.data.model.AbstractExprValue;
import com.amazon.opendistroforelasticsearch.sql.data.model.ExprValue;
import com.amazon.opendistroforelasticsearch.sql.data.type.ExprType;
import java.util.Objects;
import lombok.RequiredArgsConstructor;

/**
 * Elasticsearch IP ExprValue.
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
    return ES_IP;
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
