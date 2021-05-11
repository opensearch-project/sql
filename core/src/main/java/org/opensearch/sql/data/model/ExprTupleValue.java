/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

/*
 *   Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package org.opensearch.sql.data.model;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.storage.bindingtuple.BindingTuple;
import org.opensearch.sql.storage.bindingtuple.LazyBindingTuple;

/**
 * Expression Tuple Value.
 */
@RequiredArgsConstructor
public class ExprTupleValue extends AbstractExprValue {

  private final LinkedHashMap<String, ExprValue> valueMap;

  public static ExprTupleValue fromExprValueMap(Map<String, ExprValue> map) {
    LinkedHashMap<String, ExprValue> linkedHashMap = new LinkedHashMap<>(map);
    return new ExprTupleValue(linkedHashMap);
  }

  @Override
  public Object value() {
    LinkedHashMap<String, Object> resultMap = new LinkedHashMap<>();
    for (Entry<String, ExprValue> entry : valueMap.entrySet()) {
      resultMap.put(entry.getKey(), entry.getValue().value());
    }
    return resultMap;
  }

  @Override
  public ExprType type() {
    return ExprCoreType.STRUCT;
  }

  @Override
  public String toString() {
    return valueMap.entrySet()
        .stream()
        .map(entry -> String.format("%s:%s", entry.getKey(), entry.getValue()))
        .collect(Collectors.joining(",", "{", "}"));
  }

  @Override
  public BindingTuple bindingTuples() {
    return new LazyBindingTuple(() -> this);
  }

  @Override
  public Map<String, ExprValue> tupleValue() {
    return valueMap;
  }

  @Override
  public ExprValue keyValue(String key) {
    return valueMap.getOrDefault(key, ExprMissingValue.of());
  }

  /**
   * Override the equals method.
   * @return true for equal, otherwise false.
   */
  public boolean equal(ExprValue o) {
    if (!(o instanceof ExprTupleValue)) {
      return false;
    } else {
      ExprTupleValue other = (ExprTupleValue) o;
      Iterator<Entry<String, ExprValue>> thisIterator = this.valueMap.entrySet().iterator();
      Iterator<Entry<String, ExprValue>> otherIterator = other.valueMap.entrySet().iterator();
      while (thisIterator.hasNext() && otherIterator.hasNext()) {
        Entry<String, ExprValue> thisEntry = thisIterator.next();
        Entry<String, ExprValue> otherEntry = otherIterator.next();
        if (!(thisEntry.getKey().equals(otherEntry.getKey())
            && thisEntry.getValue().equals(otherEntry.getValue()))) {
          return false;
        }
      }
      return !(thisIterator.hasNext() || otherIterator.hasNext());
    }
  }

  /**
   * Only compare the size of the map.
   */
  @Override
  public int compare(ExprValue other) {
    return Integer.compare(valueMap.size(), other.tupleValue().size());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(valueMap);
  }
}
