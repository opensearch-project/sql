/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.data.model;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.storage.bindingtuple.BindingTuple;
import org.opensearch.sql.storage.bindingtuple.LazyBindingTuple;

/** Expression Tuple Value. */
@RequiredArgsConstructor
@AllArgsConstructor
public class ExprTupleValue extends AbstractExprValue {
  // Only used for Calcite to keep the field names in the same order as the schema.
  @Nullable private List<String> fieldNames;

  private final LinkedHashMap<String, ExprValue> valueMap;

  public static ExprTupleValue fromExprValueMap(Map<String, ExprValue> map) {
    return fromExprValueMap(null, map);
  }

  public static ExprTupleValue fromExprValueMap(
      List<String> fieldNames, Map<String, ExprValue> map) {
    LinkedHashMap<String, ExprValue> linkedHashMap = new LinkedHashMap<>(map);
    return new ExprTupleValue(fieldNames, linkedHashMap);
  }

  public static ExprTupleValue empty(List<String> fieldNames) {
    LinkedHashMap<String, ExprValue> linkedHashMap = new LinkedHashMap<>();
    return new ExprTupleValue(fieldNames, linkedHashMap);
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
  public Object valueForCalcite() {
    // Needs to keep the value the same sequence as the schema, and fill up missing values
    return fieldNames.stream()
        .map(fieldName -> valueMap.getOrDefault(fieldName, ExprMissingValue.of()).valueForCalcite())
        .toArray();
  }

  @Override
  public ExprType type() {
    return ExprCoreType.STRUCT;
  }

  @Override
  public String toString() {
    return valueMap.entrySet().stream()
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
   *
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

  /** Only compare the size of the map. */
  @Override
  public int compare(ExprValue other) {
    return Integer.compare(valueMap.size(), other.tupleValue().size());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(valueMap);
  }

  /** Implements mergeTo by merging deeply */
  @Override
  public ExprTupleValue mergeTo(ExprValue base) {
    if (base instanceof ExprTupleValue) {
      base.tupleValue()
          .forEach((key, value) -> this.tupleValue().merge(key, value, ExprValue::mergeTo));
    } else {
      throw new IllegalArgumentException(
          String.format("Cannot merge ExprTupleValue to %s", base.getClass().getSimpleName()));
    }
    return this;
  }
}
