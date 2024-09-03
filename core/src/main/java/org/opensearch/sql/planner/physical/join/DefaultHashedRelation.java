package org.opensearch.sql.planner.physical.join;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.opensearch.sql.data.model.ExprValue;

public class DefaultHashedRelation implements HashedRelation, Serializable {

  private final Map<ExprValue, List<ExprValue>> map = new HashMap<>();
  private int numKeys;
  private int numValues;

  @Override
  public List<ExprValue> get(ExprValue key) {
    return map.get(key);
  }

  @Override
  public ExprValue getValue(ExprValue key) {
    List<ExprValue> values = map.get(key);
    return values != null && !values.isEmpty() ? values.getFirst() : null;
  }

  @Override
  public boolean containsKey(ExprValue key) {
    return map.containsKey(key);
  }

  @Override
  public Iterator<ExprValue> keyIterator() {
    return map.keySet().iterator();
  }

  @Override
  public boolean isUniqueKey() {
    return numKeys == numValues;
  }

  @Override
  public void close() {
    map.clear();
  }

  @Override
  public void put(ExprValue key, ExprValue value) {
    map.computeIfAbsent(key, k -> new ArrayList<>()).add(value);
    numKeys++;
    numValues++;
  }
}
