package org.opensearch.sql.planner.physical.join;

import java.util.Iterator;
import java.util.List;
import org.opensearch.sql.data.model.ExprValue;

public interface HashedRelation {

  /** Return matched rows. */
  List<ExprValue> get(ExprValue key);

  /**
   * Return the single matched row. Only used in {@link DefaultHashedRelation#isUniqueKey()} is
   * true.
   */
  ExprValue getValue(ExprValue key);

  /** Whether the key exists. */
  boolean containsKey(ExprValue key);

  /** Return the key iterator. */
  Iterator<ExprValue> keyIterator();

  /** Whether the key is unique. */
  boolean isUniqueKey();

  /** Put the key-value pair into the relation. */
  void put(ExprValue key, ExprValue value);

  /** Release the resources */
  void close();
}
