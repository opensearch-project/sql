/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.correctness.runner.resultset;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.ToString;
import org.json.JSONPropertyName;
import org.opensearch.sql.legacy.utils.StringUtils;

/**
 * Query result for equality comparison. Based on different type of query, such as query
 * with/without ORDER BY and query with SELECT columns or just *, order of column and row may matter
 * or not. So the internal data structure of this class is passed in from outside either list or
 * set, hash map or linked hash map etc.
 */
@ToString
public class DBResult {

  /** Possible types for floating point number H2 2.x use DOUBLE PRECISION instead of DOUBLE. */
  private static final Set<String> FLOAT_TYPES =
      ImmutableSet.of("FLOAT", "DOUBLE", "REAL", "DOUBLE PRECISION", "DECFLOAT");

  /** Possible types for varchar. H2 2.x use CHARACTER VARYING instead of VARCHAR. */
  private static final Set<String> VARCHAR = ImmutableSet.of("CHARACTER VARYING", "VARCHAR");

  /** Database name for display */
  private final String databaseName;

  /** Column name and types from result set meta data */
  @Getter private final Collection<Type> schema;

  /** Data rows from result set */
  private final Collection<Row> dataRows;

  /** In theory, a result set is a multi-set (bag) that allows duplicate and doesn't have order. */
  public static DBResult result(String databaseName) {
    return new DBResult(databaseName, new ArrayList<>(), HashMultiset.create());
  }

  /**
   * But for queries with ORDER BY clause, we want to preserve the original order of data rows so we
   * can check if the order is correct.
   */
  public static DBResult resultInOrder(String databaseName) {
    return new DBResult(databaseName, new ArrayList<>(), new ArrayList<>());
  }

  public DBResult(String databaseName, Collection<Type> schema, Collection<Row> rows) {
    this.databaseName = databaseName;
    this.schema = schema;
    this.dataRows = rows;
  }

  public int columnSize() {
    return schema.size();
  }

  public void addColumn(String name, String type) {
    type = StringUtils.toUpper(type);

    // Ignore float type by assigning all type names string to it.
    if (FLOAT_TYPES.contains(type)) {
      type = FLOAT_TYPES.toString();
    } else if (VARCHAR.contains(type)) {
      type = "VARCHAR";
    }
    schema.add(new Type(StringUtils.toUpper(name), type));
  }

  public void addRow(Row row) {
    dataRows.add(row);
  }

  @JSONPropertyName("database")
  public String getDatabaseName() {
    return databaseName;
  }

  /** Flatten for simplifying json generated. */
  public Collection<Collection<Object>> getDataRows() {
    Collection<Collection<Object>> values =
        isDataRowOrdered() ? new ArrayList<>() : HashMultiset.create();
    dataRows.stream().map(Row::getValues).forEach(values::add);
    return values;
  }

  /**
   * Explain the difference between this and other DB result which is helpful for troubleshooting in
   * final test report.
   *
   * @param other other DB result
   * @return explain the difference
   */
  public String diff(DBResult other) {
    String result = diffSchema(other);
    if (result.isEmpty()) {
      return diffDataRows(other);
    }
    return result;
  }

  private String diffSchema(DBResult other) {
    List<Type> thisSchema = new ArrayList<>(schema);
    List<Type> otherSchema = new ArrayList<>(other.schema);
    return diff("Schema type", thisSchema, otherSchema);
  }

  private String diffDataRows(DBResult other) {
    if (isDataRowOrdered()) {
      return diff("Data row", (List<?>) dataRows, (List<?>) other.dataRows);
    }
    List<Row> thisRows = sort(dataRows);
    List<Row> otherRows = sort(other.dataRows);
    return diff("Data row", thisRows, otherRows);
  }

  /**
   * Check if two lists are same otherwise explain if size or any element is different at some
   * position.
   */
  private String diff(String name, List<?> thisList, List<?> otherList) {
    if (thisList.size() != otherList.size()) {
      return StringUtils.format(
          "%s size is different: this=[%d], other=[%d]", name, thisList.size(), otherList.size());
    }

    int diff = findFirstDifference(thisList, otherList);
    if (diff >= 0) {
      return StringUtils.format(
          "%s at [%d] is different: this=[%s], other=[%s]",
          name, diff, thisList.get(diff), otherList.get(diff));
    }
    return "";
  }

  /**
   * Find first different element with assumption that the lists given have same size and there is
   * no NULL element inside.
   */
  private static int findFirstDifference(List<?> list1, List<?> list2) {
    for (int i = 0; i < list1.size(); i++) {
      if (!list1.get(i).equals(list2.get(i))) {
        return i;
      }
    }
    return -1;
  }

  /**
   * Is data row a list that represent original order of data set which doesn't/shouldn't sort
   * again.
   */
  private boolean isDataRowOrdered() {
    return (dataRows instanceof List);
  }

  /** Convert a collection to list and sort and return this new list. */
  private static <T extends Comparable<T>> List<T> sort(Collection<T> collection) {
    ArrayList<T> list = new ArrayList<>(collection);
    Collections.sort(list);
    return list;
  }

  public boolean equals(final Object o) {
    if (o == this) {
      return true;
    }
    if (!(o instanceof DBResult)) {
      return false;
    }
    final DBResult other = (DBResult) o;
    // H2 calculates the value before setting column name
    // for example, for query "select 1 + 1" it returns a column named "2" instead of "1 + 1"
    boolean skipColumnNameCheck =
        databaseName.equalsIgnoreCase("h2") || other.databaseName.equalsIgnoreCase("h2");
    if (!skipColumnNameCheck && !schema.equals(other.schema)) {
      return false;
    }
    if (skipColumnNameCheck
        && !schema.stream()
            .map(Type::getType)
            .collect(Collectors.toList())
            .equals(other.schema.stream().map(Type::getType).collect(Collectors.toList()))) {
      return false;
    }
    return dataRows.equals(other.dataRows);
  }
}
