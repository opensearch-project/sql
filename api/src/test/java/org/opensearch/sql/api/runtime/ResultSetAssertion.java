/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.runtime;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

/**
 * Mixin interface providing fluent assertion API for JDBC ResultSet verification using Hamcrest
 * matchers. Tests can implement this interface to gain access to verification methods for schema
 * and data.
 */
public interface ResultSetAssertion {

  /** Creates ResultSetVerifier from a JDBC ResultSet */
  default ResultSetVerifier verify(ResultSet resultSet) throws SQLException {
    return new ResultSetVerifier(resultSet);
  }

  /** Creates column matcher for schema verification */
  default Matcher<String> col(String name) {
    return new TypeSafeMatcher<String>() {
      @Override
      protected boolean matchesSafely(String actualName) {
        return name.equals(actualName);
      }

      @Override
      public void describeTo(Description description) {
        description.appendText("column with name: " + name);
      }
    };
  }

  /** Creates column matcher for schema verification with type */
  default Matcher<ColumnInfo> col(String name, int sqlType) {
    return new TypeSafeMatcher<ColumnInfo>() {
      @Override
      protected boolean matchesSafely(ColumnInfo column) {
        return name.equals(column.name) && sqlType == column.sqlType;
      }

      @Override
      public void describeTo(Description description) {
        description.appendText("column with name: " + name + " and type: " + sqlType);
      }
    };
  }

  /** Creates row matcher for data verification */
  default Matcher<Object[]> row(Object... expectedValues) {
    return new TypeSafeMatcher<Object[]>() {
      @Override
      protected boolean matchesSafely(Object[] actualValues) {
        return Arrays.equals(expectedValues, actualValues);
      }

      @Override
      public void describeTo(Description description) {
        description.appendText("row with values: " + Arrays.toString(expectedValues));
      }
    };
  }

  /** Column information holder */
  class ColumnInfo {
    final String name;
    final int sqlType;

    ColumnInfo(String name, int sqlType) {
      this.name = name;
      this.sqlType = sqlType;
    }
  }

  /** Fluent assertion helper for JDBC ResultSet */
  class ResultSetVerifier {
    final ResultSet resultSet;
    final ResultSetMetaData metaData;

    ResultSetVerifier(ResultSet resultSet) throws SQLException {
      this.resultSet = resultSet;
      this.metaData = resultSet.getMetaData();
    }

    @SafeVarargs
    public final ResultSetVerifier expectSchema(Matcher<String>... matchers) throws SQLException {
      int columnCount = metaData.getColumnCount();
      assertEquals("Column count mismatch", matchers.length, columnCount);

      for (int i = 0; i < matchers.length; i++) {
        int columnIndex = i + 1; // JDBC is 1-based
        String columnName = metaData.getColumnName(columnIndex);
        assertThat("Column " + columnIndex + " mismatch", columnName, matchers[i]);
      }
      return this;
    }

    @SafeVarargs
    public final ResultSetVerifier expectData(Matcher<Object[]>... matchers) throws SQLException {
      List<Object[]> actualRows = new ArrayList<>();

      // Read all rows from ResultSet
      int columnCount = metaData.getColumnCount();
      while (resultSet.next()) {
        Object[] rowValues = new Object[columnCount];
        for (int i = 0; i < columnCount; i++) {
          rowValues[i] = resultSet.getObject(i + 1);
        }
        actualRows.add(rowValues);
      }

      assertEquals("Row count mismatch", matchers.length, actualRows.size());

      // Verify each row with its matcher
      for (int i = 0; i < matchers.length; i++) {
        assertThat("Row " + i + " mismatch", actualRows.get(i), matchers[i]);
      }

      return this;
    }
  }
}
