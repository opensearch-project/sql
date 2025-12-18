/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.Value;
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
  default ResultSetVerifier verify(ResultSet resultSet) {
    return new ResultSetVerifier(resultSet);
  }

  /** Creates column matcher for schema verification with type */
  default Matcher<ColumnInfo> col(String name, int sqlType) {
    return new TypeSafeMatcher<>() {
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
    return new TypeSafeMatcher<>() {
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
  @Value
  class ColumnInfo {
    String name;
    int sqlType;
  }

  /** Fluent assertion helper for JDBC ResultSet */
  @RequiredArgsConstructor
  class ResultSetVerifier {
    final ResultSet resultSet;

    @SafeVarargs
    public final ResultSetVerifier expectSchema(Matcher<ColumnInfo>... matchers)
        throws SQLException {
      ResultSetMetaData metaData = resultSet.getMetaData();
      assertEquals("Column count mismatch", matchers.length, metaData.getColumnCount());

      for (int i = 0; i < matchers.length; i++) {
        int columnIndex = i + 1; // JDBC is 1-based
        ColumnInfo columnInfo =
            new ColumnInfo(
                metaData.getColumnName(columnIndex), metaData.getColumnType(columnIndex));
        assertThat("Column " + columnIndex + " mismatch", columnInfo, matchers[i]);
      }
      return this;
    }

    @SafeVarargs
    public final ResultSetVerifier expectData(Matcher<Object[]>... matchers) throws SQLException {
      List<Object[]> actualRows = readAllRows();
      assertEquals("Row count mismatch", matchers.length, actualRows.size());

      for (int i = 0; i < matchers.length; i++) {
        assertThat("Row " + i + " mismatch", actualRows.get(i), matchers[i]);
      }
      return this;
    }

    private List<Object[]> readAllRows() throws SQLException {
      List<Object[]> rows = new ArrayList<>();
      int columnCount = resultSet.getMetaData().getColumnCount();

      while (resultSet.next()) {
        Object[] rowValues = new Object[columnCount];
        for (int i = 0; i < columnCount; i++) {
          rowValues[i] = resultSet.getObject(i + 1);
        }
        rows.add(rowValues);
      }
      return rows;
    }
  }
}
