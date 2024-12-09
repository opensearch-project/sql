/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.correctness.tests;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.opensearch.sql.correctness.report.ErrorTestCase;
import org.opensearch.sql.correctness.report.FailedTestCase;
import org.opensearch.sql.correctness.report.SuccessTestCase;
import org.opensearch.sql.correctness.report.TestReport;
import org.opensearch.sql.correctness.runner.ComparisonTest;
import org.opensearch.sql.correctness.runner.connection.DBConnection;
import org.opensearch.sql.correctness.runner.resultset.DBResult;
import org.opensearch.sql.correctness.runner.resultset.Row;
import org.opensearch.sql.correctness.runner.resultset.Type;
import org.opensearch.sql.correctness.testset.TestQuerySet;

/** Tests for {@link ComparisonTest} */
@RunWith(MockitoJUnitRunner.class)
public class ComparisonTestTest {

  @Mock private DBConnection openSearchConnection;

  @Mock private DBConnection otherDbConnection;

  private ComparisonTest correctnessTest;

  @Before
  public void setUp() {
    when(otherDbConnection.getDatabaseName()).thenReturn("Other");
    correctnessTest =
        new ComparisonTest(openSearchConnection, new DBConnection[] {otherDbConnection});
  }

  @Test
  public void testSuccess() {
    when(openSearchConnection.select(anyString()))
        .thenReturn(
            new DBResult(
                "OpenSearch",
                List.of(new Type("firstname", "text")),
                List.of(new Row(List.of("John")))));
    when(otherDbConnection.select(anyString()))
        .thenReturn(
            new DBResult(
                "Other DB",
                List.of(new Type("firstname", "text")),
                List.of(new Row(List.of("John")))));

    TestReport expected = new TestReport();
    expected.addTestCase(new SuccessTestCase(1, "SELECT * FROM accounts"));
    TestReport actual = correctnessTest.verify(querySet("SELECT * FROM accounts"));
    assertEquals(expected, actual);
  }

  @Test
  public void testFailureDueToInconsistency() {
    DBResult openSearchResult =
        new DBResult(
            "OpenSearch",
            List.of(new Type("firstname", "text")),
            List.of(new Row(List.of("John"))));
    DBResult otherDbResult =
        new DBResult(
            "Other DB", List.of(new Type("firstname", "text")), List.of(new Row(List.of("JOHN"))));
    when(openSearchConnection.select(anyString())).thenReturn(openSearchResult);
    when(otherDbConnection.select(anyString())).thenReturn(otherDbResult);

    TestReport expected = new TestReport();
    expected.addTestCase(
        new FailedTestCase(
            1, "SELECT * FROM accounts", List.of(openSearchResult, otherDbResult), ""));
    TestReport actual = correctnessTest.verify(querySet("SELECT * FROM accounts"));
    assertEquals(expected, actual);
  }

  @Test
  public void testSuccessFinally() {
    DBConnection anotherDbConnection = mock(DBConnection.class);
    when(anotherDbConnection.getDatabaseName()).thenReturn("Another");
    correctnessTest =
        new ComparisonTest(
            openSearchConnection, new DBConnection[] {otherDbConnection, anotherDbConnection});

    DBResult openSearchResult =
        new DBResult(
            "OpenSearch",
            List.of(new Type("firstname", "text")),
            List.of(new Row(List.of("John"))));
    DBResult otherDbResult =
        new DBResult(
            "Other DB", List.of(new Type("firstname", "text")), List.of(new Row(List.of("JOHN"))));
    DBResult anotherDbResult =
        new DBResult(
            "Another DB",
            List.of(new Type("firstname", "text")),
            List.of(new Row(List.of("John"))));
    when(openSearchConnection.select(anyString())).thenReturn(openSearchResult);
    when(anotherDbConnection.select(anyString())).thenReturn(anotherDbResult);

    TestReport expected = new TestReport();
    expected.addTestCase(new SuccessTestCase(1, "SELECT * FROM accounts"));
    TestReport actual = correctnessTest.verify(querySet("SELECT * FROM accounts"));
    assertEquals(expected, actual);
  }

  @Test
  public void testFailureDueToEventualInconsistency() {
    DBConnection anotherDbConnection = mock(DBConnection.class);
    when(anotherDbConnection.getDatabaseName())
        .thenReturn("ZZZ DB"); // Make sure this will be called after Other DB
    correctnessTest =
        new ComparisonTest(
            openSearchConnection, new DBConnection[] {otherDbConnection, anotherDbConnection});

    DBResult openSearchResult =
        new DBResult(
            "OpenSearch",
            List.of(new Type("firstname", "text")),
            List.of(new Row(List.of("John"))));
    DBResult otherDbResult =
        new DBResult(
            "Other DB", List.of(new Type("firstname", "text")), List.of(new Row(List.of("JOHN"))));
    DBResult anotherDbResult =
        new DBResult(
            "ZZZ DB", List.of(new Type("firstname", "text")), List.of(new Row(List.of("Hank"))));
    when(openSearchConnection.select(anyString())).thenReturn(openSearchResult);
    when(otherDbConnection.select(anyString())).thenReturn(otherDbResult);
    when(anotherDbConnection.select(anyString())).thenReturn(anotherDbResult);

    TestReport expected = new TestReport();
    expected.addTestCase(
        new FailedTestCase(
            1,
            "SELECT * FROM accounts",
            List.of(openSearchResult, otherDbResult, anotherDbResult),
            ""));
    TestReport actual = correctnessTest.verify(querySet("SELECT * FROM accounts"));
    assertEquals(expected, actual);
  }

  @Test
  public void testErrorDueToESException() {
    when(openSearchConnection.select(anyString()))
        .thenThrow(new RuntimeException("All shards failure"));

    TestReport expected = new TestReport();
    expected.addTestCase(
        new ErrorTestCase(1, "SELECT * FROM accounts", "RuntimeException: All shards failure"));
    TestReport actual = correctnessTest.verify(querySet("SELECT * FROM accounts"));
    assertEquals(expected, actual);
  }

  @Test
  public void testErrorDueToNoOtherDBSupportThisQuery() {
    when(openSearchConnection.select(anyString()))
        .thenReturn(
            new DBResult(
                "OpenSearch",
                List.of(new Type("firstname", "text")),
                List.of(new Row(List.of("John")))));
    when(otherDbConnection.select(anyString()))
        .thenThrow(new RuntimeException("Unsupported feature"));

    TestReport expected = new TestReport();
    expected.addTestCase(
        new ErrorTestCase(
            1,
            "SELECT * FROM accounts",
            "No other databases support this query: Unsupported feature;"));
    TestReport actual = correctnessTest.verify(querySet("SELECT * FROM accounts"));
    assertEquals(expected, actual);
  }

  @Test
  public void testSuccessWhenOneDBSupportThisQuery() {
    DBConnection anotherDbConnection = mock(DBConnection.class);
    when(anotherDbConnection.getDatabaseName()).thenReturn("Another");
    correctnessTest =
        new ComparisonTest(
            openSearchConnection, new DBConnection[] {otherDbConnection, anotherDbConnection});

    when(openSearchConnection.select(anyString()))
        .thenReturn(
            new DBResult(
                "OpenSearch",
                List.of(new Type("firstname", "text")),
                List.of(new Row(List.of("John")))));
    when(anotherDbConnection.select(anyString()))
        .thenReturn(
            new DBResult(
                "Another DB",
                List.of(new Type("firstname", "text")),
                List.of(new Row(List.of("John")))));

    TestReport expected = new TestReport();
    expected.addTestCase(new SuccessTestCase(1, "SELECT * FROM accounts"));
    TestReport actual = correctnessTest.verify(querySet("SELECT * FROM accounts"));
    assertEquals(expected, actual);
  }

  @Test
  public void testFailureDueToInconsistencyAndExceptionMixed() {
    DBConnection otherDBConnection2 = mock(DBConnection.class);
    when(otherDBConnection2.getDatabaseName()).thenReturn("ZZZ DB");
    correctnessTest =
        new ComparisonTest(
            openSearchConnection, new DBConnection[] {otherDbConnection, otherDBConnection2});

    DBResult openSearchResult =
        new DBResult(
            "OpenSearch",
            List.of(new Type("firstname", "text")),
            List.of(new Row(List.of("John"))));
    DBResult otherResult = new DBResult("Other", List.of(new Type("firstname", "text")), List.of());

    when(openSearchConnection.select(anyString())).thenReturn(openSearchResult);
    when(otherDbConnection.select(anyString())).thenReturn(otherResult);
    when(otherDBConnection2.select(anyString()))
        .thenThrow(new RuntimeException("Unsupported feature"));

    TestReport expected = new TestReport();
    expected.addTestCase(
        new FailedTestCase(
            1,
            "SELECT * FROM accounts",
            List.of(openSearchResult, otherResult),
            "Unsupported feature;"));
    TestReport actual = correctnessTest.verify(querySet("SELECT * FROM accounts"));
    assertEquals(expected, actual);
  }

  private TestQuerySet querySet(String query) {
    return new TestQuerySet(new String[] {query});
  }
}
