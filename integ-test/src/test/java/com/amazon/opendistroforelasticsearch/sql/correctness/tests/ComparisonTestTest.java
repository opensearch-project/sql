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

package com.amazon.opendistroforelasticsearch.sql.correctness.tests;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.amazon.opendistroforelasticsearch.sql.correctness.report.ErrorTestCase;
import com.amazon.opendistroforelasticsearch.sql.correctness.report.FailedTestCase;
import com.amazon.opendistroforelasticsearch.sql.correctness.report.SuccessTestCase;
import com.amazon.opendistroforelasticsearch.sql.correctness.report.TestReport;
import com.amazon.opendistroforelasticsearch.sql.correctness.runner.ComparisonTest;
import com.amazon.opendistroforelasticsearch.sql.correctness.runner.connection.DBConnection;
import com.amazon.opendistroforelasticsearch.sql.correctness.runner.resultset.DBResult;
import com.amazon.opendistroforelasticsearch.sql.correctness.runner.resultset.Row;
import com.amazon.opendistroforelasticsearch.sql.correctness.runner.resultset.Type;
import com.amazon.opendistroforelasticsearch.sql.correctness.testset.TestQuerySet;
import java.util.Collections;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

/**
 * Tests for {@link ComparisonTest}
 */
@RunWith(MockitoJUnitRunner.class)
public class ComparisonTestTest {

  @Mock
  private DBConnection openSearchConnection;

  @Mock
  private DBConnection otherDbConnection;

  private ComparisonTest correctnessTest;

  @Before
  public void setUp() {
    when(openSearchConnection.getDatabaseName()).thenReturn("OpenSearch");
    when(otherDbConnection.getDatabaseName()).thenReturn("Other");
    correctnessTest = new ComparisonTest(
        openSearchConnection, new DBConnection[] {otherDbConnection}
    );
  }

  @Test
  public void testSuccess() {
    when(openSearchConnection.select(anyString())).thenReturn(
        new DBResult("OpenSearch", asList(new Type("firstname", "text")), asList(new Row(asList("John"))))
    );
    when(otherDbConnection.select(anyString())).thenReturn(
        new DBResult("Other DB", asList(new Type("firstname", "text")),
            asList(new Row(asList("John"))))
    );

    TestReport expected = new TestReport();
    expected.addTestCase(new SuccessTestCase(1, "SELECT * FROM accounts"));
    TestReport actual = correctnessTest.verify(querySet("SELECT * FROM accounts"));
    assertEquals(expected, actual);
  }

  @Test
  public void testFailureDueToInconsistency() {
    DBResult openSearchResult =
        new DBResult("OpenSearch", asList(new Type("firstname", "text")), asList(new Row(asList("John"))));
    DBResult otherDbResult = new DBResult("Other DB", asList(new Type("firstname", "text")),
        asList(new Row(asList("JOHN"))));
    when(openSearchConnection.select(anyString())).thenReturn(openSearchResult);
    when(otherDbConnection.select(anyString())).thenReturn(otherDbResult);

    TestReport expected = new TestReport();
    expected.addTestCase(
        new FailedTestCase(1, "SELECT * FROM accounts", asList(openSearchResult, otherDbResult), ""));
    TestReport actual = correctnessTest.verify(querySet("SELECT * FROM accounts"));
    assertEquals(expected, actual);
  }

  @Test
  public void testSuccessFinally() {
    DBConnection anotherDbConnection = mock(DBConnection.class);
    when(anotherDbConnection.getDatabaseName()).thenReturn("Another");
    correctnessTest = new ComparisonTest(
        openSearchConnection, new DBConnection[] {otherDbConnection, anotherDbConnection}
    );

    DBResult openSearchResult =
        new DBResult("OpenSearch", asList(new Type("firstname", "text")), asList(new Row(asList("John"))));
    DBResult otherDbResult = new DBResult("Other DB", asList(new Type("firstname", "text")),
        asList(new Row(asList("JOHN"))));
    DBResult anotherDbResult = new DBResult("Another DB", asList(new Type("firstname", "text")),
        asList(new Row(asList("John"))));
    when(openSearchConnection.select(anyString())).thenReturn(openSearchResult);
    when(otherDbConnection.select(anyString())).thenReturn(otherDbResult);
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
    correctnessTest = new ComparisonTest(
        openSearchConnection, new DBConnection[] {otherDbConnection, anotherDbConnection}
    );

    DBResult openSearchResult =
        new DBResult("OpenSearch", asList(new Type("firstname", "text")), asList(new Row(asList("John"))));
    DBResult otherDbResult = new DBResult("Other DB", asList(new Type("firstname", "text")),
        asList(new Row(asList("JOHN"))));
    DBResult anotherDbResult = new DBResult("ZZZ DB", asList(new Type("firstname", "text")),
        asList(new Row(asList("Hank"))));
    when(openSearchConnection.select(anyString())).thenReturn(openSearchResult);
    when(otherDbConnection.select(anyString())).thenReturn(otherDbResult);
    when(anotherDbConnection.select(anyString())).thenReturn(anotherDbResult);

    TestReport expected = new TestReport();
    expected.addTestCase(new FailedTestCase(1, "SELECT * FROM accounts",
        asList(openSearchResult, otherDbResult, anotherDbResult), ""));
    TestReport actual = correctnessTest.verify(querySet("SELECT * FROM accounts"));
    assertEquals(expected, actual);
  }

  @Test
  public void testErrorDueToESException() {
    when(openSearchConnection.select(anyString())).thenThrow(new RuntimeException("All shards failure"));

    TestReport expected = new TestReport();
    expected.addTestCase(
        new ErrorTestCase(1, "SELECT * FROM accounts", "RuntimeException: All shards failure"));
    TestReport actual = correctnessTest.verify(querySet("SELECT * FROM accounts"));
    assertEquals(expected, actual);
  }

  @Test
  public void testErrorDueToNoOtherDBSupportThisQuery() {
    when(openSearchConnection.select(anyString())).thenReturn(
        new DBResult("OpenSearch", asList(new Type("firstname", "text")), asList(new Row(asList("John"))))
    );
    when(otherDbConnection.select(anyString()))
        .thenThrow(new RuntimeException("Unsupported feature"));

    TestReport expected = new TestReport();
    expected.addTestCase(new ErrorTestCase(1, "SELECT * FROM accounts",
        "No other databases support this query: Unsupported feature;"));
    TestReport actual = correctnessTest.verify(querySet("SELECT * FROM accounts"));
    assertEquals(expected, actual);
  }

  @Test
  public void testSuccessWhenOneDBSupportThisQuery() {
    DBConnection anotherDbConnection = mock(DBConnection.class);
    when(anotherDbConnection.getDatabaseName()).thenReturn("Another");
    correctnessTest = new ComparisonTest(
        openSearchConnection, new DBConnection[] {otherDbConnection, anotherDbConnection}
    );

    when(openSearchConnection.select(anyString())).thenReturn(
        new DBResult("OpenSearch", asList(new Type("firstname", "text")), asList(new Row(asList("John"))))
    );
    when(otherDbConnection.select(anyString()))
        .thenThrow(new RuntimeException("Unsupported feature"));
    when(anotherDbConnection.select(anyString())).thenReturn(
        new DBResult("Another DB", asList(new Type("firstname", "text")),
            asList(new Row(asList("John"))))
    );

    TestReport expected = new TestReport();
    expected.addTestCase(new SuccessTestCase(1, "SELECT * FROM accounts"));
    TestReport actual = correctnessTest.verify(querySet("SELECT * FROM accounts"));
    assertEquals(expected, actual);
  }

  @Test
  public void testFailureDueToInconsistencyAndExceptionMixed() {
    DBConnection otherDBConnection2 = mock(DBConnection.class);
    when(otherDBConnection2.getDatabaseName()).thenReturn("ZZZ DB");
    correctnessTest = new ComparisonTest(
        openSearchConnection, new DBConnection[] {otherDbConnection, otherDBConnection2}
    );

    DBResult openSearchResult =
        new DBResult("OpenSearch", asList(new Type("firstname", "text")), asList(new Row(asList("John"))));
    DBResult otherResult =
        new DBResult("Other", asList(new Type("firstname", "text")), Collections.emptyList());

    when(openSearchConnection.select(anyString())).thenReturn(openSearchResult);
    when(otherDbConnection.select(anyString())).thenReturn(otherResult);
    when(otherDBConnection2.select(anyString()))
        .thenThrow(new RuntimeException("Unsupported feature"));

    TestReport expected = new TestReport();
    expected.addTestCase(new FailedTestCase(1, "SELECT * FROM accounts",
        asList(openSearchResult, otherResult), "Unsupported feature;"));
    TestReport actual = correctnessTest.verify(querySet("SELECT * FROM accounts"));
    assertEquals(expected, actual);
  }

  private TestQuerySet querySet(String query) {
    return new TestQuerySet(new String[] {query});
  }

}
