/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.core.StringStartsWith.startsWith;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ACCOUNT;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK_CSV_SANITIZE;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_DOG;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_GAME_OF_THRONES;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_NESTED_TYPE;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_NESTED_WITH_QUOTES;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ONLINE;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import org.hamcrest.Matcher;
import org.hamcrest.core.AnyOf;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.sql.legacy.executor.csv.CSVResult;

/** Tests to cover requests with "?format=csv" parameter */
public class CsvFormatResponseIT extends SQLIntegTestCase {

  private boolean flatOption = false;

  @Override
  protected void init() throws Exception {
    loadIndex(Index.ACCOUNT);
    loadIndex(Index.NESTED);
    loadIndex(Index.NESTED_WITH_QUOTES);
    loadIndex(Index.DOG);
    loadIndex(Index.GAME_OF_THRONES);
    loadIndex(Index.ONLINE);
    loadIndex(Index.BANK_CSV_SANITIZE);
  }

  @Override
  protected Request getSqlRequest(String request, boolean explain) {

    Request sqlRequest = super.getSqlRequest(request, explain);
    sqlRequest.addParameter("format", "csv");
    sqlRequest.addParameter("flat", flatOption ? "true" : "false");
    return sqlRequest;
  }

  @Test
  public void allPercentilesByDefault() throws IOException {

    final String query =
        String.format(Locale.ROOT, "SELECT PERCENTILES(age) FROM %s", TEST_INDEX_ACCOUNT);
    final String result = executeQueryWithStringOutput(query);

    final String expectedHeaders =
        "PERCENTILES(age).1.0,PERCENTILES(age).5.0,PERCENTILES(age).25.0,"
            + "PERCENTILES(age).50.0,PERCENTILES(age).75.0,PERCENTILES(age).95.0,PERCENTILES(age).99.0";
    Assert.assertThat(result, containsString(expectedHeaders));
  }

  @Test
  public void specificPercentilesIntAndDouble() throws IOException {

    final String query =
        String.format(Locale.ROOT, "SELECT PERCENTILES(age,10,49.0) FROM %s", TEST_INDEX_ACCOUNT);
    final String result = executeQueryWithStringOutput(query);

    final String[] unexpectedPercentiles = {"1.0", "5.0", "25.0", "50.0", "75.0", "95.0", "99.0"};
    final String expectedHeaders =
        "\"PERCENTILES(age,10,49.0).10.0\",\"PERCENTILES(age,10,49.0).49.0\"";
    Assert.assertThat(result, containsString(expectedHeaders));
    for (final String unexpectedPercentile : unexpectedPercentiles) {
      Assert.assertThat(
          result, not(containsString("PERCENTILES(age,10,49.0)." + unexpectedPercentile)));
    }
  }

  public void nestedObjectsAndArraysAreQuoted() throws IOException {
    final String query =
        String.format(Locale.ROOT, "SELECT * FROM %s WHERE _id = 5", TEST_INDEX_NESTED_TYPE);
    final String result = executeQueryWithStringOutput(query);

    final String expectedMyNum = "\"[3, 4]\"";
    final String expectedComment = "\"{data=[aa, bb], likes=10}\"";
    final String expectedMessage = "\"[{dayOfWeek=6, author=zz, info=zz}]\"";

    Assert.assertThat(result, containsString(expectedMyNum));
    Assert.assertThat(result, containsString(expectedComment));
    Assert.assertThat(result, containsString(expectedMessage));
  }

  public void arraysAreQuotedInFlatMode() throws IOException {
    setFlatOption(true);

    final String query =
        String.format(Locale.ROOT, "SELECT * FROM %s WHERE _id = 5", TEST_INDEX_NESTED_TYPE);
    final String result = executeQueryWithStringOutput(query);

    final String expectedMyNum = "\"[3, 4]\"";
    final String expectedCommentData = "\"[aa, bb]\"";
    final String expectedMessage = "\"[{dayOfWeek=6, author=zz, info=zz}]\"";

    Assert.assertThat(result, containsString(expectedMyNum));
    Assert.assertThat(result, containsString(expectedCommentData));
    Assert.assertThat(result, containsString(expectedMessage));

    setFlatOption(false);
  }

  @Test
  public void doubleQuotesAreEscapedWithDoubleQuotes() throws IOException {
    final String query = "SELECT * FROM " + TEST_INDEX_NESTED_WITH_QUOTES;

    final CSVResult csvResult = executeCsvRequest(query, false);
    final List<String> rows = csvResult.getLines();
    Assert.assertThat(rows.size(), equalTo(2));

    final String expectedValue1 = "\"[{dayOfWeek=6, author=z\"\"z, info=zz}]\"";
    final String expectedValue2 =
        "\"[{dayOfWeek=3, author=this \"\"value\"\" contains quotes, info=rr}]\"";

    for (String row : rows) {
      Assert.assertThat(row, anyOf(containsString(expectedValue1), containsString(expectedValue2)));
    }
  }

  @Test
  public void fieldOrder() throws IOException {

    final String[] expectedFields = {"age", "firstname", "address", "gender", "email"};

    verifyFieldOrder(expectedFields);
  }

  @Test
  public void fieldOrderOther() throws IOException {

    final String[] expectedFields = {"email", "firstname", "age", "gender", "address"};

    verifyFieldOrder(expectedFields);
  }

  @Ignore("Getting parser error")
  public void fieldOrderWithScriptFields() throws IOException {

    final String[] expectedFields = {"email", "script1", "script2", "gender", "address"};
    final String query =
        String.format(
            Locale.ROOT,
            "SELECT email, "
                + "script(script1, \"doc['balance'].value * 2\"), "
                + "script(script2, painless, \"doc['balance'].value + 10\"), gender, address "
                + "FROM %s WHERE email='amberduke@pyrami.com'",
            TEST_INDEX_ACCOUNT);

    verifyFieldOrder(expectedFields, query);
  }

  // region Tests migrated from CSVResultsExtractorTests

  @Test
  public void simpleSearchResultNotNestedNotFlatNoAggs() throws Exception {
    String query =
        String.format(Locale.ROOT, "select dog_name,age from %s order by age", TEST_INDEX_DOG);
    final CSVResult csvResult = executeCsvRequest(query, false);

    List<String> headers = csvResult.getHeaders();
    Assert.assertEquals(2, headers.size());
    Assert.assertTrue("name should be on headers", headers.contains("dog_name"));
    Assert.assertTrue("age should be on headers", headers.contains("age"));

    List<String> lines = csvResult.getLines();
    Assert.assertEquals(2, lines.size());
    Assert.assertTrue("rex,2".equals(lines.get(0)) || "2,rex".equals(lines.get(0)));
    Assert.assertTrue("snoopy,4".equals(lines.get(1)) || "4,snoopy".equals(lines.get(1)));
  }

  @Test
  public void simpleSearchResultWithNestedNotFlatNoAggs() throws Exception {
    String query =
        String.format(Locale.ROOT, "select name,house from %s", TEST_INDEX_GAME_OF_THRONES);
    CSVResult csvResult = executeCsvRequest(query, false);

    List<String> headers = csvResult.getHeaders();
    Assert.assertEquals(2, headers.size());
    Assert.assertTrue("name should be on headers", headers.contains("name"));
    Assert.assertTrue("house should be on headers", headers.contains("house"));

    List<String> lines = csvResult.getLines();
    Assert.assertEquals(7, lines.size());

    Assert.assertThat(
        lines,
        hasRow(
            null,
            "Targaryen",
            Arrays.asList("firstname=Daenerys", "lastname=Targaryen", "ofHerName=1"),
            true));
    Assert.assertThat(
        lines,
        hasRow(
            null,
            "Stark",
            Arrays.asList("firstname=Eddard", "lastname=Stark", "ofHisName=1"),
            true));
    Assert.assertThat(
        lines,
        hasRow(
            null,
            "Stark",
            Arrays.asList("firstname=Brandon", "lastname=Stark", "ofHisName=4"),
            true));
    Assert.assertThat(
        lines,
        hasRow(
            null,
            "Lannister",
            Arrays.asList("firstname=Jaime", "lastname=Lannister", "ofHisName=1"),
            true));
  }

  @Ignore("headers incorrect in case of nested fields")
  @Test
  public void simpleSearchResultWithNestedOneFieldNotFlatNoAggs() throws Exception {
    String query =
        String.format(
            Locale.ROOT, "select name.firstname,house from %s", TEST_INDEX_GAME_OF_THRONES);
    CSVResult csvResult = executeCsvRequest(query, false);

    List<String> headers = csvResult.getHeaders();
    Assert.assertEquals(2, headers.size());
    Assert.assertTrue("name should be on headers", headers.contains("name"));
    Assert.assertTrue("house should be on headers", headers.contains("house"));

    List<String> lines = csvResult.getLines();
    Assert.assertEquals(7, lines.size());
    Assert.assertThat(lines, hasItem("{firstname=Daenerys},Targaryen"));
    Assert.assertThat(lines, hasItem("{firstname=Eddard},Stark"));
    Assert.assertThat(lines, hasItem("{firstname=Brandon},Stark"));
    Assert.assertThat(lines, hasItem("{firstname=Jaime},Lannister"));
  }

  @Ignore("headers incorrect in case of nested fields")
  @Test
  public void simpleSearchResultWithNestedTwoFieldsFromSameNestedNotFlatNoAggs() throws Exception {
    String query =
        String.format(
            Locale.ROOT,
            "select name.firstname,name.lastname,house from %s",
            TEST_INDEX_GAME_OF_THRONES);
    CSVResult csvResult = executeCsvRequest(query, false);

    List<String> headers = csvResult.getHeaders();
    Assert.assertEquals(2, headers.size());
    Assert.assertTrue("name should be on headers", headers.contains("name"));
    Assert.assertTrue("house should be on headers", headers.contains("house"));

    List<String> lines = csvResult.getLines();
    Assert.assertEquals(7, lines.size());

    Assert.assertThat(
        lines,
        hasRow(null, "Targaryen", Arrays.asList("firstname=Daenerys", "lastname=Targaryen"), true));
    Assert.assertThat(
        lines, hasRow(null, "Stark", Arrays.asList("firstname=Eddard", "lastname=Stark"), true));
    Assert.assertThat(
        lines, hasRow(null, "Stark", Arrays.asList("firstname=Brandon", "lastname=Stark"), true));
    Assert.assertThat(
        lines,
        hasRow(null, "Lannister", Arrays.asList("firstname=Jaime", "lastname=Lannister"), true));
  }

  @Test
  public void simpleSearchResultWithNestedWithFlatNoAggs() throws Exception {
    String query =
        String.format(
            Locale.ROOT, "select name.firstname,house from %s", TEST_INDEX_GAME_OF_THRONES);
    CSVResult csvResult = executeCsvRequest(query, true);

    List<String> headers = csvResult.getHeaders();
    Assert.assertEquals(2, headers.size());
    Assert.assertTrue("name.firstname should be on headers", headers.contains("name.firstname"));
    Assert.assertTrue("house should be on headers", headers.contains("house"));

    List<String> lines = csvResult.getLines();
    Assert.assertEquals(7, lines.size());
    Assert.assertTrue(lines.contains("Daenerys,Targaryen"));
    Assert.assertTrue(lines.contains("Eddard,Stark"));
    Assert.assertTrue(lines.contains("Brandon,Stark"));
    Assert.assertTrue(lines.contains("Jaime,Lannister"));
  }

  @Test
  public void joinSearchResultNotNestedNotFlatNoAggs() throws Exception {
    String query =
        String.format(
            Locale.ROOT,
            "select c.gender , h.hname,h.words from %s c " + "JOIN %s h " + "on h.hname = c.house ",
            TEST_INDEX_GAME_OF_THRONES,
            TEST_INDEX_GAME_OF_THRONES);
    CSVResult csvResult = executeCsvRequest(query, false);

    List<String> headers = csvResult.getHeaders();
    Assert.assertEquals(3, headers.size());
    Assert.assertTrue("c.gender should be on headers", headers.contains("c.gender"));
    Assert.assertTrue("h.hname should be on headers", headers.contains("h.hname"));
    Assert.assertTrue("h.words should be on headers", headers.contains("h.words"));

    List<String> lines = csvResult.getLines();
    Assert.assertEquals(4, lines.size());

    Assert.assertThat(
        lines, hasRow(null, null, Arrays.asList("F", "fireAndBlood", "Targaryen"), false));
  }

  @Test
  public void simpleNumericValueAgg() throws Exception {
    String query = String.format(Locale.ROOT, "select count(*) from %s ", TEST_INDEX_DOG);
    CSVResult csvResult = executeCsvRequest(query, false);

    List<String> headers = csvResult.getHeaders();
    Assert.assertEquals(1, headers.size());
    Assert.assertEquals("count(*)", headers.get(0));

    List<String> lines = csvResult.getLines();
    Assert.assertEquals(1, lines.size());
    Assert.assertEquals("2", lines.get(0));
  }

  @Test
  public void simpleNumericValueAggWithAlias() throws Exception {
    String query =
        String.format(Locale.ROOT, "select avg(age) as myAlias from %s ", TEST_INDEX_DOG);
    CSVResult csvResult = executeCsvRequest(query, false);

    List<String> headers = csvResult.getHeaders();
    Assert.assertEquals(1, headers.size());
    Assert.assertEquals("myAlias", headers.get(0));

    List<String> lines = csvResult.getLines();
    Assert.assertEquals(1, lines.size());
    Assert.assertEquals("3.0", lines.get(0));
  }

  @Ignore("only work for legacy engine")
  public void twoNumericAggWithAlias() throws Exception {
    String query =
        String.format(
            Locale.ROOT, "select count(*) as count, avg(age) as myAlias from %s ", TEST_INDEX_DOG);
    CSVResult csvResult = executeCsvRequest(query, false);

    List<String> headers = csvResult.getHeaders();
    Assert.assertEquals(2, headers.size());

    Assert.assertTrue(headers.contains("count"));
    Assert.assertTrue(headers.contains("myAlias"));

    List<String> lines = csvResult.getLines();
    Assert.assertEquals(1, lines.size());
    Assert.assertEquals("2,3.0", lines.get(0));
  }

  @Test
  public void aggAfterTermsGroupBy() throws Exception {
    String query =
        String.format(Locale.ROOT, "SELECT COUNT(*) FROM %s GROUP BY gender", TEST_INDEX_ACCOUNT);
    CSVResult csvResult = executeCsvRequest(query, false);
    List<String> headers = csvResult.getHeaders();
    Assert.assertEquals(1, headers.size());
    assertThat(headers, contains(equalTo("COUNT(*)")));

    List<String> lines = csvResult.getLines();
    Assert.assertEquals(2, lines.size());
    assertThat(lines, containsInAnyOrder(equalTo("507"), equalTo("493")));
  }

  @Test
  public void aggAfterTwoTermsGroupBy() throws Exception {
    String query =
        String.format(
            Locale.ROOT,
            "SELECT COUNT(*) FROM %s where age in (35,36) GROUP BY gender,age",
            TEST_INDEX_ACCOUNT);
    CSVResult csvResult = executeCsvRequest(query, false);
    List<String> headers = csvResult.getHeaders();
    Assert.assertEquals(1, headers.size());
    assertThat(headers, contains(equalTo("COUNT(*)")));

    List<String> lines = csvResult.getLines();
    Assert.assertEquals(4, lines.size());
    assertThat(
        lines, containsInAnyOrder(equalTo("31"), equalTo("28"), equalTo("21"), equalTo("24")));
  }

  @Test
  public void multipleAggAfterTwoTermsGroupBy() throws Exception {
    String query =
        String.format(
            Locale.ROOT,
            "SELECT COUNT(*) , sum(balance) FROM %s where age in (35,36) GROUP BY gender,age",
            TEST_INDEX_ACCOUNT);
    CSVResult csvResult = executeCsvRequest(query, false);
    List<String> headers = csvResult.getHeaders();
    Assert.assertEquals(2, headers.size());
    assertThat(headers, contains(equalTo("COUNT(*)"), equalTo("sum(balance)")));

    List<String> lines = csvResult.getLines();
    Assert.assertEquals(4, lines.size());
    assertThat(
        lines,
        containsInAnyOrder(
            equalTo("31,647425"),
            equalTo("28,678337"),
            equalTo("21,505660"),
            equalTo("24,472771")));
  }

  @Test
  public void dateHistogramTest() throws Exception {
    String query =
        String.format(
            Locale.ROOT,
            "select count(*) from %s group by"
                + " date_histogram('field'='insert_time','fixed_interval'='4d','alias'='days')",
            TEST_INDEX_ONLINE);
    CSVResult csvResult = executeCsvRequest(query, false);
    List<String> headers = csvResult.getHeaders();
    Assert.assertEquals(1, headers.size());
    assertThat(headers, contains(equalTo("COUNT(*)")));

    List<String> lines = csvResult.getLines();
    Assert.assertEquals(3, lines.size());
    assertThat(lines, containsInAnyOrder(equalTo("477.0"), equalTo("5664.0"), equalTo("3795.0")));
  }

  @Test
  public void statsAggregationTest() throws Exception {
    String query = String.format(Locale.ROOT, "SELECT STATS(age) FROM %s", TEST_INDEX_ACCOUNT);
    CSVResult csvResult = executeCsvRequest(query, false);
    List<String> headers = csvResult.getHeaders();
    Assert.assertEquals(5, headers.size());
    Assert.assertEquals("STATS(age).count", headers.get(0));
    Assert.assertEquals("STATS(age).sum", headers.get(1));
    Assert.assertEquals("STATS(age).avg", headers.get(2));
    Assert.assertEquals("STATS(age).min", headers.get(3));
    Assert.assertEquals("STATS(age).max", headers.get(4));

    List<String> lines = csvResult.getLines();
    Assert.assertEquals(1, lines.size());
    Assert.assertEquals("1000,30171.0,30.171,20.0,40.0", lines.get(0));
  }

  @Test
  public void extendedStatsAggregationTest() throws Exception {
    String query =
        String.format(Locale.ROOT, "SELECT EXTENDED_STATS(age) FROM %s", TEST_INDEX_ACCOUNT);
    CSVResult csvResult = executeCsvRequest(query, false);
    List<String> headers = csvResult.getHeaders();

    final String[] expectedHeaders = {
      "EXTENDED_STATS(age).count",
      "EXTENDED_STATS(age).sum",
      "EXTENDED_STATS(age).avg",
      "EXTENDED_STATS(age).min",
      "EXTENDED_STATS(age).max",
      "EXTENDED_STATS(age).sumOfSquares",
      "EXTENDED_STATS(age).variance",
      "EXTENDED_STATS(age).stdDeviation"
    };

    Assert.assertEquals(expectedHeaders.length, headers.size());
    Assert.assertThat(headers, contains(expectedHeaders));

    List<String> lines = csvResult.getLines();
    Assert.assertEquals(1, lines.size());
    String line = lines.get(0);
    Assert.assertTrue(line.startsWith("1000,30171.0,30.171,20.0,40.0,946393.0"));
    Assert.assertTrue(line.contains(",6.008"));
    Assert.assertTrue(line.contains(",36.103"));
  }

  @Test
  public void percentileAggregationTest() throws Exception {
    String query =
        String.format(
            Locale.ROOT,
            "select percentiles(age) as per from %s where age > 31",
            TEST_INDEX_ACCOUNT);
    CSVResult csvResult = executeCsvRequest(query, false);
    List<String> headers = csvResult.getHeaders();
    Assert.assertEquals(7, headers.size());
    Assert.assertEquals("per.1.0", headers.get(0));
    Assert.assertEquals("per.5.0", headers.get(1));
    Assert.assertEquals("per.25.0", headers.get(2));
    Assert.assertEquals("per.50.0", headers.get(3));
    Assert.assertEquals("per.75.0", headers.get(4));
    Assert.assertEquals("per.95.0", headers.get(5));
    Assert.assertEquals("per.99.0", headers.get(6));

    List<String> lines = csvResult.getLines();
    Assert.assertEquals(1, lines.size());

    List<Double> result =
        Arrays.stream(lines.get(0).split(","))
            .mapToDouble(Double::valueOf)
            .boxed()
            .collect(Collectors.toList());
    assertEquals(7, result.size());
    assertEquals(32.0, result.get(0), 0.6);
    assertEquals(32.0, result.get(1), 0.6);
    assertEquals(34.0, result.get(2), 0.6);

    assertEquals("32.0,32.0,34.0,36.0,38.0,40.0,40.0", lines.get(0), 0.6);
  }

  private void assertEquals(String expected, String actual, Double delta) {
    List<Double> actualList =
        Arrays.stream(actual.split(","))
            .mapToDouble(Double::valueOf)
            .boxed()
            .collect(Collectors.toList());
    List<Double> expectedList =
        Arrays.stream(expected.split(","))
            .mapToDouble(Double::valueOf)
            .boxed()
            .collect(Collectors.toList());

    assertEquals(expectedList.size(), actualList.size());
    for (int i = 0; i < expectedList.size(); i++) {
      assertEquals(expectedList.get(i), actualList.get(i), delta);
    }
  }

  @Test
  public void includeScore() throws Exception {
    String query =
        String.format(
            Locale.ROOT,
            "select age, firstname, _score from %s where age > 31 order by _score desc limit 2 ",
            TEST_INDEX_ACCOUNT);
    CSVResult csvResult = executeCsvRequest(query, false, true, false);
    List<String> headers = csvResult.getHeaders();
    Assert.assertEquals(3, headers.size());
    Assert.assertTrue(headers.contains("age"));
    Assert.assertTrue(headers.contains("firstname"));
    Assert.assertTrue(headers.contains("_score"));
    List<String> lines = csvResult.getLines();
    Assert.assertTrue(lines.get(0).contains("1.0"));
    Assert.assertTrue(lines.get(1).contains("1.0"));
  }

  /* todo: more tests:
   * filter/nested and than metric
   * histogram
   * geo
   */

  @Test
  public void scriptedField() throws Exception {
    String query =
        String.format(
            Locale.ROOT,
            "select age+1 as agePlusOne ,age , firstname from %s where age =  31 limit 1",
            TEST_INDEX_ACCOUNT);
    CSVResult csvResult = executeCsvRequest(query, false);
    List<String> headers = csvResult.getHeaders();
    Assert.assertEquals(3, headers.size());
    Assert.assertTrue(headers.contains("agePlusOne"));
    Assert.assertTrue(headers.contains("age"));
    Assert.assertTrue(headers.contains("firstname"));
    List<String> lines = csvResult.getLines();
    Assert.assertTrue(
        lines.get(0).contains("32,31")
            || lines.get(0).contains("32.0,31.0")
            || lines.get(0).contains("31,32")
            || lines.get(0).contains("31.0,32.0"));
  }

  @Ignore("separator not exposed")
  @Test
  public void twoCharsSeperator() throws Exception {
    String query =
        String.format(Locale.ROOT, "select dog_name,age from %s order by age", TEST_INDEX_DOG);
    CSVResult csvResult = executeCsvRequest(query, false);

    List<String> headers = csvResult.getHeaders();
    Assert.assertEquals(2, headers.size());
    Assert.assertTrue("name should be on headers", headers.contains("dog_name"));
    Assert.assertTrue("age should be on headers", headers.contains("age"));

    List<String> lines = csvResult.getLines();
    Assert.assertEquals(2, lines.size());
    Assert.assertTrue("rex||2".equals(lines.get(0)) || "2||rex".equals(lines.get(0)));
    Assert.assertTrue("snoopy||4".equals(lines.get(1)) || "4||snoopy".equals(lines.get(1)));
  }

  @Ignore("tested in @see: org.opensearch.sql.sql.IdentifierIT.testMetafieldIdentifierTest")
  public void includeIdAndNotTypeOrScore() throws Exception {
    String query =
        String.format(
            Locale.ROOT,
            "select age, firstname, _id from %s where lastname = 'Marquez' ",
            TEST_INDEX_ACCOUNT);
    CSVResult csvResult = executeCsvRequest(query, false, false, true);
    List<String> headers = csvResult.getHeaders();
    Assert.assertEquals(3, headers.size());
    Assert.assertTrue(headers.contains("age"));
    Assert.assertTrue(headers.contains("firstname"));
    Assert.assertTrue(headers.contains("_id"));
    List<String> lines = csvResult.getLines();
    Assert.assertTrue(lines.get(0).contains(",437") || lines.get(0).contains("437,"));
  }

  // endregion Tests migrated from CSVResultsExtractorTests

  @Ignore("only work for legacy engine")
  public void sensitiveCharacterSanitizeTest() throws IOException {
    String requestBody =
        "{"
            + "  \"=cmd|' /C notepad'!_xlbgnm.A1\": \"+cmd|' /C notepad'!_xlbgnm.A1\",\n"
            + "  \"-cmd|' /C notepad'!_xlbgnm.A1\": \"@cmd|' /C notepad'!_xlbgnm.A1\"\n"
            + "}";

    Request request = new Request("PUT", "/userdata/_doc/1?refresh=true");
    request.setJsonEntity(requestBody);
    TestUtils.performRequest(client(), request);

    CSVResult csvResult = executeCsvRequest("SELECT * FROM userdata", false, false, false);
    List<String> headers = csvResult.getHeaders();
    Assert.assertEquals(2, headers.size());
    Assert.assertTrue(headers.contains("'=cmd|' /C notepad'!_xlbgnm.A1"));
    Assert.assertTrue(headers.contains("'-cmd|' /C notepad'!_xlbgnm.A1"));

    List<String> lines = csvResult.getLines();
    Assert.assertEquals(1, lines.size());
    Assert.assertTrue(lines.get(0).contains("'+cmd|' /C notepad'!_xlbgnm.A1"));
    Assert.assertTrue(lines.get(0).contains("'@cmd|' /C notepad'!_xlbgnm.A1"));
  }

  @Ignore("only work for legacy engine")
  public void sensitiveCharacterSanitizeAndQuotedTest() throws IOException {
    String requestBody =
        "{"
            + "  \"=cmd|' /C notepad'!_xlbgnm.A1,,\": \",+cmd|' /C notepad'!_xlbgnm.A1\",\n"
            + "  \",@cmd|' /C notepad'!_xlbgnm.A1\": \"+cmd|' /C notepad,,'!_xlbgnm.A1\",\n"
            + "  \"-cmd|' /C notepad,,'!_xlbgnm.A1\": \",,,@cmd|' /C notepad'!_xlbgnm.A1\"\n"
            + "}";

    Request request = new Request("PUT", "/userdata2/_doc/1?refresh=true");
    request.setJsonEntity(requestBody);
    TestUtils.performRequest(client(), request);

    CSVResult csvResult = executeCsvRequest("SELECT * FROM userdata2", false, false, false);
    String headers = String.join(",", csvResult.getHeaders());
    Assert.assertTrue(headers.contains("\"'=cmd|' /C notepad'!_xlbgnm.A1,,\""));
    Assert.assertTrue(headers.contains("\",@cmd|' /C notepad'!_xlbgnm.A1\""));
    Assert.assertTrue(headers.contains("\"'-cmd|' /C notepad,,'!_xlbgnm.A1\""));

    List<String> lines = csvResult.getLines();
    Assert.assertEquals(1, lines.size());
    Assert.assertTrue(lines.get(0).contains("\",+cmd|' /C notepad'!_xlbgnm.A1\""));
    Assert.assertTrue(lines.get(0).contains("\"'+cmd|' /C notepad,,'!_xlbgnm.A1\""));
    Assert.assertTrue(lines.get(0).contains("\",,,@cmd|' /C notepad'!_xlbgnm.A1\""));
  }

  @Test
  public void sanitizeTest() throws IOException {
    CSVResult csvResult =
        executeCsvRequest(
            String.format(
                Locale.ROOT, "SELECT firstname, lastname FROM %s", TEST_INDEX_BANK_CSV_SANITIZE),
            false);
    List<String> lines = csvResult.getLines();
    assertEquals(5, lines.size());
    assertEquals(lines.get(0), "'+Amber JOHnny,Duke Willmington+");
    assertEquals(lines.get(1), "'-Hattie,Bond-");
    assertEquals(lines.get(2), "'=Nanette,Bates=");
    assertEquals(lines.get(3), "'@Dale,Adams@");
    assertEquals(lines.get(4), "\",Elinor\",\"Ratliff,,,\"");
  }

  @Test
  public void selectFunctionAsFieldTest() throws IOException {
    String query = "select log(age) from " + TEST_INDEX_ACCOUNT;
    CSVResult result = executeCsvRequest(query, false, false, false);
    List<String> headers = result.getHeaders();
    Assert.assertEquals(1, headers.size());
  }

  @Test
  public void unionTest() throws IOException {
    String query =
        String.format(
            Locale.ROOT,
            "SELECT firstname, lastname FROM %s LIMIT 3 "
                + "UNION ALL SELECT firstname, lastname FROM %s LIMIT 3",
            TestsConstants.TEST_INDEX_ACCOUNT,
            TestsConstants.TEST_INDEX_ACCOUNT);
    CSVResult result = executeCsvRequest(query, false, false, false);
    List<String> headers = result.getHeaders();
    Assert.assertEquals(2, headers.size());
    Assert.assertTrue(headers.contains("firstname"));
    Assert.assertTrue(headers.contains("lastname"));

    List<String> lines = result.getLines();
    Assert.assertEquals(6, lines.size());
    assertEquals(lines.get(0), "Amber,Duke");
    assertEquals(lines.get(1), "Hattie,Bond");
    assertEquals(lines.get(2), "Nanette,Bates");
    assertEquals(lines.get(0), "Amber,Duke");
    assertEquals(lines.get(1), "Hattie,Bond");
    assertEquals(lines.get(2), "Nanette,Bates");
  }

  @Test
  public void unionWithAliasLeftTest() throws IOException {
    String query =
        String.format(
            Locale.ROOT,
            "SELECT lastname AS firstname FROM %s LIMIT 3 "
                + "UNION ALL SELECT firstname FROM %s LIMIT 3",
            TestsConstants.TEST_INDEX_ACCOUNT,
            TestsConstants.TEST_INDEX_ACCOUNT);
    CSVResult result = executeCsvRequest(query, false, false, false);
    List<String> headers = result.getHeaders();
    Assert.assertEquals(1, headers.size());
    Assert.assertTrue(headers.contains("firstname"));

    List<String> lines = result.getLines();
    Assert.assertEquals(6, lines.size());
    assertEquals(lines.get(0), "Duke");
    assertEquals(lines.get(1), "Bond");
    assertEquals(lines.get(2), "Bates");
    assertEquals(lines.get(3), "Amber");
    assertEquals(lines.get(4), "Hattie");
    assertEquals(lines.get(5), "Nanette");
  }

  @Test
  public void unionWithAliasRightTest() throws IOException {
    String query =
        String.format(
            Locale.ROOT,
            "SELECT firstname FROM %s LIMIT 3 "
                + "UNION ALL SELECT lastname AS firstname FROM %s LIMIT 3",
            TestsConstants.TEST_INDEX_ACCOUNT,
            TestsConstants.TEST_INDEX_ACCOUNT);
    CSVResult result = executeCsvRequest(query, false, false, false);
    List<String> headers = result.getHeaders();
    Assert.assertEquals(1, headers.size());
    Assert.assertTrue(headers.contains("firstname"));

    List<String> lines = result.getLines();
    Assert.assertEquals(6, lines.size());
    assertEquals(lines.get(0), "Amber");
    assertEquals(lines.get(1), "Hattie");
    assertEquals(lines.get(2), "Nanette");
    assertEquals(lines.get(3), "Duke");
    assertEquals(lines.get(4), "Bond");
    assertEquals(lines.get(5), "Bates");
  }

  @Test
  public void unionWithAliasBothSideTest() throws IOException {
    String query =
        String.format(
            Locale.ROOT,
            "SELECT firstname AS name FROM %s LIMIT 3 "
                + "UNION ALL SELECT lastname AS name FROM %s LIMIT 3",
            TestsConstants.TEST_INDEX_ACCOUNT,
            TestsConstants.TEST_INDEX_ACCOUNT);
    CSVResult result = executeCsvRequest(query, false, false, false);
    List<String> headers = result.getHeaders();
    Assert.assertEquals(1, headers.size());
    Assert.assertTrue(headers.contains("name"));

    List<String> lines = result.getLines();
    Assert.assertEquals(6, lines.size());
    assertEquals(lines.get(0), "Amber");
    assertEquals(lines.get(1), "Hattie");
    assertEquals(lines.get(2), "Nanette");
    assertEquals(lines.get(3), "Duke");
    assertEquals(lines.get(4), "Bond");
    assertEquals(lines.get(5), "Bates");
  }

  private void verifyFieldOrder(final String[] expectedFields) throws IOException {

    final String fields = String.join(", ", expectedFields);
    final String query =
        String.format(
            Locale.ROOT,
            "SELECT %s FROM %s " + "WHERE email='amberduke@pyrami.com'",
            fields,
            TEST_INDEX_ACCOUNT);

    verifyFieldOrder(expectedFields, query);
  }

  private void verifyFieldOrder(final String[] expectedFields, final String query)
      throws IOException {

    final String result = executeQueryWithStringOutput(query);

    final String expectedHeader = String.join(",", expectedFields);
    Assert.assertThat(result, startsWith(expectedHeader));
  }

  private void setFlatOption(boolean flat) {

    this.flatOption = flat;
  }

  private CSVResult executeCsvRequest(final String query, boolean flat) throws IOException {

    return executeCsvRequest(query, flat, false, false);
  }

  private CSVResult executeCsvRequest(
      final String query, boolean flat, boolean includeScore, boolean includeId)
      throws IOException {

    final String requestBody = super.makeRequest(query);
    final String endpoint =
        String.format(
            Locale.ROOT,
            "/_plugins/_sql?format=csv&flat=%b&_id=%b&_score=%b",
            flat,
            includeId,
            includeScore);
    final Request sqlRequest = new Request("POST", endpoint);
    sqlRequest.setJsonEntity(requestBody);
    RequestOptions.Builder restOptionsBuilder = RequestOptions.DEFAULT.toBuilder();
    restOptionsBuilder.addHeader("Content-Type", "application/json");
    sqlRequest.setOptions(restOptionsBuilder);

    final Response response = client().performRequest(sqlRequest);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    final String responseString = TestUtils.getResponseBody(response, true);

    return csvResultFromStringResponse(responseString);
  }

  private CSVResult csvResultFromStringResponse(final String response) {

    final List<String> rows = new ArrayList<>();

    final String newLine = String.format(Locale.ROOT, "%n");
    int newLineIndex = response.indexOf(newLine);

    final String headerLine;

    if (-1 == newLineIndex) {
      // assuming only headers
      headerLine = response.trim();
    } else {
      headerLine = response.substring(0, newLineIndex).trim();
      final String rowLines = response.substring(newLineIndex + newLine.length()).trim();
      if (!rowLines.isEmpty()) {
        rows.addAll(Arrays.asList(rowLines.split(newLine)));
      }
    }

    final List<String> headers = new ArrayList<>(Arrays.asList(headerLine.split(",")));
    return new CSVResult(headers, rows);
  }

  private static AnyOf<List<String>> hasRow(
      final String prefix,
      final String suffix,
      final List<String> items,
      final boolean areItemsNested) {

    final Collection<List<String>> permutations = TestUtils.getPermutations(items);

    final List<Matcher<? super List<String>>> matchers =
        permutations.stream()
            .map(
                permutation -> {
                  final String delimiter = areItemsNested ? ", " : ",";
                  final String objectField = String.join(delimiter, permutation);
                  final String row =
                      String.format(
                          Locale.ROOT,
                          "%s%s%s%s%s",
                          printablePrefix(prefix),
                          areItemsNested ? "\"{" : "",
                          objectField,
                          areItemsNested ? "}\"" : "",
                          printableSuffix(suffix));
                  return hasItem(row);
                })
            .collect(Collectors.toCollection(LinkedList::new));

    return anyOf(matchers);
  }

  private static String printablePrefix(final String prefix) {

    if (prefix == null || prefix.trim().isEmpty()) {
      return "";
    }

    return prefix + ",";
  }

  private static String printableSuffix(final String suffix) {

    if (suffix == null || suffix.trim().isEmpty()) {
      return "";
    }

    return "," + suffix;
  }
}
