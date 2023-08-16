/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.legacy;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

import com.fasterxml.jackson.core.JsonFactory;
import java.io.IOException;
import org.junit.Test;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.json.JsonXContentParser;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.search.SearchHit;

public class MathFunctionsIT extends SQLIntegTestCase {

  private static final String FROM = "FROM " + TestsConstants.TEST_INDEX_ACCOUNT;

  @Override
  protected void init() throws Exception {
    loadIndex(Index.ACCOUNT);
  }

  @Test
  public void lowerCaseFunctionCall() throws IOException {
    SearchHit[] hits = query(
        "SELECT abs(age - 100) AS abs"
    );
    for (SearchHit hit : hits) {
      double abs = (double) getField(hit, "abs");
      assertThat(abs, greaterThanOrEqualTo(0.0));
    }
  }

  @Test
  public void upperCaseFunctionCall() throws IOException {
    SearchHit[] hits = query(
        "SELECT ABS(age - 100) AS abs"
    );
    for (SearchHit hit : hits) {
      double abs = (double) getField(hit, "abs");
      assertThat(abs, greaterThanOrEqualTo(0.0));
    }
  }

  @Test
  public void eulersNumber() throws IOException {
    SearchHit[] hits = query(
        "SELECT E() AS e"
    );
    double e = (double) getField(hits[0], "e");
    assertThat(e, equalTo(Math.E));
  }

  @Test
  public void pi() throws IOException {
    SearchHit[] hits = query(
        "SELECT PI() AS pi"
    );
    double pi = (double) getField(hits[0], "pi");
    assertThat(pi, equalTo(Math.PI));
  }

  @Test
  public void expm1Function() throws IOException {
    SearchHit[] hits = query(
        "SELECT EXPM1(2) AS expm1"
    );
    double expm1 = (double) getField(hits[0], "expm1");
    assertThat(expm1, equalTo(Math.expm1(2)));
  }

  @Test
  public void degreesFunction() throws IOException {
    SearchHit[] hits = query(
        "SELECT age, DEGREES(age) AS degrees"
    );
    for (SearchHit hit : hits) {
      int age = (int) getFieldFromSource(hit, "age");
      double degrees = (double) getField(hit, "degrees");
      assertThat(degrees, equalTo(Math.toDegrees(age)));
    }
  }

  @Test
  public void radiansFunction() throws IOException {
    SearchHit[] hits = query(
        "SELECT age, RADIANS(age) as radians"
    );
    for (SearchHit hit : hits) {
      int age = (int) getFieldFromSource(hit, "age");
      double radians = (double) getField(hit, "radians");
      assertThat(radians, equalTo(Math.toRadians(age)));
    }
  }

  @Test
  public void sin() throws IOException {
    SearchHit[] hits = query(
        "SELECT SIN(PI()) as sin"
    );
    double sin = (double) getField(hits[0], "sin");
    assertThat(sin, equalTo(Math.sin(Math.PI)));
  }

  @Test
  public void asin() throws IOException {
    SearchHit[] hits = query(
        "SELECT ASIN(PI()) as asin"
    );
    double asin = Double.valueOf((String) getField(hits[0], "asin"));
    assertThat(asin, equalTo(Math.asin(Math.PI)));
  }

  @Test
  public void sinh() throws IOException {
    SearchHit[] hits = query(
        "SELECT SINH(PI()) as sinh"
    );
    double sinh = (double) getField(hits[0], "sinh");
    assertThat(sinh, equalTo(Math.sinh(Math.PI)));
  }

  @Test
  public void power() throws IOException {
    SearchHit[] hits = query(
        "SELECT POWER(age, 2) AS power",
        "WHERE (age IS NOT NULL) AND (balance IS NOT NULL) and (POWER(balance, 3) > 0)"
    );
    double power = (double) getField(hits[0], "power");
    assertTrue(power >= 0);
  }

  @Test
  public void atan2() throws IOException {
    SearchHit[] hits = query(
        "SELECT ATAN2(age, age) AS atan2",
        "WHERE (age IS NOT NULL) AND (ATAN2(age, age) > 0)"
    );
    double atan2 = (double) getField(hits[0], "atan2");
    assertThat(atan2, equalTo(Math.atan2(1, 1)));
  }

  @Test
  public void cot() throws IOException {
    SearchHit[] hits = query(
        "SELECT COT(PI()) AS cot"
    );
    double cot = (double) getField(hits[0], "cot");
    assertThat(cot, closeTo(1 / Math.tan(Math.PI), 0.001));
  }

  @Test
  public void sign() throws IOException {
    SearchHit[] hits = query(
        "SELECT SIGN(E()) AS sign"
    );
    double sign = (double) getField(hits[0], "sign");
    assertThat(sign, equalTo(Math.signum(Math.E)));
  }

  @Test
  public void logWithOneParam() throws IOException {
    SearchHit[] hits = query("SELECT LOG(3) AS log");
    double log = (double) getField(hits[0], "log");
    assertThat(log, equalTo(Math.log(3)));
  }

  @Test
  public void logWithTwoParams() throws IOException {
    SearchHit[] hits = query("SELECT LOG(2, 3) AS log");
    double log = (double) getField(hits[0], "log");
    assertThat(log, closeTo(Math.log(3) / Math.log(2), 0.0001));
  }

  @Test
  public void logInAggregationShouldPass() {
    assertThat(
        executeQuery(
            "SELECT LOG(age) FROM " + TestsConstants.TEST_INDEX_ACCOUNT
                + " WHERE age IS NOT NULL GROUP BY LOG(age) ORDER BY LOG(age)", "jdbc"
        ),
        containsString("\"type\": \"double\"")
    );
    assertThat(
        executeQuery(
            "SELECT LOG(2, age) FROM " + TestsConstants.TEST_INDEX_ACCOUNT +
                " WHERE age IS NOT NULL GROUP BY LOG(2, age) ORDER BY LOG(2, age)", "jdbc"
        ),
        containsString("\"type\": \"double\"")
    );
  }

  @Test
  public void log10Test() throws IOException {
    SearchHit[] hits = query("SELECT log10(1000) AS log10");
    double log10 = (double) getField(hits[0], "log10");
    assertThat(log10, equalTo(3.0));
  }

  @Test
  public void ln() throws IOException {
    SearchHit[] hits = query("SELECT LN(5) AS ln");
    double ln = (double) getField(hits[0], "ln");
    assertThat(ln, equalTo(Math.log(5)));
  }

  @Test
  public void lnInAggregationShouldPass() {
    assertThat(
        executeQuery(
            "SELECT LN(age) FROM " + TestsConstants.TEST_INDEX_ACCOUNT +
                " WHERE age IS NOT NULL GROUP BY LN(age) ORDER BY LN(age)", "jdbc"
        ),
        containsString("\"type\": \"double\"")
    );
  }

  @Test
  public void rand() throws IOException {
    SearchHit[] hits = query("SELECT RAND() AS rand", "ORDER BY rand");
    for (SearchHit hit : hits) {
      double rand = (double) getField(hit, "rand");
      assertTrue(rand >= 0 && rand < 1);
    }
  }

  private SearchHit[] query(String select, String... statements) throws IOException {
    final String response =
        executeQueryWithStringOutput(select + " " + FROM + " " + String.join(" ", statements));

    final XContentParser parser = new JsonXContentParser(
        NamedXContentRegistry.EMPTY,
        LoggingDeprecationHandler.INSTANCE,
        new JsonFactory().createParser(response));
    return SearchResponse.fromXContent(parser).getHits().getHits();
  }

  private Object getField(SearchHit hit, String fieldName) {
    return hit.field(fieldName).getValue();
  }

  private Object getFieldFromSource(SearchHit hit, String fieldName) {
    return hit.getSourceAsMap().get(fieldName);
  }
}
