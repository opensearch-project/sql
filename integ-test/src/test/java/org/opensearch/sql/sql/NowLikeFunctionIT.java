/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql;

import static com.carrotsearch.randomizedtesting.RandomizedTest.$;
import static com.carrotsearch.randomizedtesting.RandomizedTest.$$;
import static org.opensearch.sql.legacy.plugin.RestSqlAction.QUERY_API_ENDPOINT;
import static org.opensearch.sql.util.TestUtils.getResponseBody;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import java.io.IOException;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.time.temporal.Temporal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Locale;
import java.util.TimeZone;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.sql.legacy.SQLIntegTestCase;

public class NowLikeFunctionIT extends SQLIntegTestCase {
  @Override
  public void init() throws Exception {
    super.init();
  }

  // Integration test framework sets for OpenSearch instance a random timezone.
  // If server's TZ doesn't match localhost's TZ, time measurements for `now` would differ.
  // We should set localhost's TZ now and recover the value back in the end of the test.
  private final TimeZone testTz = TimeZone.getDefault();
  private final TimeZone systemTz = TimeZone.getTimeZone(System.getProperty("user.timezone"));

  @Before
  public void setTimeZone() {
    TimeZone.setDefault(systemTz);
  }

  @After
  public void resetTimeZone() {
    TimeZone.setDefault(testTz);
  }

  private final String name;
  private final Boolean hasFsp;
  private final Boolean hasShortcut;
  private final Boolean constValue;
  private final Supplier<Temporal> referenceGetter;
  private final BiFunction<CharSequence, DateTimeFormatter, Temporal> parser;
  private final String serializationPatternStr;

  public NowLikeFunctionIT(
      @Name("name") String name,
      @Name("hasFsp") Boolean hasFsp,
      @Name("hasShortcut") Boolean hasShortcut,
      @Name("constValue") Boolean constValue,
      @Name("referenceGetter") Supplier<Temporal> referenceGetter,
      @Name("parser") BiFunction<CharSequence, DateTimeFormatter, Temporal> parser,
      @Name("serializationPatternStr") String serializationPatternStr) {
    this.name = name;
    this.hasFsp = hasFsp;
    this.hasShortcut = hasShortcut;
    this.constValue = constValue;
    this.referenceGetter = referenceGetter;
    this.parser = parser;
    this.serializationPatternStr = serializationPatternStr;
  }

  @ParametersFactory(argumentFormatting = "%1$s")
  public static Iterable<Object[]> compareTwoDates() {
    return Arrays.asList(
        $$(
            $(
                "now",
                false,
                false,
                true,
                (Supplier<Temporal>) LocalDateTime::now,
                (BiFunction<CharSequence, DateTimeFormatter, Temporal>) LocalDateTime::parse,
                "uuuu-MM-dd HH:mm:ss"),
            $(
                "current_timestamp",
                false,
                false,
                true,
                (Supplier<Temporal>) LocalDateTime::now,
                (BiFunction<CharSequence, DateTimeFormatter, Temporal>) LocalDateTime::parse,
                "uuuu-MM-dd HH:mm:ss"),
            $(
                "localtimestamp",
                false,
                false,
                true,
                (Supplier<Temporal>) LocalDateTime::now,
                (BiFunction<CharSequence, DateTimeFormatter, Temporal>) LocalDateTime::parse,
                "uuuu-MM-dd HH:mm:ss"),
            $(
                "localtime",
                false,
                false,
                true,
                (Supplier<Temporal>) LocalDateTime::now,
                (BiFunction<CharSequence, DateTimeFormatter, Temporal>) LocalDateTime::parse,
                "uuuu-MM-dd HH:mm:ss"),
            $(
                "sysdate",
                true,
                false,
                false,
                (Supplier<Temporal>) LocalDateTime::now,
                (BiFunction<CharSequence, DateTimeFormatter, Temporal>) LocalDateTime::parse,
                "uuuu-MM-dd HH:mm:ss"),
            $(
                "curtime",
                false,
                false,
                false,
                (Supplier<Temporal>) LocalTime::now,
                (BiFunction<CharSequence, DateTimeFormatter, Temporal>) LocalTime::parse,
                "HH:mm:ss"),
            $(
                "current_time",
                false,
                false,
                false,
                (Supplier<Temporal>) LocalTime::now,
                (BiFunction<CharSequence, DateTimeFormatter, Temporal>) LocalTime::parse,
                "HH:mm:ss"),
            $(
                "curdate",
                false,
                false,
                false,
                (Supplier<Temporal>) LocalDate::now,
                (BiFunction<CharSequence, DateTimeFormatter, Temporal>) LocalDate::parse,
                "uuuu-MM-dd"),
            $(
                "current_date",
                false,
                false,
                false,
                (Supplier<Temporal>) LocalDate::now,
                (BiFunction<CharSequence, DateTimeFormatter, Temporal>) LocalDate::parse,
                "uuuu-MM-dd"),
            $(
                "utc_date",
                false,
                false,
                true,
                (Supplier<Temporal>) (() -> utcDateTimeNow().toLocalDate()),
                (BiFunction<CharSequence, DateTimeFormatter, Temporal>) LocalDate::parse,
                "uuuu-MM-dd"),
            $(
                "utc_time",
                false,
                false,
                true,
                (Supplier<Temporal>) (() -> utcDateTimeNow().toLocalTime()),
                (BiFunction<CharSequence, DateTimeFormatter, Temporal>) LocalTime::parse,
                "HH:mm:ss"),
            $(
                "utc_timestamp",
                false,
                false,
                true,
                (Supplier<Temporal>) (NowLikeFunctionIT::utcDateTimeNow),
                (BiFunction<CharSequence, DateTimeFormatter, Temporal>) LocalDateTime::parse,
                "uuuu-MM-dd HH:mm:ss")));
  }

  private long getDiff(Temporal sample, Temporal reference) {
    if (sample instanceof LocalDate) {
      return Period.between((LocalDate) sample, (LocalDate) reference).getDays();
    }
    return Duration.between(sample, reference).toSeconds();
  }

  public static LocalDateTime utcDateTimeNow() {
    ZonedDateTime zonedDateTime = LocalDateTime.now().atZone(TimeZone.getDefault().toZoneId());
    return zonedDateTime.withZoneSameInstant(ZoneId.of("UTC")).toLocalDateTime();
  }

  @Test
  public void testNowLikeFunctions() throws IOException {
    var serializationPattern =
        new DateTimeFormatterBuilder()
            .appendPattern(serializationPatternStr)
            .optionalStart()
            .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
            .toFormatter();

    Temporal reference = referenceGetter.get();
    double delta = 2d; // acceptable time diff, secs
    if (reference instanceof LocalDate)
      delta = 1d; // Max date delta could be 1 if test runs on the very edge of two days
    // We ignore probability of a test run on edge of month or year to simplify the checks

    var calls =
        new ArrayList<String>() {
          {
            add(name + "()");
          }
        };
    if (hasShortcut) calls.add(name);
    if (hasFsp) calls.add(name + "(0)");

    // Column order is: func(), func, func(0)
    //                   shortcut ^    fsp ^
    JSONObject result = executeQuery("select " + String.join(", ", calls));

    var rows = result.getJSONArray("datarows");
    JSONArray firstRow = rows.getJSONArray(0);
    for (int i = 0; i < rows.length(); i++) {
      var row = rows.getJSONArray(i);
      if (constValue) assertTrue(firstRow.similar(row));

      int column = 0;
      assertEquals(
          0,
          getDiff(reference, parser.apply(row.getString(column++), serializationPattern)),
          delta);

      if (hasShortcut) {
        assertEquals(
            0,
            getDiff(reference, parser.apply(row.getString(column++), serializationPattern)),
            delta);
      }
      if (hasFsp) {
        assertEquals(
            0,
            getDiff(reference, parser.apply(row.getString(column), serializationPattern)),
            delta);
      }
    }
  }

  protected JSONObject executeQuery(String query) throws IOException {
    Request request = new Request("POST", QUERY_API_ENDPOINT);
    request.setJsonEntity(String.format(Locale.ROOT, "{\n" + "  \"query\": \"%s\"\n" + "}", query));

    RequestOptions.Builder restOptionsBuilder = RequestOptions.DEFAULT.toBuilder();
    restOptionsBuilder.addHeader("Content-Type", "application/json");
    request.setOptions(restOptionsBuilder);

    Response response = client().performRequest(request);
    return new JSONObject(getResponseBody(response));
  }
}
