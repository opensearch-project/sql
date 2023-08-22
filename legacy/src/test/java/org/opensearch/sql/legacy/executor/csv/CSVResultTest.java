/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.executor.csv;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Test;

/** Unit tests for {@link CSVResult} */
public class CSVResultTest {

  private static final String SEPARATOR = ",";

  @Test
  public void getHeadersShouldReturnHeadersSanitized() {
    CSVResult csv = csv(headers("name", "=age"), lines(line("John", "30")));
    assertEquals(headers("name", "'=age"), csv.getHeaders());
  }

  @Test
  public void getLinesShouldReturnLinesSanitized() {
    CSVResult csv =
        csv(
            headers("name", "city"),
            lines(
                line("John", "Seattle"),
                line("John", "=Seattle"),
                line("John", "+Seattle"),
                line("-John", "Seattle"),
                line("@John", "Seattle"),
                line("John", "Seattle=")));

    assertEquals(
        line(
            "John,Seattle",
            "John,'=Seattle",
            "John,'+Seattle",
            "'-John,Seattle",
            "'@John,Seattle",
            "John,Seattle="),
        csv.getLines());
  }

  @Test
  public void getHeadersShouldReturnHeadersQuotedIfRequired() {
    CSVResult csv = csv(headers("na,me", ",,age"), lines(line("John", "30")));
    assertEquals(headers("\"na,me\"", "\",,age\""), csv.getHeaders());
  }

  @Test
  public void getLinesShouldReturnLinesQuotedIfRequired() {
    CSVResult csv = csv(headers("name", "age"), lines(line("John,Smith", "30,,,")));
    assertEquals(line("\"John,Smith\",\"30,,,\""), csv.getLines());
  }

  @Test
  public void getHeadersShouldReturnHeadersBothSanitizedAndQuotedIfRequired() {
    CSVResult csv =
        csv(headers("na,+me", ",,,=age", "=city,"), lines(line("John", "30", "Seattle")));
    assertEquals(headers("\"na,+me\"", "\",,,=age\"", "\"'=city,\""), csv.getHeaders());
  }

  @Test
  public void getLinesShouldReturnLinesBothSanitizedAndQuotedIfRequired() {
    CSVResult csv =
        csv(
            headers("name", "city"),
            lines(
                line("John", "Seattle"),
                line("John", "=Seattle"),
                line("John", "+Sea,ttle"),
                line(",-John", "Seattle"),
                line(",,,@John", "Seattle"),
                line("John", "Seattle=")));

    assertEquals(
        line(
            "John,Seattle",
            "John,'=Seattle",
            "John,\"'+Sea,ttle\"",
            "\",-John\",Seattle",
            "\",,,@John\",Seattle",
            "John,Seattle="),
        csv.getLines());
  }

  private CSVResult csv(List<String> headers, List<List<String>> lines) {
    return new CSVResult(SEPARATOR, headers, lines);
  }

  private List<String> headers(String... headers) {
    return Arrays.stream(headers).collect(Collectors.toList());
  }

  private List<String> line(String... line) {
    return Arrays.stream(line).collect(Collectors.toList());
  }

  @SafeVarargs
  private final List<List<String>> lines(List<String>... lines) {
    return Arrays.stream(lines).collect(Collectors.toList());
  }
}
