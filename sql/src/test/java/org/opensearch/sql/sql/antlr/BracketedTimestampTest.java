/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.sql.antlr;

import org.junit.jupiter.api.Test;

public class BracketedTimestampTest extends SQLParserTest {
  @Test
  void date_shortened_test() {
    acceptQuery("SELECT {d '2001-05-07'}");
  }

  @Test
  void date_test() {
    acceptQuery("SELECT {date '2001-05-07'}");
  }

  @Test
  void time_shortened_test() {
    acceptQuery("SELECT {t '10:11:12'}");
  }

  @Test
  void time_test() {
    acceptQuery("SELECT {time '10:11:12'}");
  }

  @Test
  void timestamp_shortened_test() {
    acceptQuery("SELECT {ts '2001-05-07 10:11:12'}");
  }

  @Test
  void timestamp_test() {
    acceptQuery("SELECT {timestamp '2001-05-07 10:11:12'}");
  }
}
