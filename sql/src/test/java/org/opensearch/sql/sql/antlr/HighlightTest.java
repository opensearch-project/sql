/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql.antlr;

import org.junit.jupiter.api.Test;

public class HighlightTest extends SQLParserTest {
  @Test
  void single_field_test() {
    acceptQuery("SELECT HIGHLIGHT(Tags) FROM Index WHERE MATCH(Tags, 'Time')");
  }

  @Test
  void multiple_highlights_test() {
    acceptQuery(
        "SELECT HIGHLIGHT(Tags), HIGHLIGHT(Body) FROM Index "
            + "WHERE MULTI_MATCH([Tags, Body], 'Time')");
  }

  @Test
  void wildcard_test() {
    acceptQuery("SELECT HIGHLIGHT('T*') FROM Index WHERE MULTI_MATCH([Tags, Body], 'Time')");
  }

  @Test
  void highlight_all_test() {

    acceptQuery("SELECT HIGHLIGHT('*') FROM Index WHERE MULTI_MATCH([Tags, Body], 'Time')");
  }

  @Test
  void multiple_parameters_failure_test() {
    rejectQuery(
        "SELECT HIGHLIGHT(Tags1, Tags2) FROM Index WHERE MULTI_MATCH([Tags, Body], 'Time')");
  }

  @Test
  void no_parameters_failure_test() {
    rejectQuery("SELECT HIGHLIGHT() FROM Index WHERE MULTI_MATCH([Tags, Body], 'Time')");
  }
}
