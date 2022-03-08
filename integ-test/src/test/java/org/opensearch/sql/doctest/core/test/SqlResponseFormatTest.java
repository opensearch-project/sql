/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.doctest.core.test;

import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.doctest.core.response.SqlResponseFormat.IGNORE_RESPONSE;
import static org.opensearch.sql.doctest.core.response.SqlResponseFormat.ORIGINAL_RESPONSE;
import static org.opensearch.sql.doctest.core.response.SqlResponseFormat.PRETTY_JSON_RESPONSE;
import static org.opensearch.sql.doctest.core.response.SqlResponseFormat.TABLE_RESPONSE;
import static org.opensearch.sql.doctest.core.response.SqlResponseFormat.TABLE_UNSORTED_RESPONSE;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import org.apache.http.HttpEntity;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.client.Response;
import org.opensearch.sql.doctest.core.response.SqlResponse;
import org.opensearch.sql.doctest.core.response.SqlResponseFormat;

/**
 * Test cases for {@link SqlResponseFormat}
 */
public class SqlResponseFormatTest {

  private final String expected =
      "{" +
          "\"schema\":[{\"name\":\"firstname\",\"type\":\"text\"}]," +
          "\"datarows\":[[\"John\"]]," +
          "\"total\":10," +
          "\"size\":1," +
          "\"status\":200" +
          "}";

  private SqlResponse sqlResponse;

  @Before
  public void setUp() throws IOException {
    mockResponse(expected);
  }

  @Test
  public void testIgnoreResponseFormat() {
    assertThat(IGNORE_RESPONSE.format(sqlResponse), emptyString());
  }

  @Test
  public void testOriginalFormat() {
    assertThat(ORIGINAL_RESPONSE.format(sqlResponse), is(expected + "\n"));
  }

  @Test
  public void testPrettyJsonFormat() {
    assertThat(
        PRETTY_JSON_RESPONSE.format(sqlResponse),
        is(
            "{\n" +
                "  \"schema\" : [\n" +
                "    {\n" +
                "      \"name\" : \"firstname\",\n" +
                "      \"type\" : \"text\"\n" +
                "    }\n" +
                "  ],\n" +
                "  \"datarows\" : [\n" +
                "    [\n" +
                "      \"John\"\n" +
                "    ]\n" +
                "  ],\n" +
                "  \"total\" : 10,\n" +
                "  \"size\" : 1,\n" +
                "  \"status\" : 200\n" +
                "}"
        )
    );
  }

  @Test
  public void testTableFormat() {
    assertThat(
        TABLE_RESPONSE.format(sqlResponse),
        is(
            "+---------+\n" +
                "|firstname|\n" +
                "+=========+\n" +
                "|     John|\n" +
                "+---------+\n"
        )
    );
  }

  @Test
  public void rowsInTableShouldBeSorted() throws IOException {
    mockResponse(
        "{" +
            "\"schema\":[" +
            "{\"name\":\"firstname\",\"type\":\"text\"}," +
            "{\"name\":\"age\",\"type\":\"integer\"}" +
            "]," +
            "\"datarows\":[" +
            "[\"John\", 30]," +
            "[\"John\", 24]," +
            "[\"Allen\", 45]" +
            "]," +
            "\"total\":10," +
            "\"size\":3," +
            "\"status\":200" +
            "}"
    );

    assertThat(
        TABLE_RESPONSE.format(sqlResponse),
        is(
            "+---------+---+\n" +
                "|firstname|age|\n" +
                "+=========+===+\n" +
                "|    Allen| 45|\n" +
                "+---------+---+\n" +
                "|     John| 24|\n" +
                "+---------+---+\n" +
                "|     John| 30|\n" +
                "+---------+---+\n"
        )
    );
  }

  @Test
  public void rowsInTableUnsortedShouldMaintainOriginalOrder() throws IOException {
    mockResponse(
        "{" +
            "\"schema\":[" +
            "{\"name\":\"firstname\",\"type\":\"text\"}," +
            "{\"name\":\"age\",\"type\":\"integer\"}" +
            "]," +
            "\"datarows\":[" +
            "[\"John\", 30]," +
            "[\"John\", 24]," +
            "[\"Allen\", 45]" +
            "]," +
            "\"total\":10," +
            "\"size\":3," +
            "\"status\":200" +
            "}"
    );

    assertThat(
        TABLE_UNSORTED_RESPONSE.format(sqlResponse),
        is(
            "+---------+---+\n" +
                "|firstname|age|\n" +
                "+=========+===+\n" +
                "|     John| 30|\n" +
                "+---------+---+\n" +
                "|     John| 24|\n" +
                "+---------+---+\n" +
                "|    Allen| 45|\n" +
                "+---------+---+\n"
        )
    );
  }

  private void mockResponse(String content) throws IOException {
    Response response = mock(Response.class);
    HttpEntity entity = mock(HttpEntity.class);
    when(response.getEntity()).thenReturn(entity);
    when(entity.getContent()).thenReturn(new ByteArrayInputStream(content.getBytes()));
    sqlResponse = new SqlResponse(response);
  }

}
