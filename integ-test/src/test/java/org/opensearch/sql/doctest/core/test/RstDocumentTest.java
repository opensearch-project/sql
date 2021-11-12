/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.doctest.core.test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.sql.doctest.core.markup.RstDocument;

/**
 * Test cases for {@link RstDocument}
 */
public class RstDocumentTest {

  private ByteArrayOutputStream content;

  private RstDocument document;

  @Before
  public void setUp() {
    content = new ByteArrayOutputStream();
    document = new RstDocument(new PrintWriter(content, true)); // Enable auto flush
  }

  @Test
  public void testSection() {
    document.section("Test Section");
    assertThat(
        content.toString(),
        is(
            "Test Section\n" +
                "============\n" +
                "\n"
        )
    );
  }

  @Test
  public void testSubSection() {
    document.subSection("Test Sub Section");
    assertThat(
        content.toString(),
        is(
            "Test Sub Section\n" +
                "----------------\n" +
                "\n"
        )
    );
  }

  @Test
  public void testParagraph() {
    document.paragraph("Test paragraph");
    assertThat(
        content.toString(),
        is(
            "Test paragraph\n" +
                "\n"
        )
    );
  }

  @Test
  public void testCodeBlock() {
    document.codeBlock("Test code", ">> curl localhost:9200");
    assertThat(
        content.toString(),
        is(
            "Test code::\n" +
                "\n" +
                "\t>> curl localhost:9200\n" +
                "\n"
        )
    );
  }

  @Test
  public void testTable() {
    document.table(
        "Test table",
        "+----------+\n" +
            "|Test Table|\n" +
            "+==========+\n" +
            "| test data|\n" +
            "+----------+"
    );

    assertThat(
        content.toString(),
        is(
            "Test table:\n" +
                "\n" +
                "+----------+\n" +
                "|Test Table|\n" +
                "+==========+\n" +
                "| test data|\n" +
                "+----------+\n" +
                "\n"
        )
    );
  }

  @Test
  public void testImage() {
    document.image("Query syntax", "/docs/user/img/query_syntax.png");

    assertThat(
        content.toString(),
        is(
            "Query syntax:\n" +
                "\n" +
                ".. image:: /docs/user/img/query_syntax.png\n" +
                "\n"
        )
    );
  }

}
