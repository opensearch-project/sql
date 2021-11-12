/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.doctest.core.markup;

import java.io.Closeable;

/**
 * Document for different format and markup
 */
public interface Document extends Closeable {

  /**
   * Remove checked IOException in method signature.
   */
  @Override
  void close();

  Document section(String title);

  Document subSection(String title);

  Document paragraph(String text);

  Document codeBlock(String description, String code);

  Document table(String description, String table);

  Document image(String description, String filePath);

}
