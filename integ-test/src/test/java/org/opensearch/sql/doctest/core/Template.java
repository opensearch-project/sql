/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.doctest.core;

import static java.nio.file.StandardCopyOption.COPY_ATTRIBUTES;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.opensearch.sql.legacy.utils.StringUtils;
import org.opensearch.sql.util.TestUtils;

/**
 * Abstraction for document template file
 */
public class Template {

  private static final String TEMPLATE_FOLDER_ROOT = "src/test/resources/doctest/templates/";

  private final Path templateFullPath;

  public Template(String templateRelativePath) {
    this.templateFullPath =
        Paths.get(TestUtils.getResourceFilePath(TEMPLATE_FOLDER_ROOT + templateRelativePath));
  }

  /**
   * Copy template file to target document. Replace it if existing.
   *
   * @param docFullPath full path of target document
   */
  public void copyToDocument(Path docFullPath) {
    try {
      Files.createDirectories(docFullPath.getParent());
      Files.copy(templateFullPath, docFullPath, REPLACE_EXISTING, COPY_ATTRIBUTES);
    } catch (IOException e) {
      throw new IllegalStateException(StringUtils.format(
          "Failed to copy from template [%s] to document file [%s]", templateFullPath, docFullPath),
          e);
    }
  }

}
