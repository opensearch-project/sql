/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.utils;

import java.util.regex.Pattern;
import lombok.experimental.UtilityClass;

@UtilityClass
public class PathUtils {

  /** Path separator string */
  public final String SEPARATOR = ".";

  /** Pattern that matches the path separator string */
  public final Pattern SEPARATOR_PATTERN = Pattern.compile(SEPARATOR, Pattern.LITERAL);
}
