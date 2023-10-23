/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.utils;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import lombok.experimental.UtilityClass;
import org.apache.commons.lang3.RandomStringUtils;

@UtilityClass
public class IDUtils {
  public static final int PREFIX_LEN = 10;

  public static String decode(String id) {
    return new String(Base64.getDecoder().decode(id)).substring(PREFIX_LEN);
  }

  public static String encode(String datasourceName) {
    String randomId = RandomStringUtils.randomAlphanumeric(PREFIX_LEN) + datasourceName;
    return Base64.getEncoder().encodeToString(randomId.getBytes(StandardCharsets.UTF_8));
  }
}
