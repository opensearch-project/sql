/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.jdbc.parser;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.opensearch.sql.jdbc.parser.PropertiesParser.DRIVER;
import static org.opensearch.sql.jdbc.parser.PropertiesParser.PASSWORD;
import static org.opensearch.sql.jdbc.parser.PropertiesParser.URL;
import static org.opensearch.sql.jdbc.parser.PropertiesParser.USERNAME;

import com.google.common.collect.ImmutableMap;
import java.util.Properties;
import org.junit.jupiter.api.Test;

class PropertiesParserTest {
  @Test
  public void parse() {
    Properties properties =
        new PropertiesParser()
            .parse(
                ImmutableMap.of(
                    URL, "jdbc:hive2://localhost:10000/default",
                    USERNAME, "username",
                    PASSWORD, "password",
                    DRIVER, "org.apache.hive.jdbc.HiveDriver"));
    assertEquals("jdbc:hive2://localhost:10000/default", properties.getProperty(URL));
    assertEquals("username", properties.getProperty(USERNAME));
    assertEquals("password", properties.getProperty(PASSWORD));
    assertEquals("org.apache.hive.jdbc.HiveDriver", properties.getProperty(DRIVER));
  }
}
