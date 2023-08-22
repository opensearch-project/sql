/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.correctness.tests;

import static java.util.Collections.emptyMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.junit.Test;
import org.opensearch.sql.correctness.TestConfig;

/** Tests for {@link TestConfig} */
public class TestConfigTest {

  @Test
  public void testDefaultConfig() {
    TestConfig config = new TestConfig(emptyMap());
    assertThat(config.getOpenSearchHostUrl(), is(emptyString()));
    assertThat(
        config.getOtherDbConnectionNameAndUrls(),
        allOf(
            hasEntry("H2", "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1"),
            hasEntry("SQLite", "jdbc:sqlite::memory:")));
  }

  @Test
  public void testCustomESUrls() {
    Map<String, String> args = ImmutableMap.of("esHost", "localhost:9200");
    TestConfig config = new TestConfig(args);
    assertThat(config.getOpenSearchHostUrl(), is("localhost:9200"));
  }

  @Test
  public void testCustomDbUrls() {
    Map<String, String> args =
        ImmutableMap.of(
            "otherDbUrls",
            "H2=jdbc:h2:mem:test;DB_CLOSE_DELAY=-1," + "Derby=jdbc:derby:memory:myDb;create=true");

    TestConfig config = new TestConfig(args);
    assertThat(
        config.getOtherDbConnectionNameAndUrls(),
        allOf(
            hasEntry("H2", "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1"),
            hasEntry("Derby", "jdbc:derby:memory:myDb;create=true")));
  }
}
