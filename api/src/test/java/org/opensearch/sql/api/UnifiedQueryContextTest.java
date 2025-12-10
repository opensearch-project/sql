/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.opensearch.sql.common.setting.Settings.Key.*;

import org.junit.Test;
import org.opensearch.sql.calcite.SysLimit;
import org.opensearch.sql.executor.QueryType;

public class UnifiedQueryContextTest extends UnifiedQueryTestBase {

  @Test
  public void testContextCreationWithDefaults() {
    UnifiedQueryContext context =
        UnifiedQueryContext.builder()
            .queryType(QueryType.PPL)
            .catalog("opensearch", testSchema)
            .defaultNamespace("opensearch")
            .build();

    assertNotNull("Context should be created", context);
    assertNotNull("PlanContext should be created", context.getPlanContext());
    assertNotNull("Settings should be created", context.getSettings());
    assertEquals(
        "Settings should have default system limits",
        SysLimit.DEFAULT,
        SysLimit.fromSettings(context.getSettings()));
  }

  @Test
  public void testContextCreationWithCustomConfig() {
    UnifiedQueryContext context =
        UnifiedQueryContext.builder()
            .queryType(QueryType.PPL)
            .catalog("opensearch", testSchema)
            .cacheMetadata(true)
            .setting("plugins.query.size_limit", 200)
            .build();

    Integer querySizeLimit = context.getSettings().getSettingValue(QUERY_SIZE_LIMIT);
    assertEquals("Custom setting should be applied", Integer.valueOf(200), querySizeLimit);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidSettingName() {
    UnifiedQueryContext.builder()
        .queryType(QueryType.PPL)
        .catalog("opensearch", testSchema)
        .setting("invalid.setting.name", 123)
        .build();
  }

  @Test(expected = NullPointerException.class)
  public void testMissingQueryType() {
    UnifiedQueryContext.builder().catalog("opensearch", testSchema).build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testUnsupportedQueryType() {
    UnifiedQueryContext context =
        UnifiedQueryContext.builder()
            .queryType(QueryType.SQL) // only PPL is supported for now
            .catalog("opensearch", testSchema)
            .build();
    new UnifiedQueryPlanner(context);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidDefaultNamespace() {
    UnifiedQueryContext.builder()
        .queryType(QueryType.PPL)
        .catalog("opensearch", testSchema)
        .defaultNamespace("nonexistent")
        .build();
  }
}
