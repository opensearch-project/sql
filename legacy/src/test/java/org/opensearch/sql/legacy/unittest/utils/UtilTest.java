/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.unittest.utils;

import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;
import org.opensearch.sql.legacy.utils.Util;

public class UtilTest {

  @Test
  public void clearEmptyPaths_EmptyMap_ShouldReturnTrue() {
    Map<String, Object> map = new HashMap<>();
    boolean result = Util.clearEmptyPaths(map);
    //
    Assert.assertTrue(result);
  }

  @Test
  public void clearEmptyPaths_EmptyPathSize1_ShouldReturnTrueAndMapShouldBeEmpty() {
    Map<String, Object> map = new HashMap<>();
    map.put("a", new HashMap<String, Object>());
    boolean result = Util.clearEmptyPaths(map);
    Assert.assertTrue(result);
    Assert.assertEquals(0, map.size());
  }

  @Test
  public void clearEmptyPaths_EmptyPathSize2_ShouldReturnTrueAndMapShouldBeEmpty() {
    Map<String, Object> map = new HashMap<>();
    Map<String, Object> innerMap = new HashMap<>();
    innerMap.put("b", new HashMap<String, Object>());
    map.put("a", innerMap);
    boolean result = Util.clearEmptyPaths(map);
    Assert.assertTrue(result);
    Assert.assertEquals(0, map.size());
  }

  @Test
  public void clearEmptyPaths_2PathsOneEmpty_MapShouldBeSizeOne() {
    Map<String, Object> map = new HashMap<>();
    map.put("a", new HashMap<String, Object>());
    map.put("c", 1);
    Util.clearEmptyPaths(map);
    Assert.assertEquals(1, map.size());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void clearEmptyPaths_MapSizeTwoAndTwoOneInnerEmpty_MapShouldBeSizeTwoAndOne() {
    Map<String, Object> map = new HashMap<>();
    Map<String, Object> innerMap = new HashMap<>();
    innerMap.put("b", 2);
    innerMap.put("c", new HashMap<String, Object>());
    map.put("a", innerMap);
    map.put("c", 1);
    Util.clearEmptyPaths(map);
    Assert.assertEquals(2, map.size());
    Assert.assertEquals(1, ((HashMap<String, Object>) map.get("a")).size());
  }
}
