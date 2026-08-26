/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.ObjectInputFilter;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.common.setting.Settings;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
public class DeserializationFilterUtilTest {

  @Test
  void allowlisted_class_is_allowed() {
    ObjectInputFilter filter = DeserializationFilterUtil.createFilter(null, "");
    assertEquals(
        ObjectInputFilter.Status.ALLOWED,
        filter.checkInput(info(String.class, /*depth*/ 1, /*refs*/ 1, /*bytes*/ 100)));
  }

  @Test
  void disallowed_class_is_rejected() {
    ObjectInputFilter filter = DeserializationFilterUtil.createFilter(null, "");
    assertEquals(
        ObjectInputFilter.Status.REJECTED,
        filter.checkInput(info(java.net.URL.class, /*depth*/ 1, /*refs*/ 1, /*bytes*/ 100)));
  }

  @Test
  void null_settings_fall_back_to_default_limits() {
    // depth 21 > DEFAULT_MAX_DEPTH (20); refs 1001 > DEFAULT_MAX_REFS (1000);
    // bytes 15001 > DEFAULT_MAX_BYTES (15000).
    ObjectInputFilter filter = DeserializationFilterUtil.createFilter(null, "");
    assertEquals(
        ObjectInputFilter.Status.REJECTED,
        filter.checkInput(info(/*class*/ null, /*depth*/ 21, /*refs*/ 1, /*bytes*/ 100)));
    assertEquals(
        ObjectInputFilter.Status.REJECTED,
        filter.checkInput(info(/*class*/ null, /*depth*/ 1, /*refs*/ 1001, /*bytes*/ 100)));
    assertEquals(
        ObjectInputFilter.Status.REJECTED,
        filter.checkInput(info(/*class*/ null, /*depth*/ 1, /*refs*/ 1, /*bytes*/ 15001)));
  }

  @Test
  void limits_are_read_from_settings() {
    // Settings override the defaults; the filter must enforce the configured values.
    Settings settings = settingsWith(/*depth*/ 5, /*refs*/ 10, /*bytes*/ 100);
    ObjectInputFilter filter = DeserializationFilterUtil.createFilter(settings, "");
    assertEquals(
        ObjectInputFilter.Status.REJECTED,
        filter.checkInput(info(/*class*/ null, /*depth*/ 6, /*refs*/ 1, /*bytes*/ 50)));
    assertEquals(
        ObjectInputFilter.Status.REJECTED,
        filter.checkInput(info(/*class*/ null, /*depth*/ 1, /*refs*/ 11, /*bytes*/ 50)));
    assertEquals(
        ObjectInputFilter.Status.REJECTED,
        filter.checkInput(info(/*class*/ null, /*depth*/ 1, /*refs*/ 1, /*bytes*/ 101)));
    assertEquals(
        ObjectInputFilter.Status.ALLOWED,
        filter.checkInput(info(String.class, /*depth*/ 1, /*refs*/ 1, /*bytes*/ 50)));
  }

  @Test
  void additional_pattern_is_honored() {
    // Patterns passed in should extend the base allowlist.
    ObjectInputFilter filter = DeserializationFilterUtil.createFilter(null, "java.net.URI;");
    assertEquals(
        ObjectInputFilter.Status.ALLOWED,
        filter.checkInput(info(java.net.URI.class, /*depth*/ 1, /*refs*/ 1, /*bytes*/ 100)));
  }

  private static Settings settingsWith(int depth, int refs, int bytes) {
    Map<Settings.Key, Object> values =
        Map.of(
            Settings.Key.DESERIALIZATION_MAX_DEPTH, depth,
            Settings.Key.DESERIALIZATION_MAX_REFS, refs,
            Settings.Key.DESERIALIZATION_MAX_BYTES, bytes);
    return new Settings() {
      @Override
      @SuppressWarnings("unchecked")
      public <T> T getSettingValue(Settings.Key key) {
        return (T) values.get(key);
      }

      @Override
      public List<?> getSettings() {
        return List.of();
      }
    };
  }

  private static ObjectInputFilter.FilterInfo info(
      Class<?> cls, long depth, long refs, long bytes) {
    return new ObjectInputFilter.FilterInfo() {
      @Override
      public Class<?> serialClass() {
        return cls;
      }

      @Override
      public long arrayLength() {
        return -1;
      }

      @Override
      public long depth() {
        return depth;
      }

      @Override
      public long references() {
        return refs;
      }

      @Override
      public long streamBytes() {
        return bytes;
      }
    };
  }
}
