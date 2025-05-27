/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.utils;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Locale;
import org.apache.commons.lang3.ObjectUtils;

public final class ReflectionUtils {

  private ReflectionUtils() {}

  public static Object getFieldValue(Object object, Field field) {
    try {
      return field.get(object);
    } catch (IllegalAccessException e) {
      throw new IllegalArgumentException("Get reflection field value error", e);
    }
  }

  public static void setFieldValue(Object object, Field field, Object value) {
    try {
      field.set(object, value);
    } catch (IllegalAccessException e) {
      throw new IllegalArgumentException("Cannot set field value for reflection object", e);
    }
  }

  public static void makeAccessible(Field field) {
    if (!Modifier.isPublic(field.getModifiers())) {
      field.setAccessible(true);
    }
  }

  public static Field getDeclaredField(Object object, String fieldName) {
    NoSuchFieldException noSuchFieldEx = null;
    for (Class<?> superClass = object.getClass();
        superClass != Object.class;
        superClass = superClass.getSuperclass()) {
      try {
        return superClass.getDeclaredField(fieldName);
      } catch (NoSuchFieldException e) {
        noSuchFieldEx = ObjectUtils.firstNonNull(noSuchFieldEx, e);
        throw new IllegalStateException(
            String.format(
                Locale.ROOT,
                "Could not find field: %s in class: %s",
                fieldName,
                object.getClass().getName()),
            noSuchFieldEx);
      }
    }
    return null;
  }
}
