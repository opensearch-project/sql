/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

import static org.junit.Assert.assertTrue;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;

/**
 * Alerting and other plugins call {@code PPLQueryAction} with this module's classes plus core, ppl,
 * and protocol, but without the {@code opensearch} module (#5923). The classes they load must not
 * reference that module's types.
 */
public class ThinJarClassesTest {

  private static final String EXCLUDED_PACKAGE = "org.opensearch.sql.opensearch.";

  private static final List<Class<?>> LOADED_BY_OTHER_PLUGINS =
      List.of(
          PPLQueryAction.class,
          TransportPPLQueryRequest.class,
          TransportPPLQueryResponse.class,
          PPLQueryTask.class);

  @Test
  public void classesLoadedByOtherPluginsDoNotReferenceTheOpenSearchModule() {
    List<String> violations = new ArrayList<>();
    for (Class<?> type : LOADED_BY_OTHER_PLUGINS) {
      for (Class<?> referenced : referencedTypes(type)) {
        if (referenced.getName().startsWith(EXCLUDED_PACKAGE)) {
          violations.add(type.getSimpleName() + " -> " + referenced.getName());
        }
      }
    }
    assertTrue("Types outside the thin jar: " + violations, violations.isEmpty());
  }

  private static List<Class<?>> referencedTypes(Class<?> type) {
    List<Class<?>> types = new ArrayList<>();
    for (Class<?> c = type; c != null && c != Object.class; c = c.getSuperclass()) {
      types.add(c);
      types.addAll(List.of(c.getInterfaces()));
      for (Field field : c.getDeclaredFields()) {
        types.add(field.getType());
      }
      for (Method method : c.getDeclaredMethods()) {
        types.add(method.getReturnType());
        types.addAll(List.of(method.getParameterTypes()));
      }
      for (Constructor<?> constructor : c.getDeclaredConstructors()) {
        types.addAll(List.of(constructor.getParameterTypes()));
      }
    }
    return types;
  }
}
