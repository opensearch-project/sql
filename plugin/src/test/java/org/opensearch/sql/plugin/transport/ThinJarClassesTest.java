/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;

/**
 * Alerting and other plugins call {@code PPLQueryAction} with this module's classes plus core, ppl,
 * and protocol, but without the {@code opensearch} module (#5923). The classes they load must not
 * reference that module's types, and must link with only that classpath (#5925).
 */
public class ThinJarClassesTest {

  private static final String EXCLUDED_PACKAGE = "org.opensearch.sql.opensearch.";

  /** Set by the plugin test task: the thin jar, core, ppl, protocol, OpenSearch and org.json. */
  private static final String THIN_JAR_CLASSPATH = "tests.thinJar.classpath";

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

  @Test
  public void classesLoadedByOtherPluginsLinkWithOnlyTheThinJarClasspath() throws Exception {
    String classpath = System.getProperty(THIN_JAR_CLASSPATH);
    assertNotNull(THIN_JAR_CLASSPATH + " is set by the plugin test task", classpath);
    List<URL> urls = new ArrayList<>();
    for (String path : classpath.split(File.pathSeparator)) {
      urls.add(Path.of(path).toUri().toURL());
    }
    List<String> failures = new ArrayList<>();
    try (URLClassLoader loader =
        new URLClassLoader(urls.toArray(URL[]::new), ClassLoader.getPlatformClassLoader())) {
      for (Class<?> type : LOADED_BY_OTHER_PLUGINS) {
        try {
          referencedTypes(Class.forName(type.getName(), true, loader));
        } catch (LinkageError | ClassNotFoundException e) {
          failures.add(type.getSimpleName() + " -> " + e);
        }
      }
    }
    assertTrue(
        "Classes that do not link with the thin jar classpath: " + failures, failures.isEmpty());
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
