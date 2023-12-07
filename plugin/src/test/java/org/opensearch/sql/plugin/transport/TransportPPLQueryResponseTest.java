/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.opensearch.core.action.ActionResponse;

public class TransportPPLQueryResponseTest {

  @Rule public ExpectedException exceptionRule = ExpectedException.none();

  @Test
  public void testFromActionResponseSameClassloader() {
    TransportPPLQueryResponse response1 = new TransportPPLQueryResponse("mock result");
    TransportPPLQueryResponse response2 = TransportPPLQueryResponse.fromActionResponse(response1);
    assertEquals(response1.getResult(), response2.getResult());
  }

  @Test
  public void testFromActionResponseDifferentClassLoader()
      throws ClassNotFoundException,
          InstantiationException,
          IllegalAccessException,
          NoSuchMethodException,
          InvocationTargetException {
    ClassLoader loader = TransportPPLQueryResponseTest.class.getClassLoader();
    String classFilePath =
        loader
            .getResource("org/opensearch/sql/plugin/transport/TransportPPLQueryResponse.class")
            .getPath();

    CustomClassLoader classLoader1 = new CustomClassLoader(classFilePath);
    CustomClassLoader classLoader2 = new CustomClassLoader(classFilePath);

    Class<?> class1 =
        classLoader1.findClass("org.opensearch.sql.plugin.transport.TransportPPLQueryResponse");
    Class<?> class2 =
        classLoader2.findClass("org.opensearch.sql.plugin.transport.TransportPPLQueryResponse");

    assertFalse(class1.isAssignableFrom(class2));
    String result = "mock result";
    TransportPPLQueryResponse response2 =
        TransportPPLQueryResponse.fromActionResponse(
            (ActionResponse) class1.getDeclaredConstructor(String.class).newInstance(result));
    assertEquals(result, response2.getResult());
  }
}

class CustomClassLoader extends ClassLoader {

  private final Path classFilePath;

  public CustomClassLoader(String classFilePath) {
    this.classFilePath = Path.of(classFilePath);
  }

  @Override
  protected Class<?> findClass(String name) throws ClassNotFoundException {
    try {
      byte[] classBytes = Files.readAllBytes(classFilePath);
      return defineClass(name, classBytes, 0, classBytes.length);
    } catch (IOException e) {
      throw new ClassNotFoundException("Failed to load class: " + name, e);
    }
  }
}
