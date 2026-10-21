/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.toIntArg;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.TreeSet;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

/**
 * Guards against PPL UDF runtime methods that take a Java {@code int}.
 *
 * <p>PPL widens integer arithmetic to BIGINT for overflow safety (#5603), so an argument such as
 * {@code 1 + 1} reaches the runtime as a {@code long}. Code generation cannot pass a {@code long}
 * to an {@code int} parameter, so the query fails (#5660). UDF methods must take {@code long} and
 * convert internally (e.g. {@code Math.toIntExact}). Calcite built-ins, whose signatures we cannot
 * change, are narrowed in {@code PPLFuncImpTable#narrowIntArgs} instead.
 */
class UDFIntParameterTest {

  private static final String UDF_PACKAGE = "org/opensearch/sql/expression/function/udf";

  @Test
  void udfMethodsDoNotTakeIntParameters() throws Exception {
    Path location =
        Paths.get(ImplementorUDF.class.getProtectionDomain().getCodeSource().getLocation().toURI());
    TreeSet<String> violations = new TreeSet<>();
    try (FileSystem jar =
        Files.isDirectory(location) ? null : FileSystems.newFileSystem(location)) {
      Path root = jar == null ? location : jar.getPath("/");
      try (Stream<Path> files = Files.walk(root.resolve(UDF_PACKAGE))) {
        for (Path file : files.filter(f -> f.toString().endsWith(".class")).toList()) {
          String className =
              root.relativize(file).toString().replace('/', '.').replaceAll("\\.class$", "");
          for (Method method :
              Class.forName(className, false, getClass().getClassLoader()).getDeclaredMethods()) {
            if (Modifier.isPublic(method.getModifiers())
                && Modifier.isStatic(method.getModifiers())
                && List.of(method.getParameterTypes()).contains(int.class)) {
              violations.add(
                  className
                      + "#"
                      + method.getName()
                      + Arrays.toString(
                          Arrays.stream(method.getParameterTypes())
                              .map(Class::getSimpleName)
                              .toArray()));
            }
          }
        }
      }
    }
    assertTrue(
        violations.isEmpty(),
        "UDF methods must take long instead of int (#5660); found: " + violations);
  }

  @Test
  void toIntArgConvertsInRangeValues() {
    assertEquals(256, toIntArg(256L));
    assertEquals(Integer.MAX_VALUE, toIntArg(Integer.MAX_VALUE));
    assertEquals(Integer.MIN_VALUE, toIntArg(Integer.MIN_VALUE));
  }

  @Test
  void toIntArgRejectsOutOfRangeValues() {
    // 2^32 + 2 would silently wrap to 2 with a plain (int) cast.
    ArithmeticException e = assertThrows(ArithmeticException.class, () -> toIntArg(4294967298L));
    assertEquals("Value 4294967298 out of range", e.getMessage());
  }
}
