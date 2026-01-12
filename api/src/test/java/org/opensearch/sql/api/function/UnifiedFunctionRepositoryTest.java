/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.sql.api.UnifiedQueryTestBase;
import org.opensearch.sql.api.function.UnifiedFunctionRepository.UnifiedFunctionDescriptor;

public class UnifiedFunctionRepositoryTest extends UnifiedQueryTestBase {

  private UnifiedFunctionRepository repository;

  @Before
  @Override
  public void setUp() {
    super.setUp();
    repository = new UnifiedFunctionRepository(context);
  }

  @Test
  public void testLoadAllFunctions() {
    List<UnifiedFunctionDescriptor> functions = repository.loadFunctions();

    assertTrue("Should load at least one function", functions.size() >= 1);
    for (UnifiedFunctionDescriptor descriptor : functions) {
      assertNotNull("Function name should not be null", descriptor.getFunctionName());
      assertNotNull("Builder should not be null", descriptor.getBuilder());
    }
  }

  @Test
  public void testLoadSpecificFunction() {
    UnifiedFunctionDescriptor jsonFunc = repository.loadFunction("json").orElseThrow();

    assertEquals("JSON", jsonFunc.getFunctionName());
    assertNotNull("Builder should be present", jsonFunc.getBuilder());
  }

  @Test
  public void testLoadSpecificFunctionCaseInsensitive() {
    UnifiedFunctionDescriptor upperCase = repository.loadFunction("JSON").orElseThrow();
    UnifiedFunctionDescriptor lowerCase = repository.loadFunction("json").orElseThrow();
    UnifiedFunctionDescriptor mixedCase = repository.loadFunction("Json").orElseThrow();

    assertEquals("JSON", upperCase.getFunctionName());
    assertEquals("JSON", lowerCase.getFunctionName());
    assertEquals("JSON", mixedCase.getFunctionName());
  }

  @Test
  public void testLoadNonExistentFunctionReturnsEmpty() {
    assertTrue(
        "Non-existent function should return empty Optional",
        repository.loadFunction("NON_EXISTENT_FUNCTION").isEmpty());
  }

  @Test
  public void testFunctionBuilderCreatesValidFunction() {
    UnifiedFunctionDescriptor descriptor =
        repository.loadFunctions().stream()
            .filter(d -> d.getFunctionName().equalsIgnoreCase("json"))
            .findFirst()
            .orElseThrow();
    UnifiedFunction jsonFunc = descriptor.getBuilder().build(List.of("VARCHAR"));

    assertNotNull("Function should be created", jsonFunc);
    assertTrue("Function name should be JSON", jsonFunc.getFunctionName().equalsIgnoreCase("JSON"));
  }
}
