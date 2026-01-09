/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.sql.api.UnifiedQueryTestBase;
import org.opensearch.sql.api.function.UnifiedFunctionRepository.FunctionDescriptor;

/** Unit tests for {@link UnifiedFunctionRepository}. */
public class UnifiedFunctionRepositoryTest extends UnifiedQueryTestBase {

  private UnifiedFunctionRepository repository;

  @Before
  @Override
  public void setUp() {
    super.setUp();
    repository = new UnifiedFunctionRepository(context);
  }

  @Test
  public void testLoadFunctionsReturnsAtLeastOneFunction() {
    List<FunctionDescriptor> functions = repository.loadFunctions();

    assertTrue("Should load at least one function", functions.size() >= 1);
  }

  @Test
  public void testLoadedFunctionDescriptorsAreValid() {
    List<FunctionDescriptor> functions = repository.loadFunctions();

    for (FunctionDescriptor descriptor : functions) {
      assertNotNull("Function name should not be null", descriptor.getFunctionName());
      assertFalse("Function name should not be empty", descriptor.getFunctionName().isEmpty());
      assertNotNull("Builder should not be null", descriptor.getBuilder());
    }
  }

  @Test
  public void testLoadSpecificFunction() {
    FunctionDescriptor jsonFunc = repository.loadFunction("json");

    assertEquals("JSON", jsonFunc.getFunctionName());
    assertNotNull("Builder should be present", jsonFunc.getBuilder());
  }

  @Test
  public void testLoadSpecificFunctionCaseInsensitive() {
    FunctionDescriptor modFunc = repository.loadFunction("MOD");

    assertEquals("MOD", modFunc.getFunctionName());
    assertNotNull("Builder should be present", modFunc.getBuilder());
  }

  @Test
  public void testBuilderCreatesValidFunction() {
    FunctionDescriptor descriptor = repository.loadFunction("json");

    UnifiedFunction jsonFunc = descriptor.getBuilder().build(List.of("VARCHAR"));

    assertNotNull("Function should be created", jsonFunc);
    assertEquals("JSON", jsonFunc.getFunctionName());
    assertEquals(List.of("VARCHAR"), jsonFunc.getInputTypes());
    assertEquals("VARCHAR", jsonFunc.getReturnType());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testLoadFunctionThrowsForNonExistentFunction() {
    repository.loadFunction("nonexistent_function");
  }
}
