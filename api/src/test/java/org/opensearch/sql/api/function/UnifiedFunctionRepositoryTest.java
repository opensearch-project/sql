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
      assertNotNull("Identifier should not be null", descriptor.getIdentifier());
      assertFalse("Identifier should not be empty", descriptor.getIdentifier().isEmpty());
      assertNotNull("Expression info should not be null", descriptor.getExpressionInfo());
      assertNotNull("Builder should not be null", descriptor.getBuilder());
    }
  }

  @Test
  public void testBuilderWithSingleInputType() {
    FunctionDescriptor jsonFunc = repository.loadFunction("json");

    assertEquals("json", jsonFunc.getIdentifier());
    assertEquals("JSON(...) -> DYNAMIC", jsonFunc.getExpressionInfo());
    assertNotNull("Builder should be present", jsonFunc.getBuilder());
  }

  @Test
  public void testBuilderWithMultipleInputTypes() {
    FunctionDescriptor modFunc = repository.loadFunction("mod");

    assertEquals("mod", modFunc.getIdentifier());
    assertEquals("MOD('MOD(<NUMERIC>, <NUMERIC>)') -> DYNAMIC", modFunc.getExpressionInfo());
    assertNotNull("Builder should be present", modFunc.getBuilder());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testLoadFunctionThrowsForNonExistentFunction() {
    repository.loadFunction("nonexistent_function");
  }
}
