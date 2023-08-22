/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.antlr.semantic.types;

import static java.util.Collections.singletonList;
import static org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType.BOOLEAN;
import static org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType.INTEGER;
import static org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType.KEYWORD;
import static org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType.NUMBER;
import static org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType.STRING;

import java.util.Arrays;
import org.junit.Assert;
import org.junit.Test;
import org.opensearch.sql.legacy.antlr.semantic.types.special.Product;

/** Test cases fro product type */
public class ProductTypeTest {

  @Test
  public void singleSameTypeInTwoProductsShouldPass() {
    Product product1 = new Product(singletonList(INTEGER));
    Product product2 = new Product(singletonList(INTEGER));
    Assert.assertTrue(product1.isCompatible(product2));
    Assert.assertTrue(product2.isCompatible(product1));
  }

  @Test
  public void singleCompatibleTypeInTwoProductsShouldPass() {
    Product product1 = new Product(singletonList(NUMBER));
    Product product2 = new Product(singletonList(INTEGER));
    Assert.assertTrue(product1.isCompatible(product2));
    Assert.assertTrue(product2.isCompatible(product1));
  }

  @Test
  public void twoCompatibleTypesInTwoProductsShouldPass() {
    Product product1 = new Product(Arrays.asList(NUMBER, KEYWORD));
    Product product2 = new Product(Arrays.asList(INTEGER, STRING));
    Assert.assertTrue(product1.isCompatible(product2));
    Assert.assertTrue(product2.isCompatible(product1));
  }

  @Test
  public void incompatibleTypesInTwoProductsShouldFail() {
    Product product1 = new Product(singletonList(BOOLEAN));
    Product product2 = new Product(singletonList(STRING));
    Assert.assertFalse(product1.isCompatible(product2));
    Assert.assertFalse(product2.isCompatible(product1));
  }

  @Test
  public void compatibleButDifferentTypeNumberInTwoProductsShouldFail() {
    Product product1 = new Product(Arrays.asList(KEYWORD, INTEGER));
    Product product2 = new Product(singletonList(STRING));
    Assert.assertFalse(product1.isCompatible(product2));
    Assert.assertFalse(product2.isCompatible(product1));
  }

  @Test
  public void baseTypeShouldBeIncompatibleWithProductType() {
    Product product = new Product(singletonList(INTEGER));
    Assert.assertFalse(INTEGER.isCompatible(product));
    Assert.assertFalse(product.isCompatible(INTEGER));
  }
}
