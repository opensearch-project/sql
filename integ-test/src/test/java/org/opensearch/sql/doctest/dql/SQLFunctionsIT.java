/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

/*
 *   Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package org.opensearch.sql.doctest.dql;

import static org.opensearch.sql.legacy.antlr.semantic.types.TypeExpression.TypeExpressionSpec;

import org.opensearch.sql.doctest.core.DocTest;
import org.opensearch.sql.doctest.core.annotation.DocTestConfig;
import org.opensearch.sql.doctest.core.annotation.Section;
import org.opensearch.sql.legacy.antlr.semantic.types.function.ScalarFunction;
import org.opensearch.sql.legacy.utils.StringUtils;

@DocTestConfig(template = "dql/functions.rst")
public class SQLFunctionsIT extends DocTest {

  /**
   * List only specifications of all SQL functions supported for now
   */
  @Section
  public void listFunctions() {
    for (ScalarFunction func : ScalarFunction
        .values()) { // Java Enum.values() return enums in order they are defined
      section(
          title(func.getName()),
          description(listFunctionSpecs(func))
      );
    }
  }

  private String listFunctionSpecs(ScalarFunction func) {
    TypeExpressionSpec[] specs = func.specifications();
    if (specs.length == 0) {
      return "Specification is undefined and type check is skipped for now";
    }

    StringBuilder specStr = new StringBuilder("Specifications: \n\n");
    for (int i = 0; i < specs.length; i++) {
      specStr.append(
          StringUtils.format("%d. %s%s\n", (i + 1), func.getName(), specs[i])
      );
    }
    return specStr.toString();
  }
}
