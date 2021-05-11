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
 *   Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package org.opensearch.sql.utils;

import static org.opensearch.sql.data.model.ExprValueUtils.getDoubleValue;
import static org.opensearch.sql.data.model.ExprValueUtils.getFloatValue;
import static org.opensearch.sql.data.model.ExprValueUtils.getIntegerValue;
import static org.opensearch.sql.data.model.ExprValueUtils.getLongValue;
import static org.opensearch.sql.data.model.ExprValueUtils.getStringValue;

import org.opensearch.sql.data.model.ExprByteValue;
import org.opensearch.sql.data.model.ExprDoubleValue;
import org.opensearch.sql.data.model.ExprFloatValue;
import org.opensearch.sql.data.model.ExprIntegerValue;
import org.opensearch.sql.data.model.ExprLongValue;
import org.opensearch.sql.data.model.ExprShortValue;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.exception.ExpressionEvaluationException;

public class ComparisonUtil {
  /**
   * Util to compare the object (integer, long, float, double, string) values.
   * ExprValue A
   */
  public static int compare(ExprValue v1, ExprValue v2) {
    if (v1.isMissing() || v2.isMissing()) {
      throw new ExpressionEvaluationException("invalid to call compare operation on missing value");
    } else if (v1.isNull() || v2.isNull()) {
      throw new ExpressionEvaluationException("invalid to call compare operation on null value");
    }

    if (v1 instanceof ExprByteValue) {
      return v1.byteValue().compareTo(v2.byteValue());
    } else if (v1 instanceof ExprShortValue) {
      return v1.shortValue().compareTo(v2.shortValue());
    } else if (v1 instanceof ExprIntegerValue) {
      return getIntegerValue(v1).compareTo(getIntegerValue(v2));
    } else if (v1 instanceof ExprLongValue) {
      return getLongValue(v1).compareTo(getLongValue(v2));
    } else if (v1 instanceof ExprFloatValue) {
      return getFloatValue(v1).compareTo(getFloatValue(v2));
    } else if (v1 instanceof ExprDoubleValue) {
      return getDoubleValue(v1).compareTo(getDoubleValue(v2));
    } else if (v1 instanceof ExprStringValue) {
      return getStringValue(v1).compareTo(getStringValue(v2));
    } else {
      throw new ExpressionEvaluationException(
          String.format("%s instances are not comparable", v1.getClass().getSimpleName()));
    }
  }
}
