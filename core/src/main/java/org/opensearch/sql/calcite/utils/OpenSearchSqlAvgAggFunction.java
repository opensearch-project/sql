/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

/*
 * This file contains code from the Apache Calcite project (original license below).
 * It contains modifications, which are licensed as above:
 */

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opensearch.sql.calcite.utils;

import static com.google.common.base.Preconditions.checkArgument;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlAvgAggFunction;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeTransforms;
import org.apache.calcite.util.Optionality;

public class OpenSearchSqlAvgAggFunction extends SqlAggFunction {

  // ~ Constructors -----------------------------------------------------------

  /** Creates a SqlAvgAggFunction. */
  public OpenSearchSqlAvgAggFunction(SqlKind kind) {
    this(kind.name(), kind);
  }

  OpenSearchSqlAvgAggFunction(String name, SqlKind kind) {
    super(
        name,
        null,
        kind,
        AVG_AGG_NULLABLE.andThen(SqlTypeTransforms.FORCE_NULLABLE),
        null,
        OperandTypes.NUMERIC,
        SqlFunctionCategory.NUMERIC,
        false,
        false,
        Optionality.FORBIDDEN);
    checkArgument(SqlKind.AVG_AGG_FUNCTIONS.contains(kind), "unsupported sql kind");
  }

  // ~ Methods ----------------------------------------------------------------

  /**
   * Returns the specific function, e.g. AVG or STDDEV_POP.
   *
   * @return Subtype
   */
  @Deprecated // to be removed before 2.0
  public SqlAvgAggFunction.Subtype getSubtype() {
    return SqlAvgAggFunction.Subtype.valueOf(kind.name());
  }

  /** Sub-type of aggregate function. */
  @Deprecated // to be removed before 2.0
  public enum Subtype {
    AVG,
    STDDEV_POP,
    STDDEV_SAMP,
    VAR_POP,
    VAR_SAMP
  }

  public static final SqlReturnTypeInference AVG_AGG_NULLABLE =
      opBinding -> {
        final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
        final RelDataType relDataType =
            typeFactory.getTypeSystem().deriveAvgAggType(typeFactory, opBinding.getOperandType(0));
        if (opBinding.getGroupCount() > 0
            || opBinding.hasFilter()
            || opBinding.getOperator().kind == SqlKind.STDDEV_SAMP) {
          return typeFactory.createTypeWithNullability(relDataType, true);
        } else {
          return relDataType;
        }
      };
}
