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

package org.opensearch.sql.calcite.udf.udaf;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlSplittableAggFunction;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeTransforms;
import org.apache.calcite.util.Optionality;
import org.checkerframework.checker.nullness.qual.Nullable;

public class NullableSqlSumAggFunction extends SqlAggFunction {

  // ~ Instance fields --------------------------------------------------------

  @Deprecated // to be removed before 2.0
  private final RelDataType type;

  // ~ Constructors -----------------------------------------------------------

  public NullableSqlSumAggFunction(RelDataType type) {
    super(
        "SUM",
        null,
        SqlKind.SUM,
        ReturnTypes.AGG_SUM.andThen(SqlTypeTransforms.FORCE_NULLABLE),
        null,
        OperandTypes.NUMERIC,
        SqlFunctionCategory.NUMERIC,
        false,
        false,
        Optionality.FORBIDDEN);
    this.type = type;
  }

  // ~ Methods ----------------------------------------------------------------

  @SuppressWarnings("deprecation")
  @Override
  public List<RelDataType> getParameterTypes(RelDataTypeFactory typeFactory) {
    return ImmutableList.of(type);
  }

  @Deprecated // to be removed before 2.0
  public RelDataType getType() {
    return type;
  }

  @SuppressWarnings("deprecation")
  @Override
  public RelDataType getReturnType(RelDataTypeFactory typeFactory) {
    return type;
  }

  @Override
  public <T extends Object> @Nullable T unwrap(Class<T> clazz) {
    if (clazz.isInstance(SqlSplittableAggFunction.SumSplitter.INSTANCE)) {
      return clazz.cast(SqlSplittableAggFunction.SumSplitter.INSTANCE);
    }
    return super.unwrap(clazz);
  }

  @Override
  public SqlAggFunction getRollup() {
    return this;
  }
}
