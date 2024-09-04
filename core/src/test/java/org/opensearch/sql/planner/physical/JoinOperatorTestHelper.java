/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical;

import static org.opensearch.sql.data.type.ExprCoreType.DATE;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import org.opensearch.sql.ast.tree.Join;
import org.opensearch.sql.data.model.ExprDateValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.planner.physical.join.HashJoinOperator;
import org.opensearch.sql.planner.physical.join.JoinOperator;
import org.opensearch.sql.planner.physical.join.NestedLoopJoinOperator;

public class JoinOperatorTestHelper extends PhysicalPlanTestBase {

  private final List<ExprValue> errorInputs =
      new ImmutableList.Builder<ExprValue>()
          .add(
              ExprValueUtils.tupleValue(
                  ImmutableMap.of(
                      "day", new ExprDateValue("2021-01-03"), "host", "h1", "errors", 2)))
          .add(
              ExprValueUtils.tupleValue(
                  ImmutableMap.of(
                      "day", new ExprDateValue("2021-01-03"), "host", "h2", "errors", 3)))
          .add(
              ExprValueUtils.tupleValue(
                  ImmutableMap.of(
                      "day", new ExprDateValue("2021-01-04"), "host", "h1", "errors", 1)))
          .add(
              ExprValueUtils.tupleValue(
                  ImmutableMap.of(
                      "day", new ExprDateValue("2021-01-04"), "host", "h2", "errors", 10)))
          .add(
              ExprValueUtils.tupleValue(
                  ImmutableMap.of(
                      "day", new ExprDateValue("2021-01-06"), "host", "h1", "errors", 1)))
          .add(
              ExprValueUtils.tupleValue(
                  ImmutableMap.of(
                      "day", new ExprDateValue("2021-01-07"), "host", "h1", "errors", 6)))
          .add(
              ExprValueUtils.tupleValue(
                  ImmutableMap.of(
                      "day", new ExprDateValue("2021-01-07"), "host", "h2", "errors", 8)))
          .add(
              ExprValueUtils.tupleValue(
                  ImmutableMap.of(
                      "day", new ExprDateValue("2021-01-07"), "host", "h2", "errors", 12)))
          .add(
              ExprValueUtils.tupleValue(
                  ImmutableMap.of(
                      "day", new ExprDateValue("2021-01-08"), "host", "h1", "errors", 13)))
          .build();

  private final List<ExprValue> nameInputs =
      new ImmutableList.Builder<ExprValue>()
          .add(ExprValueUtils.tupleValue(ImmutableMap.of("id", 1, "name", "a")))
          .add(ExprValueUtils.tupleValue(ImmutableMap.of("id", 2, "name", "b")))
          .add(
              ExprValueUtils.tupleValue(
                  new LinkedHashMap<>() {
                    {
                      put("id", 3);
                      put("name", null);
                    }
                  }))
          .add(ExprValueUtils.tupleValue(ImmutableMap.of("id", 4, "name", "d")))
          .add(ExprValueUtils.tupleValue(ImmutableMap.of("id", 5, "name", "e")))
          .add(ExprValueUtils.tupleValue(ImmutableMap.of("id", 6, "name", "f")))
          .add(ExprValueUtils.tupleValue(ImmutableMap.of("id", 7, "name", "g")))
          .add(
              ExprValueUtils.tupleValue(
                  new LinkedHashMap<>() {
                    {
                      put("id", 8);
                      put("name", null);
                    }
                  }))
          .add(ExprValueUtils.tupleValue(ImmutableMap.of("id", 9, "name", "i")))
          .add(ExprValueUtils.tupleValue(ImmutableMap.of("id", 10, "name", "j")))
          .add(ExprValueUtils.tupleValue(ImmutableMap.of("id", 11, "name", "k")))
          .build();

  private final List<ExprValue> sameNameInputs =
      new ImmutableList.Builder<ExprValue>()
          .add(ExprValueUtils.tupleValue(ImmutableMap.of("id", 1, "name", "a")))
          .add(ExprValueUtils.tupleValue(ImmutableMap.of("id", 3, "name", "c")))
          .add(
              ExprValueUtils.tupleValue(
                  new LinkedHashMap<>() {
                    {
                      put("id", 5);
                      put("name", null);
                    }
                  }))
          .add(
              ExprValueUtils.tupleValue(
                  new LinkedHashMap<>() {
                    {
                      put("id", 8);
                      put("name", null);
                    }
                  }))
          .add(ExprValueUtils.tupleValue(ImmutableMap.of("id", 10, "name", "j")))
          .add(ExprValueUtils.tupleValue(ImmutableMap.of("id", 10, "name", "jj")))
          .add(ExprValueUtils.tupleValue(ImmutableMap.of("id", 10, "name", "jjj")))
          .add(ExprValueUtils.tupleValue(ImmutableMap.of("id", 15, "name", "o")))
          .add(
              ExprValueUtils.tupleValue(
                  new LinkedHashMap<>() {
                    {
                      put("id", 16);
                      put("name", null);
                    }
                  }))
          .add(ExprValueUtils.tupleValue(ImmutableMap.of("id", 17, "name", "q")))
          .build();

  private final ExecutionEngine.Schema errorSchema =
      new ExecutionEngine.Schema(
          List.of(
              new ExecutionEngine.Schema.Column("day", "error_t.day", DATE),
              new ExecutionEngine.Schema.Column("host", "error_t.host", STRING),
              new ExecutionEngine.Schema.Column("errors", "error_t.errors", INTEGER)));

  private final ExecutionEngine.Schema nameSchema =
      new ExecutionEngine.Schema(
          List.of(
              new ExecutionEngine.Schema.Column("id", "name_t.id", INTEGER),
              new ExecutionEngine.Schema.Column("name", "name_t.name", STRING)));

  private final ExecutionEngine.Schema sameNameSchema =
      new ExecutionEngine.Schema(
          List.of(
              new ExecutionEngine.Schema.Column("id", "name_t2.id", INTEGER),
              new ExecutionEngine.Schema.Column("name", "name_t2.name", STRING)));

  public PhysicalPlan makeNestedLoopJoin(
      Join.JoinType joinType, JoinOperator.BuildSide buildSide, boolean reversed) {
    PhysicalPlan left =
        reversed
            ? testTableScan("name_t", nameSchema, nameInputs)
            : testTableScan("error_t", errorSchema, errorInputs);
    PhysicalPlan right =
        reversed
            ? testTableScan("error_t", errorSchema, errorInputs)
            : testTableScan("name_t", nameSchema, nameInputs);
    return new NestedLoopJoinOperator(
        left,
        right,
        joinType,
        buildSide,
        DSL.equal(DSL.ref("error_t.errors", INTEGER), DSL.ref("name_t.id", INTEGER)));
  }

  public PhysicalPlan makeNestedLoopJoinWithSameColumnNames(
      Join.JoinType joinType, JoinOperator.BuildSide buildSide, boolean reversed) {
    PhysicalPlan left =
        reversed
            ? testTableScan("name_t2", sameNameSchema, sameNameInputs)
            : testTableScan("name_t", nameSchema, nameInputs);
    PhysicalPlan right =
        reversed
            ? testTableScan("name_t", nameSchema, nameInputs)
            : testTableScan("name_t2", sameNameSchema, sameNameInputs);
    return new NestedLoopJoinOperator(
        left,
        right,
        joinType,
        buildSide,
        DSL.equal(DSL.ref("name_t.id", INTEGER), DSL.ref("name_t2.id", INTEGER)));
  }

  public PhysicalPlan makeHashJoin(
      Join.JoinType joinType,
      JoinOperator.BuildSide buildSide,
      Optional<Expression> nonEquiCond,
      boolean reversed) {
    PhysicalPlan left =
        reversed
            ? testTableScan("name_t", nameSchema, nameInputs)
            : testTableScan("error_t", errorSchema, errorInputs);
    PhysicalPlan right =
        reversed
            ? testTableScan("error_t", errorSchema, errorInputs)
            : testTableScan("name_t", nameSchema, nameInputs);
    List<Expression> leftKeys =
        reversed
            ? ImmutableList.of(DSL.ref("id", INTEGER))
            : ImmutableList.of(DSL.ref("errors", INTEGER));

    List<Expression> rightKeys =
        reversed
            ? ImmutableList.of(DSL.ref("errors", INTEGER))
            : ImmutableList.of(DSL.ref("id", INTEGER));
    return new HashJoinOperator(leftKeys, rightKeys, joinType, buildSide, left, right, nonEquiCond);
  }

  public PhysicalPlan makeHashJoinWithSameColumnNames(
      Join.JoinType joinType,
      JoinOperator.BuildSide buildSide,
      Optional<Expression> nonEquiCond,
      boolean reversed) {
    PhysicalPlan left =
        reversed
            ? testTableScan("name_t", nameSchema, nameInputs)
            : testTableScan("name_t2", sameNameSchema, sameNameInputs);
    PhysicalPlan right =
        reversed
            ? testTableScan("name_t2", sameNameSchema, sameNameInputs)
            : testTableScan("name_t", nameSchema, nameInputs);
    List<Expression> leftKeys = ImmutableList.of(DSL.ref("id", INTEGER));

    List<Expression> rightKeys = ImmutableList.of(DSL.ref("id", INTEGER));
    return new HashJoinOperator(leftKeys, rightKeys, joinType, buildSide, left, right, nonEquiCond);
  }

  /** {day:DATE '2021-01-04',host:"h1",errors:1,id:1,name:"a"} */
  protected ExprValue error1_id1 =
      ExprValueUtils.tupleValue(
          ImmutableMap.of(
              "error_t.day",
              new ExprDateValue("2021-01-04"),
              "error_t.host",
              "h1",
              "error_t.errors",
              1,
              "name_t.id",
              1,
              "name_t.name",
              "a"));

  /** {day:DATE '2021-01-06',host:"h1",errors:1,id:1,name:"a"} */
  protected ExprValue error1_id1_duplicated =
      ExprValueUtils.tupleValue(
          ImmutableMap.of(
              "error_t.day",
              new ExprDateValue("2021-01-06"),
              "error_t.host",
              "h1",
              "error_t.errors",
              1,
              "name_t.id",
              1,
              "name_t.name",
              "a"));

  /** {day:DATE '2021-01-03',host:"h1",errors:2,id:2,name:"b"} */
  protected ExprValue error2_id2 =
      ExprValueUtils.tupleValue(
          ImmutableMap.of(
              "error_t.day",
              new ExprDateValue("2021-01-03"),
              "error_t.host",
              "h1",
              "error_t.errors",
              2,
              "name_t.id",
              2,
              "name_t.name",
              "b"));

  /** {day:DATE '2021-01-03',host:"h2",errors:3,id:3,name:NULL} */
  protected ExprValue error3_id3 =
      ExprValueUtils.tupleValue(
          new LinkedHashMap<>() {
            {
              put("error_t.day", new ExprDateValue("2021-01-03"));
              put("error_t.host", "h2");
              put("error_t.errors", 3);
              put("name_t.id", 3);
              put("name_t.name", null);
            }
          });

  /** {day:DATE '2021-01-03',host:"h2",errors:3,id:NULL,name:NULL} */
  protected ExprValue error3_null =
      ExprValueUtils.tupleValue(
          new LinkedHashMap<>() {
            {
              put("error_t.day", new ExprDateValue("2021-01-03"));
              put("error_t.host", "h2");
              put("error_t.errors", 3);
              put("name_t.id", null);
              put("name_t.name", null);
            }
          });

  /** {day:DATE '2021-01-07',host:"h1",errors:6,id:6,name:"f"} */
  protected ExprValue error6_id6 =
      ExprValueUtils.tupleValue(
          ImmutableMap.of(
              "error_t.day",
              new ExprDateValue("2021-01-07"),
              "error_t.host",
              "h1",
              "error_t.errors",
              6,
              "name_t.id",
              6,
              "name_t.name",
              "f"));

  /** {day:DATE '2021-01-07',host:"h1",errors:6,id:NULL,name:NULL} */
  protected ExprValue error6_null =
      ExprValueUtils.tupleValue(
          new LinkedHashMap<>() {
            {
              put("error_t.day", new ExprDateValue("2021-01-07"));
              put("error_t.host", "h1");
              put("error_t.errors", 6);
              put("name_t.id", null);
              put("name_t.name", null);
            }
          });

  /** {day:DATE '2021-01-07',host:"h2",errors:8,id:8,name:NULL} */
  protected ExprValue error8_id8 =
      ExprValueUtils.tupleValue(
          new LinkedHashMap<>() {
            {
              put("error_t.day", new ExprDateValue("2021-01-07"));
              put("error_t.host", "h2");
              put("error_t.errors", 8);
              put("name_t.id", 8);
              put("name_t.name", null);
            }
          });

  /** {day:DATE '2021-01-07',host:"h2",errors:8,id:NULL,name:NULL} */
  protected ExprValue error8_null =
      ExprValueUtils.tupleValue(
          new LinkedHashMap<>() {
            {
              put("error_t.day", new ExprDateValue("2021-01-07"));
              put("error_t.host", "h2");
              put("error_t.errors", 8);
              put("name_t.id", null);
              put("name_t.name", null);
            }
          });

  /** {day:DATE '2021-01-04',host:"h2",errors:10,id:10,name:"j"} */
  protected ExprValue error10_id10 =
      ExprValueUtils.tupleValue(
          ImmutableMap.of(
              "error_t.day",
              new ExprDateValue("2021-01-04"),
              "error_t.host",
              "h2",
              "error_t.errors",
              10,
              "name_t.id",
              10,
              "name_t.name",
              "j"));

  /** {day:DATE '2021-01-04',host:"h2",errors:10,id:NULL,name:NULL} */
  protected ExprValue error10_null =
      ExprValueUtils.tupleValue(
          new LinkedHashMap<>() {
            {
              put("error_t.day", new ExprDateValue("2021-01-04"));
              put("error_t.host", "h2");
              put("error_t.errors", 10);
              put("name_t.id", null);
              put("name_t.name", null);
            }
          });

  /** {day:DATE '2021-01-07',host:"h2",errors:12,id:NULL,name:NULL} */
  protected ExprValue error12_null =
      ExprValueUtils.tupleValue(
          new LinkedHashMap<>() {
            {
              put("error_t.day", new ExprDateValue("2021-01-07"));
              put("error_t.host", "h2");
              put("error_t.errors", 12);
              put("name_t.id", null);
              put("name_t.name", null);
            }
          });

  /** {day:DATE '2021-01-08',host:"h1",errors:13,id:NULL,name:NULL} */
  protected ExprValue error13_null =
      ExprValueUtils.tupleValue(
          new LinkedHashMap<>() {
            {
              put("error_t.day", new ExprDateValue("2021-01-08"));
              put("error_t.host", "h1");
              put("error_t.errors", 13);
              put("name_t.id", null);
              put("name_t.name", null);
            }
          });

  /** {id:1,name:"a",day:DATE '2021-01-04',host:"h1",errors:1} */
  protected ExprValue id1_error1 =
      ExprValueUtils.tupleValue(
          ImmutableMap.of(
              "name_t.id",
              1,
              "name_t.name",
              "a",
              "error_t.day",
              new ExprDateValue("2021-01-04"),
              "error_t.host",
              "h1",
              "error_t.errors",
              1));

  /** {id:1,name:"a",day:DATE '2021-01-06',host:"h1",errors:1} */
  protected ExprValue id1_error1_duplicated =
      ExprValueUtils.tupleValue(
          ImmutableMap.of(
              "name_t.id",
              1,
              "name_t.name",
              "a",
              "error_t.day",
              new ExprDateValue("2021-01-06"),
              "error_t.host",
              "h1",
              "error_t.errors",
              1));

  /** {id:2,name:"b",day:DATE '2021-01-03',host:"h1",errors:2} */
  protected ExprValue id2_error2 =
      ExprValueUtils.tupleValue(
          ImmutableMap.of(
              "name_t.id",
              2,
              "name_t.name",
              "b",
              "error_t.day",
              new ExprDateValue("2021-01-03"),
              "error_t.host",
              "h1",
              "error_t.errors",
              2));

  /** {id:3,name:NULL,day:DATE '2021-01-03',host:"h2",errors:3} */
  protected ExprValue id3_error3 =
      ExprValueUtils.tupleValue(
          new LinkedHashMap<>() {
            {
              put("name_t.id", 3);
              put("name_t.name", null);
              put("error_t.day", new ExprDateValue("2021-01-03"));
              put("error_t.host", "h2");
              put("error_t.errors", 3);
            }
          });

  /** {id:4,name:"d",day:NULL,host:NULL,errors:NULL} */
  protected ExprValue id4_null =
      ExprValueUtils.tupleValue(
          new LinkedHashMap<>() {
            {
              put("name_t.id", 4);
              put("name_t.name", "d");
              put("error_t.day", null);
              put("error_t.host", null);
              put("error_t.errors", null);
            }
          });

  /** {id:5,name:"e",day:NULL,host:NULL,errors:NULL} */
  protected ExprValue id5_null =
      ExprValueUtils.tupleValue(
          new LinkedHashMap<>() {
            {
              put("name_t.id", 5);
              put("name_t.name", "e");
              put("error_t.day", null);
              put("error_t.host", null);
              put("error_t.errors", null);
            }
          });

  /** {id:6,name:"f",day:DATE '2021-01-07',host:"h1",errors:6} */
  protected ExprValue id6_error6 =
      ExprValueUtils.tupleValue(
          ImmutableMap.of(
              "name_t.id",
              6,
              "name_t.name",
              "f",
              "error_t.day",
              new ExprDateValue("2021-01-07"),
              "error_t.host",
              "h1",
              "error_t.errors",
              6));

  /** {id:7,name:"g",day:NULL,host:NULL,errors:NULL} */
  protected ExprValue id7_null =
      ExprValueUtils.tupleValue(
          new LinkedHashMap<>() {
            {
              put("name_t.id", 7);
              put("name_t.name", "g");
              put("error_t.day", null);
              put("error_t.host", null);
              put("error_t.errors", null);
            }
          });

  /** {id:8,name:NULL,day:DATE '2021-01-07',host:"h2",errors:8} */
  protected ExprValue id8_error8 =
      ExprValueUtils.tupleValue(
          new LinkedHashMap<>() {
            {
              put("name_t.id", 8);
              put("name_t.name", null);
              put("error_t.day", new ExprDateValue("2021-01-07"));
              put("error_t.host", "h2");
              put("error_t.errors", 8);
            }
          });

  /** {id:9,name:"i",day:NULL,host:NULL,errors:NULL} */
  protected ExprValue id9_null =
      ExprValueUtils.tupleValue(
          new LinkedHashMap<>() {
            {
              put("name_t.id", 9);
              put("name_t.name", "i");
              put("error_t.day", null);
              put("error_t.host", null);
              put("error_t.errors", null);
            }
          });

  /** {id:10,name:"j",day:DATE '2021-01-04',host:"h2",errors:10} */
  protected ExprValue id10_error10 =
      ExprValueUtils.tupleValue(
          ImmutableMap.of(
              "name_t.id",
              10,
              "name_t.name",
              "j",
              "error_t.day",
              new ExprDateValue("2021-01-04"),
              "error_t.host",
              "h2",
              "error_t.errors",
              10));

  /** {id:11,name:"k",day:NULL,host:NULL,errors:NULL} */
  protected ExprValue id11_null =
      ExprValueUtils.tupleValue(
          new LinkedHashMap<>() {
            {
              put("name_t.id", 11);
              put("name_t.name", "k");
              put("error_t.day", null);
              put("error_t.host", null);
              put("error_t.errors", null);
            }
          });

  /** {day:NULL,host:NULL,errors:NULL,id:3,name:NULL} */
  protected ExprValue null_id3 =
      ExprValueUtils.tupleValue(
          new LinkedHashMap<>() {
            {
              put("error_t.day", null);
              put("error_t.host", null);
              put("error_t.errors", null);
              put("name_t.id", 3);
              put("name_t.name", null);
            }
          });

  /** {day:NULL,host:NULL,errors:NULL,id:4,name:"d"} */
  protected ExprValue null_id4 =
      ExprValueUtils.tupleValue(
          new LinkedHashMap<>() {
            {
              put("error_t.day", null);
              put("error_t.host", null);
              put("error_t.errors", null);
              put("name_t.id", 4);
              put("name_t.name", "d");
            }
          });

  /** {day:NULL,host:NULL,errors:NULL,id:5,name:"e"} */
  protected ExprValue null_id5 =
      ExprValueUtils.tupleValue(
          new LinkedHashMap<>() {
            {
              put("error_t.day", null);
              put("error_t.host", null);
              put("error_t.errors", null);
              put("name_t.id", 5);
              put("name_t.name", "e");
            }
          });

  /** {day:NULL,host:NULL,errors:NULL,id:6,name:"f"} */
  protected ExprValue null_id6 =
      ExprValueUtils.tupleValue(
          new LinkedHashMap<>() {
            {
              put("error_t.day", null);
              put("error_t.host", null);
              put("error_t.errors", null);
              put("name_t.id", 6);
              put("name_t.name", "f");
            }
          });

  /** {day:NULL,host:NULL,errors:NULL,id:7,name:"g"} */
  protected ExprValue null_id7 =
      ExprValueUtils.tupleValue(
          new LinkedHashMap<>() {
            {
              put("error_t.day", null);
              put("error_t.host", null);
              put("error_t.errors", null);
              put("name_t.id", 7);
              put("name_t.name", "g");
            }
          });

  /** {day:NULL,host:NULL,errors:NULL,id:8,name:NULL} */
  protected ExprValue null_id8 =
      ExprValueUtils.tupleValue(
          new LinkedHashMap<>() {
            {
              put("error_t.day", null);
              put("error_t.host", null);
              put("error_t.errors", null);
              put("name_t.id", 8);
              put("name_t.name", null);
            }
          });

  /** {day:NULL,host:NULL,errors:NULL,id:9,name:"i"} */
  protected ExprValue null_id9 =
      ExprValueUtils.tupleValue(
          new LinkedHashMap<>() {
            {
              put("error_t.day", null);
              put("error_t.host", null);
              put("error_t.errors", null);
              put("name_t.id", 9);
              put("name_t.name", "i");
            }
          });

  /** {day:NULL,host:NULL,errors:NULL,id:10,name:"j"} */
  protected ExprValue null_id10 =
      ExprValueUtils.tupleValue(
          new LinkedHashMap<>() {
            {
              put("error_t.day", null);
              put("error_t.host", null);
              put("error_t.errors", null);
              put("name_t.id", 10);
              put("name_t.name", "j");
            }
          });

  /** {day:NULL,host:NULL,errors:NULL,id:11,name:"k"} */
  protected ExprValue null_id11 =
      ExprValueUtils.tupleValue(
          new LinkedHashMap<>() {
            {
              put("error_t.day", null);
              put("error_t.host", null);
              put("error_t.errors", null);
              put("name_t.id", 11);
              put("name_t.name", "k");
            }
          });

  /** {id:NULL,name:NULL,day:DATE '2021-01-07',host:"h2",errors:12} */
  protected ExprValue null_error12 =
      ExprValueUtils.tupleValue(
          new LinkedHashMap<>() {
            {
              put("name_t.id", null);
              put("name_t.name", null);
              put("error_t.day", new ExprDateValue("2021-01-07"));
              put("error_t.host", "h2");
              put("error_t.errors", 12);
            }
          });

  /** {id:NULL,name:NULL,day:DATE '2021-01-08',host:"h1",errors:13} */
  protected ExprValue null_error13 =
      ExprValueUtils.tupleValue(
          new LinkedHashMap<>() {
            {
              put("name_t.id", null);
              put("name_t.name", null);
              put("error_t.day", new ExprDateValue("2021-01-08"));
              put("error_t.host", "h1");
              put("error_t.errors", 13);
            }
          });

  /** {name_t.id:1,name_t.name:"a",name_t2.id:1,name_t2.name:"a"} */
  ExprValue id1_same_id1 =
      ExprValueUtils.tupleValue(
          ImmutableMap.of(
              "name_t.id", 1, "name_t.name", "a", "name_t2.id", 1, "name_t2.name", "a"));

  /** {name_t.id:3,name_t.name:NULL,name_t2.id:3,name_t2.name:"c"} */
  ExprValue id3_same_id3 =
      ExprValueUtils.tupleValue(
          new LinkedHashMap<>() {
            {
              put("name_t.id", 3);
              put("name_t.name", null);
              put("name_t2.id", 3);
              put("name_t2.name", "c");
            }
          });

  /** {name_t.id:5,name_t.name:"e",name_t2.id:5,name_t2.name:NULL} */
  ExprValue id5_same_id5 =
      ExprValueUtils.tupleValue(
          new LinkedHashMap<>() {
            {
              put("name_t.id", 5);
              put("name_t.name", "e");
              put("name_t2.id", 5);
              put("name_t2.name", null);
            }
          });

  /** {name_t.id:8,name_t.name:NULL,name_t2.id:8,name_t2.name:NULL} */
  ExprValue id8_same_id8 =
      ExprValueUtils.tupleValue(
          new LinkedHashMap<>() {
            {
              put("name_t.id", 8);
              put("name_t.name", null);
              put("name_t2.id", 8);
              put("name_t2.name", null);
            }
          });

  /** {name_t.id:10,name_t.name:"j",name_t2.id:10,name_t2.name:"j"} */
  ExprValue id10_same_id10 =
      ExprValueUtils.tupleValue(
          ImmutableMap.of(
              "name_t.id", 10, "name_t.name", "j", "name_t2.id", 10, "name_t2.name", "j"));

  /** {name_t.id:10,name_t.name:"j",name_t2.id:10,name_t2.name:"jj"} */
  ExprValue id10_same_id10_duplicated =
      ExprValueUtils.tupleValue(
          ImmutableMap.of(
              "name_t.id", 10, "name_t.name", "j", "name_t2.id", 10, "name_t2.name", "jj"));

  /** {name_t.id:10,name_t.name:"j",name_t2.id:10,name_t2.name:"jjj"} */
  ExprValue id10_same_id10_duplicated2 =
      ExprValueUtils.tupleValue(
          ImmutableMap.of(
              "name_t.id", 10, "name_t.name", "j", "name_t2.id", 10, "name_t2.name", "jjj"));
}
