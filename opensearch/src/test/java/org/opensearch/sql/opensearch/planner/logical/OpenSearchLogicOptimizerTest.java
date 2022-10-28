/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.planner.logical;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.opensearch.sql.data.model.ExprValueUtils.integerValue;
import static org.opensearch.sql.data.type.ExprCoreType.DOUBLE;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.LONG;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.opensearch.utils.Utils.indexScan;
import static org.opensearch.sql.opensearch.utils.Utils.indexScanAgg;
import static org.opensearch.sql.opensearch.utils.Utils.noProjects;
import static org.opensearch.sql.opensearch.utils.Utils.projects;
import static org.opensearch.sql.planner.logical.LogicalPlanDSL.aggregation;
import static org.opensearch.sql.planner.logical.LogicalPlanDSL.filter;
import static org.opensearch.sql.planner.logical.LogicalPlanDSL.limit;
import static org.opensearch.sql.planner.logical.LogicalPlanDSL.project;
import static org.opensearch.sql.planner.logical.LogicalPlanDSL.relation;
import static org.opensearch.sql.planner.logical.LogicalPlanDSL.sort;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.ast.tree.Sort;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.config.ExpressionConfig;
import org.opensearch.sql.opensearch.OpenSearchTestBase;
import org.opensearch.sql.opensearch.utils.Utils;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.optimizer.LogicalPlanOptimizer;
import org.opensearch.sql.storage.Table;

@ExtendWith(MockitoExtension.class)
class OpenSearchLogicOptimizerTest extends OpenSearchTestBase {

  @Mock
  private Table table;

  /**
   * SELECT intV as i FROM schema WHERE intV = 1.
   */
  @Test
  void project_filter_merge_with_relation() {
    assertEquals(
        project(
            indexScan("schema",
                dsl.equal(DSL.ref("intV", INTEGER), DSL.literal(integerValue(1))),
                ImmutableSet.of(DSL.ref("intV", INTEGER))),
            DSL.named("i", DSL.ref("intV", INTEGER))
        ),
        optimize(
            project(
                filter(
                    relation("schema", table),
                    dsl.equal(DSL.ref("intV", INTEGER), DSL.literal(integerValue(1)))
                ),
                DSL.named("i", DSL.ref("intV", INTEGER)))
        )
    );
  }

  /**
   * SELECT avg(intV) FROM schema GROUP BY string_value.
   */
  @Test
  void aggregation_merge_relation() {
    assertEquals(
        project(
            indexScanAgg("schema", ImmutableList
                    .of(DSL.named("AVG(intV)",
                        dsl.avg(DSL.ref("intV", INTEGER)))),
                ImmutableList.of(DSL.named("longV",
                    dsl.abs(DSL.ref("longV", LONG))))),
            DSL.named("AVG(intV)", DSL.ref("AVG(intV)", DOUBLE))),
        optimize(
            project(
                aggregation(
                    relation("schema", table),
                    ImmutableList
                        .of(DSL.named("AVG(intV)",
                            dsl.avg(DSL.ref("intV", INTEGER)))),
                    ImmutableList.of(DSL.named("longV",
                        dsl.abs(DSL.ref("longV", LONG))))),
                DSL.named("AVG(intV)", DSL.ref("AVG(intV)", DOUBLE)))
        )
    );
  }

  /**
   * SELECT avg(intV) FROM schema WHERE intV = 1 GROUP BY string_value.
   */
  @Test
  void aggregation_merge_filter_relation() {
    assertEquals(
        project(
            indexScanAgg("schema",
                dsl.equal(DSL.ref("intV", INTEGER), DSL.literal(integerValue(1))),
                ImmutableList
                    .of(DSL.named("AVG(intV)",
                        dsl.avg(DSL.ref("intV", INTEGER)))),
                ImmutableList.of(DSL.named("longV",
                    dsl.abs(DSL.ref("longV", LONG))))),
            DSL.named("AVG(intV)", DSL.ref("AVG(intV)", DOUBLE))),
        optimize(
            project(
                aggregation(
                    filter(
                        relation("schema", table),
                        dsl.equal(DSL.ref("intV", INTEGER), DSL.literal(integerValue(1)))
                    ),
                    ImmutableList
                        .of(DSL.named("AVG(intV)",
                            dsl.avg(DSL.ref("intV", INTEGER)))),
                    ImmutableList.of(DSL.named("longV",
                        dsl.abs(DSL.ref("longV", LONG))))),
                DSL.named("AVG(intV)", DSL.ref("AVG(intV)", DOUBLE)))
        )
    );
  }

  @Disabled
  @Test
  void aggregation_cant_merge_indexScan_with_project() {
    assertEquals(
        aggregation(
            OpenSearchLogicalIndexScan.builder().relationName("schema")
                .filter(dsl.equal(DSL.ref("intV", INTEGER), DSL.literal(integerValue(1))))
                .projectList(ImmutableSet.of(DSL.ref("intV", INTEGER)))
                .build(),
            ImmutableList
                .of(DSL.named("AVG(intV)",
                    dsl.avg(DSL.ref("intV", INTEGER)))),
            ImmutableList.of(DSL.named("longV",
                dsl.abs(DSL.ref("longV", LONG))))),
        optimize(
            aggregation(
                OpenSearchLogicalIndexScan.builder().relationName("schema")
                    .filter(dsl.equal(DSL.ref("intV", INTEGER), DSL.literal(integerValue(1))))
                    .projectList(
                        ImmutableSet.of(DSL.ref("intV", INTEGER)))
                    .build(),
                ImmutableList
                    .of(DSL.named("AVG(intV)",
                        dsl.avg(DSL.ref("intV", INTEGER)))),
                ImmutableList.of(DSL.named("longV",
                    dsl.abs(DSL.ref("longV", LONG))))))
    );
  }

  /**
   * Sort - Relation --> IndexScan.
   */
  @Test
  void sort_merge_with_relation() {
    assertEquals(
        indexScan("schema", Pair.of(Sort.SortOption.DEFAULT_ASC, DSL.ref("intV", INTEGER))),
        optimize(
            sort(
                relation("schema", table),
                Pair.of(Sort.SortOption.DEFAULT_ASC, DSL.ref("intV", INTEGER))
            )
        )
    );
  }

  /**
   * Sort - IndexScan --> IndexScan.
   */
  @Test
  void sort_merge_with_indexScan() {
    assertEquals(
        indexScan("schema",
            Pair.of(Sort.SortOption.DEFAULT_ASC, DSL.ref("intV", INTEGER)),
            Pair.of(Sort.SortOption.DEFAULT_ASC, DSL.ref("longV", LONG))),
        optimize(
            sort(
                indexScan("schema", Pair.of(Sort.SortOption.DEFAULT_ASC, DSL.ref("intV", INTEGER))),
                Pair.of(Sort.SortOption.DEFAULT_ASC, DSL.ref("longV", LONG))
            )
        )
    );
  }

  /**
   * Sort - Filter - Relation --> IndexScan.
   */
  @Test
  void sort_filter_merge_with_relation() {
    assertEquals(
        indexScan("schema",
            dsl.equal(DSL.ref("intV", INTEGER), DSL.literal(integerValue(1))),
            Pair.of(Sort.SortOption.DEFAULT_ASC, DSL.ref("longV", LONG))
        ),
        optimize(
            sort(
                filter(
                    relation("schema", table),
                    dsl.equal(DSL.ref("intV", INTEGER), DSL.literal(integerValue(1)))
                ),
                Pair.of(Sort.SortOption.DEFAULT_ASC, DSL.ref("longV", LONG))
            )
        )
    );
  }

  @Test
  void sort_with_expression_cannot_merge_with_relation() {
    assertEquals(
        sort(
            relation("schema", table),
            Pair.of(Sort.SortOption.DEFAULT_ASC, dsl.abs(DSL.ref("intV", INTEGER)))
        ),
        optimize(
            sort(
                relation("schema", table),
                Pair.of(Sort.SortOption.DEFAULT_ASC, dsl.abs(DSL.ref("intV", INTEGER)))
            )
        )
    );
  }

  /**
   * SELECT avg(intV) FROM schema GROUP BY stringV ORDER BY stringV.
   */
  @Test
  void sort_merge_indexagg() {
    assertEquals(
        project(
            indexScanAgg("schema",
                ImmutableList.of(DSL.named("AVG(intV)", dsl.avg(DSL.ref("intV", INTEGER)))),
                ImmutableList.of(DSL.named("stringV", DSL.ref("stringV", STRING))),
                ImmutableList
                    .of(Pair.of(Sort.SortOption.DEFAULT_ASC, DSL.ref("stringV", STRING)))),
            DSL.named("AVG(intV)", DSL.ref("AVG(intV)", DOUBLE))),
        optimize(
            project(
                sort(
                    aggregation(
                        relation("schema", table),
                        ImmutableList
                            .of(DSL.named("AVG(intV)", dsl.avg(DSL.ref("intV", INTEGER)))),
                        ImmutableList.of(DSL.named("stringV", DSL.ref("stringV", STRING)))),
                    Pair.of(Sort.SortOption.DEFAULT_ASC, DSL.ref("stringV", STRING))
                ),
                DSL.named("AVG(intV)", DSL.ref("AVG(intV)", DOUBLE)))
        )
    );
  }

  /**
   * SELECT avg(intV) FROM schema GROUP BY stringV ORDER BY stringV.
   */
  @Test
  void sort_merge_indexagg_nulls_last() {
    assertEquals(
        project(
            indexScanAgg("schema",
                ImmutableList.of(DSL.named("AVG(intV)", dsl.avg(DSL.ref("intV", INTEGER)))),
                ImmutableList.of(DSL.named("stringV", DSL.ref("stringV", STRING))),
                ImmutableList
                    .of(Pair.of(Sort.SortOption.DEFAULT_DESC, DSL.ref("stringV", STRING)))),
            DSL.named("AVG(intV)", DSL.ref("AVG(intV)", DOUBLE))),
        optimize(
            project(
                sort(
                    aggregation(
                        relation("schema", table),
                        ImmutableList
                            .of(DSL.named("AVG(intV)", dsl.avg(DSL.ref("intV", INTEGER)))),
                        ImmutableList.of(DSL.named("stringV", DSL.ref("stringV", STRING)))),
                    Pair.of(Sort.SortOption.DEFAULT_DESC, DSL.ref("stringV", STRING))
                ),
                DSL.named("AVG(intV)", DSL.ref("AVG(intV)", DOUBLE)))
        )
    );
  }


  /**
   * Can't Optimize the following query.
   * SELECT avg(intV) FROM schema GROUP BY stringV ORDER BY avg(intV).
   */
  @Test
  void sort_refer_to_aggregator_should_not_merge_with_indexAgg() {
    assertEquals(
        sort(
            indexScanAgg("schema",
                ImmutableList.of(DSL.named("AVG(intV)", dsl.avg(DSL.ref("intV", INTEGER)))),
                ImmutableList.of(DSL.named("stringV", DSL.ref("stringV", STRING)))),
            Pair.of(Sort.SortOption.DEFAULT_DESC, DSL.ref("AVG(intV)", INTEGER))
        ),
        optimize(
            sort(
                indexScanAgg("schema",
                    ImmutableList.of(DSL.named("AVG(intV)", dsl.avg(DSL.ref("intV", INTEGER)))),
                    ImmutableList.of(DSL.named("stringV", DSL.ref("stringV", STRING)))),
                Pair.of(Sort.SortOption.DEFAULT_DESC, DSL.ref("AVG(intV)", INTEGER))
            )
        )
    );
  }

  /**
   * SELECT avg(intV) FROM schema GROUP BY stringV ORDER BY stringV ASC NULL_LAST.
   */
  @Test
  void sort_with_customized_option_should_merge_with_indexAgg() {
    assertEquals(
        indexScanAgg(
            "schema",
            ImmutableList.of(DSL.named("AVG(intV)", dsl.avg(DSL.ref("intV", INTEGER)))),
            ImmutableList.of(DSL.named("stringV", DSL.ref("stringV", STRING))),
            ImmutableList.of(
                Pair.of(
                    new Sort.SortOption(Sort.SortOrder.ASC, Sort.NullOrder.NULL_LAST),
                    DSL.ref("stringV", STRING)))),
        optimize(
            sort(
                indexScanAgg(
                    "schema",
                    ImmutableList.of(DSL.named("AVG(intV)", dsl.avg(DSL.ref("intV", INTEGER)))),
                    ImmutableList.of(DSL.named("stringV", DSL.ref("stringV", STRING)))),
                Pair.of(
                    new Sort.SortOption(Sort.SortOrder.ASC, Sort.NullOrder.NULL_LAST),
                    DSL.ref("stringV", STRING)))));
  }

  @Test
  void limit_merge_with_relation() {
    assertEquals(
        project(
            indexScan("schema", 1, 1, projects(DSL.ref("intV", INTEGER))),
            DSL.named("intV", DSL.ref("intV", INTEGER))
        ),
        optimize(
            project(
                limit(
                    relation("schema", table),
                    1, 1
                ),
                DSL.named("intV", DSL.ref("intV", INTEGER))
            )
        )
    );
  }

  @Test
  void limit_merge_with_index_scan() {
    assertEquals(
        project(
            indexScan("schema",
                dsl.equal(DSL.ref("intV", INTEGER), DSL.literal(integerValue(1))),
                1, 1,
                projects(DSL.ref("intV", INTEGER))
            ),
            DSL.named("intV", DSL.ref("intV", INTEGER))
        ),
        optimize(
            project(
                limit(
                    filter(
                        relation("schema", table),
                        dsl.equal(DSL.ref("intV", INTEGER), DSL.literal(integerValue(1)))
                    ), 1, 1
                ),
            DSL.named("intV", DSL.ref("intV", INTEGER)))
        )
    );
  }

  @Test
  void limit_merge_with_index_scan_sort() {
    assertEquals(
        project(
            indexScan("schema",
                dsl.equal(DSL.ref("intV", INTEGER), DSL.literal(integerValue(1))),
                1, 1,
                Utils.sort(DSL.ref("longV", LONG), Sort.SortOption.DEFAULT_ASC),
                projects(DSL.ref("intV", INTEGER))
            ),
            DSL.named("intV", DSL.ref("intV", INTEGER))
        ),
        optimize(
            project(
                limit(
                    sort(
                        filter(
                            relation("schema", table),
                            dsl.equal(DSL.ref("intV", INTEGER), DSL.literal(integerValue(1)))
                        ),
                        Pair.of(Sort.SortOption.DEFAULT_ASC, DSL.ref("longV", LONG))
                    ), 1, 1
                ),
                DSL.named("intV", DSL.ref("intV", INTEGER))
            )
        )
    );
  }

  @Test
  void aggregation_cant_merge_index_scan_with_limit() {
    assertEquals(
        project(
            aggregation(
                indexScan("schema", 10, 0, noProjects()),
                ImmutableList
                    .of(DSL.named("AVG(intV)",
                        dsl.avg(DSL.ref("intV", INTEGER)))),
                ImmutableList.of(DSL.named("longV",
                    dsl.abs(DSL.ref("longV", LONG))))),
            DSL.named("AVG(intV)", DSL.ref("AVG(intV)", DOUBLE))),
        optimize(
            project(
                aggregation(
                    indexScan("schema", 10, 0, noProjects()),
                    ImmutableList
                        .of(DSL.named("AVG(intV)",
                            dsl.avg(DSL.ref("intV", INTEGER)))),
                    ImmutableList.of(DSL.named("longV",
                        dsl.abs(DSL.ref("longV", LONG))))),
                DSL.named("AVG(intV)", DSL.ref("AVG(intV)", DOUBLE)))));
  }

  @Test
  void push_down_projectList_to_relation() {
    assertEquals(
        project(
            indexScan("schema", projects(DSL.ref("intV", INTEGER))),
            DSL.named("i", DSL.ref("intV", INTEGER))
        ),
        optimize(
            project(
                relation("schema", table),
                DSL.named("i", DSL.ref("intV", INTEGER)))
        )
    );
  }

  /**
   * Project(intV, abs(intV)) -> Relation.
   * -- will be optimized as
   * Project(intV, abs(intV)) -> Relation(project=intV).
   */
  @Test
  void push_down_should_handle_duplication() {
    assertEquals(
        project(
            indexScan("schema", projects(DSL.ref("intV", INTEGER))),
            DSL.named("i", DSL.ref("intV", INTEGER)),
            DSL.named("absi", dsl.abs(DSL.ref("intV", INTEGER)))
        ),
        optimize(
            project(
                relation("schema", table),
                DSL.named("i", DSL.ref("intV", INTEGER)),
                DSL.named("absi", dsl.abs(DSL.ref("intV", INTEGER))))
        )
    );
  }

  /**
   * Project(ListA) -> Project(ListB) -> Relation.
   * -- will be optimized as
   * Project(ListA) -> Project(ListB) -> Relation(project=ListB).
   */
  @Test
  void only_one_project_should_be_push() {
    assertEquals(
        project(
            project(
                indexScan("schema",
                    projects(DSL.ref("intV", INTEGER), DSL.ref("stringV", STRING))
                ),
                DSL.named("i", DSL.ref("intV", INTEGER)),
                DSL.named("s", DSL.ref("stringV", STRING))
            ),
            DSL.named("i", DSL.ref("intV", INTEGER))
        ),
        optimize(
            project(
                project(
                    relation("schema", table),
                    DSL.named("i", DSL.ref("intV", INTEGER)),
                    DSL.named("s", DSL.ref("stringV", STRING))
                ),
                DSL.named("i", DSL.ref("intV", INTEGER))
            )
        )
    );
  }

  @Test
  void project_literal_no_push() {
    assertEquals(
        project(
            relation("schema", table),
            DSL.named("i", DSL.literal("str"))
        ),
        optimize(
            project(
                relation("schema", table),
                DSL.named("i", DSL.literal("str"))
            )
        )
    );
  }

  /**
   * SELECT AVG(intV) FILTER(WHERE intV > 1) FROM schema GROUP BY stringV.
   */
  @Test
  void filter_aggregation_merge_relation() {
    assertEquals(
        project(
            indexScanAgg("schema", ImmutableList.of(DSL.named("AVG(intV)",
                dsl.avg(DSL.ref("intV", INTEGER))
                    .condition(dsl.greater(DSL.ref("intV", INTEGER), DSL.literal(1))))),
                ImmutableList.of(DSL.named("stringV", DSL.ref("stringV", STRING)))),
            DSL.named("avg(intV) filter(where intV > 1)", DSL.ref("avg(intV)", DOUBLE))),
        optimize(
            project(
                aggregation(
                    relation("schema", table),
                    ImmutableList.of(DSL.named("AVG(intV)",
                        dsl.avg(DSL.ref("intV", INTEGER))
                            .condition(dsl.greater(DSL.ref("intV", INTEGER), DSL.literal(1))))),
                    ImmutableList.of(DSL.named("stringV", DSL.ref("stringV", STRING)))),
                DSL.named("avg(intV) filter(where intV > 1)", DSL.ref("avg(intV)", DOUBLE)))
        )
    );
  }

  /**
   * SELECT AVG(intV) FILTER(WHERE intV > 1) FROM schema WHERE longV < 1 GROUP BY stringV.
   */
  @Test
  void filter_aggregation_merge_filter_relation() {
    assertEquals(
        project(
            indexScanAgg("schema",
                dsl.less(DSL.ref("longV", LONG), DSL.literal(1)),
                ImmutableList.of(DSL.named("avg(intV)",
                        dsl.avg(DSL.ref("intV", INTEGER))
                            .condition(dsl.greater(DSL.ref("intV", INTEGER), DSL.literal(1))))),
                ImmutableList.of(DSL.named("stringV", DSL.ref("stringV", STRING)))),
            DSL.named("avg(intV) filter(where intV > 1)", DSL.ref("avg(intV)", DOUBLE))),
        optimize(
            project(
                aggregation(
                    filter(
                        relation("schema", table),
                        dsl.less(DSL.ref("longV", LONG), DSL.literal(1))
                    ),
                    ImmutableList.of(DSL.named("avg(intV)",
                        dsl.avg(DSL.ref("intV", INTEGER))
                            .condition(dsl.greater(DSL.ref("intV", INTEGER), DSL.literal(1))))),
                    ImmutableList.of(DSL.named("stringV", DSL.ref("stringV", STRING)))),
                DSL.named("avg(intV) filter(where intV > 1)", DSL.ref("avg(intV)", DOUBLE)))
        )
    );
  }

  private LogicalPlan optimize(LogicalPlan plan) {
    final LogicalPlanOptimizer optimizer = OpenSearchLogicalPlanOptimizerFactory.create();
    final LogicalPlan optimize = optimizer.optimize(plan);
    return optimize;
  }
}
