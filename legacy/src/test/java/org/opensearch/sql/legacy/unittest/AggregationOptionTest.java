/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.unittest;

import com.alibaba.druid.sql.ast.expr.SQLAggregateOption;
import com.alibaba.druid.sql.ast.expr.SQLQueryExpr;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import org.opensearch.sql.legacy.domain.Field;
import org.opensearch.sql.legacy.domain.Select;
import org.opensearch.sql.legacy.exception.SqlParseException;
import org.opensearch.sql.legacy.parser.SqlParser;
import org.opensearch.sql.legacy.util.SqlParserUtils;

/** Unit test class for feature of aggregation options: DISTINCT, ALL, UNIQUE, DEDUPLICATION */
public class AggregationOptionTest {

  @Test
  public void selectDistinctFieldsShouldHaveAggregationOption() {
    List<Field> fields = getSelectFields("SELECT DISTINCT gender, city FROM accounts");
    for (Field field : fields) {
      Assert.assertEquals(field.getOption(), SQLAggregateOption.DISTINCT);
    }
  }

  @Test
  public void selectWithoutDistinctFieldsShouldNotHaveAggregationOption() {
    List<Field> fields = getSelectFields("SELECT gender, city FROM accounts");
    for (Field field : fields) {
      Assert.assertNull(field.getOption());
    }
  }

  @Test
  public void selectDistinctWithoutGroupByShouldHaveGroupByItems() {
    List<List<Field>> groupBys = getGroupBys("SELECT DISTINCT gender, city FROM accounts");
    Assert.assertFalse(groupBys.isEmpty());
  }

  @Test
  public void selectWithoutDistinctWithoutGroupByShouldNotHaveGroupByItems() {
    List<List<Field>> groupBys = getGroupBys("SELECT gender, city FROM accounts");
    Assert.assertTrue(groupBys.isEmpty());
  }

  private List<List<Field>> getGroupBys(String query) {
    return getSelect(query).getGroupBys();
  }

  private List<Field> getSelectFields(String query) {
    return getSelect(query).getFields();
  }

  private Select getSelect(String query) {
    SQLQueryExpr queryExpr = SqlParserUtils.parse(query);
    Select select = null;
    try {
      select = new SqlParser().parseSelect(queryExpr);
    } catch (SqlParseException e) {
      e.printStackTrace();
    }
    return select;
  }
}
