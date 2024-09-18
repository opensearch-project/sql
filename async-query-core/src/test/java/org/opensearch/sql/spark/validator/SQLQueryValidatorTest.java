/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.validator;

import static org.junit.jupiter.api.Assertions.assertThrows;

import lombok.AllArgsConstructor;
import org.antlr.v4.runtime.CommonTokenStream;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.common.antlr.CaseInsensitiveCharStream;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.spark.antlr.parser.SqlBaseLexer;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser.SingleStatementContext;

class SQLQueryValidatorTest {
  GrammarElementValidatorFactory factory = new GrammarElementValidatorFactory();
  SQLQueryValidator sqlQueryValidator = new SQLQueryValidator(factory);

  @AllArgsConstructor
  private enum TestQuery {
    // DDL Statements
    ALTER_DATABASE(
        "ALTER DATABASE inventory SET DBPROPERTIES ('Edit-date' = '01/01/2001');"),
    ALTER_TABLE(
        "ALTER TABLE default.StudentInfo PARTITION (age='10') RENAME TO PARTITION (age='15');"),
    ALTER_VIEW("ALTER VIEW tempdb1.v1 RENAME TO tempdb1.v2;"),
    CREATE_DATABASE("CREATE DATABASE IF NOT EXISTS customer_db;\n"),
    CREATE_FUNCTION("CREATE FUNCTION simple_udf AS 'SimpleUdf' USING JAR '/tmp/SimpleUdf.jar';"),
    CREATE_TABLE("CREATE TABLE Student_Dupli like Student;"),
    CREATE_VIEW(
        "CREATE OR REPLACE VIEW experienced_employee"
            + "    (ID COMMENT 'Unique identification number', Name)"
            + "    COMMENT 'View for experienced employees'"
            + "    AS SELECT id, name FROM all_employee"
            + "        WHERE working_years > 5;"),
    DROP_DATABASE("DROP DATABASE inventory_db CASCADE;"),
    DROP_FUNCTION("DROP FUNCTION test_avg;"),
    DROP_TABLE("DROP TABLE employeetable;"),
    DROP_VIEW("DROP VIEW employeeView;"),
    REPAIR_TABLE("REPAIR TABLE t1;"),
    TRUNCATE_TABLE("TRUNCATE TABLE Student partition(age=10);"),

    // DML Statements
    INSERT_TABLE("INSERT INTO target_table SELECT * FROM source_table;"),
    INSERT_OVERWRITE_DIRECTORY(
        "INSERT OVERWRITE DIRECTORY '/path/to/output' SELECT * FROM source_table;"),
    LOAD("LOAD DATA INPATH '/path/to/data' INTO TABLE target_table;"),

    // Data Retrieval Statements
    SELECT("SELECT 1"),
    EXPLAIN("EXPLAIN SELECT * FROM my_table;"),
    COMMON_TABLE_EXPRESSION(
        "WITH cte AS (SELECT * FROM my_table WHERE age > 30) SELECT * FROM cte;"),
    CLUSTER_BY_CLAUSE("SELECT * FROM my_table CLUSTER BY age;"),
    DISTRIBUTE_BY_CLAUSE("SELECT * FROM my_table DISTRIBUTE BY name;"),
    GROUP_BY_CLAUSE("SELECT name, count(*) FROM my_table GROUP BY name;"),
    HAVING_CLAUSE("SELECT name, count(*) FROM my_table GROUP BY name HAVING count(*) > 1;"),
    HINTS("SELECT /*+ BROADCAST(my_table) */ * FROM my_table;"),
    INLINE_TABLE("SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS inline_table(id, value);"),
    FILE("SELECT * FROM text.`/path/to/file.txt`;"),
    INNER_JOIN("SELECT t1.name, t2.age FROM table1 t1 INNER JOIN table2 t2 ON t1.id = t2.id;"),
    CROSS_JOIN("SELECT t1.name, t2.age FROM table1 t1 CROSS JOIN table2 t2;"),
    LEFT_OUTER_JOIN(
        "SELECT t1.name, t2.age FROM table1 t1 LEFT OUTER JOIN table2 t2 ON t1.id = t2.id;"),
    LEFT_SEMI_JOIN("SELECT t1.name FROM table1 t1 LEFT SEMI JOIN table2 t2 ON t1.id = t2.id;"),
    RIGHT_OUTER_JOIN(
        "SELECT t1.name, t2.age FROM table1 t1 RIGHT OUTER JOIN table2 t2 ON t1.id = t2.id;"),
    FULL_OUTER_JOIN(
        "SELECT t1.name, t2.age FROM table1 t1 FULL OUTER JOIN table2 t2 ON t1.id = t2.id;"),
    LEFT_ANTI_JOIN("SELECT t1.name FROM table1 t1 LEFT ANTI JOIN table2 t2 ON t1.id = t2.id;"),
    LIKE_PREDICATE("SELECT * FROM my_table WHERE name LIKE 'A%';"),
    LIMIT_CLAUSE("SELECT * FROM my_table LIMIT 10;"),
    OFFSET_CLAUSE("SELECT * FROM my_table OFFSET 5;"),
    ORDER_BY_CLAUSE("SELECT * FROM my_table ORDER BY age DESC;"),
    SET_OPERATORS("SELECT * FROM table1 UNION SELECT * FROM table2;"),
    SORT_BY_CLAUSE("SELECT * FROM my_table SORT BY age DESC;"),
    TABLESAMPLE("SELECT * FROM my_table TABLESAMPLE(10 PERCENT);"),
    //    TABLE_VALUED_FUNCTION("SELECT explode(array(10, 20));"), TODO: Need to handle this case
    TABLE_VALUED_FUNCTION("SELECT * FROM explode(array(10, 20));"),
    WHERE_CLAUSE("SELECT * FROM my_table WHERE age > 30;"),
    AGGREGATE_FUNCTION("SELECT count(*) FROM my_table;"),
    WINDOW_FUNCTION("SELECT name, age, rank() OVER (ORDER BY age DESC) FROM my_table;"),
    CASE_CLAUSE("SELECT name, CASE WHEN age > 30 THEN 'Adult' ELSE 'Young' END FROM my_table;"),
    PIVOT_CLAUSE(
        "SELECT * FROM (SELECT name, age, gender FROM my_table) PIVOT (COUNT(*) FOR gender IN ('M',"
            + " 'F'));"),
    UNPIVOT_CLAUSE(
        "SELECT name, value, category FROM (SELECT name, 'M' AS gender, age AS male_age, 0 AS"
            + " female_age FROM my_table) UNPIVOT (value FOR category IN (male_age, female_age));"),
    LATERAL_VIEW_CLAUSE(
        "SELECT name, age, exploded_value FROM my_table LATERAL VIEW OUTER EXPLODE(split(comments,"
            + " ',')) exploded_table AS exploded_value;"),
    LATERAL_SUBQUERY(
        "SELECT name, age, (SELECT max(age) FROM my_table t2 WHERE t1.age < t2.age) AS next_age"
            + " FROM my_table t1;"),
    TRANSFORM_CLAUSE(
        "SELECT transform(zip_code, name, age) USING 'cat' AS (a, b, c) FROM my_table;"),

    // Auxiliary Statements
    ADD_FILE("ADD FILE /tmp/test.txt;"),
    ADD_JAR("ADD JAR /path/to/my.jar;"),
    ANALYZE_TABLE("ANALYZE TABLE my_table COMPUTE STATISTICS;"),
    CACHE_TABLE("CACHE TABLE my_table;"),
    CLEAR_CACHE("CLEAR CACHE;"),
    DESCRIBE_DATABASE("DESCRIBE DATABASE my_db;"),
    DESCRIBE_FUNCTION("DESCRIBE FUNCTION my_function;"),
    DESCRIBE_QUERY("DESCRIBE QUERY SELECT * FROM my_table;"),
    DESCRIBE_TABLE("DESCRIBE TABLE my_table;"),
    LIST_FILE("LIST FILE '/path/to/files';"),
    LIST_JAR("LIST JAR;"),
    REFRESH("REFRESH;"),
    REFRESH_TABLE("REFRESH TABLE my_table;"),
    REFRESH_FUNCTION("REFRESH FUNCTION my_function;"),
    RESET("RESET;"),
    SET("SET spark.sql.shuffle.partitions=200;"),
    SHOW_COLUMNS("SHOW COLUMNS FROM my_table;"),
    SHOW_CREATE_TABLE("SHOW CREATE TABLE my_table;"),
    SHOW_DATABASES("SHOW DATABASES;"),
    SHOW_FUNCTIONS("SHOW FUNCTIONS;"),
    SHOW_PARTITIONS("SHOW PARTITIONS my_table;"),
    SHOW_TABLE_EXTENDED("SHOW TABLE EXTENDED LIKE 'my_table';"),
    SHOW_TABLES("SHOW TABLES;"),
    SHOW_TBLPROPERTIES("SHOW TBLPROPERTIES my_table;"),
    SHOW_VIEWS("SHOW VIEWS;"),
    UNCACHE_TABLE("UNCACHE TABLE my_table;"),

    // Functions
    ARRAY_FUNCTIONS("SELECT array_contains(array(1, 2, 3), 2);"),
    MAP_FUNCTIONS("SELECT map_keys(map('a', 1, 'b', 2));"),
    DATE_AND_TIMESTAMP_FUNCTIONS("SELECT date_format(current_date(), 'yyyy-MM-dd');"),
    JSON_FUNCTIONS("SELECT json_tuple('{\"a\":1, \"b\":2}', 'a', 'b');"),
    MATHEMATICAL_FUNCTIONS("SELECT round(3.1415, 2);"),
    STRING_FUNCTIONS("SELECT map_concat('Hello', ' ', 'World');"),
    BITWISE_FUNCTIONS("SELECT bit_count(42);"),
    CONVERSION_FUNCTIONS("SELECT cast('2023-04-01' as date);"),
    CONDITIONAL_FUNCTIONS("SELECT if(1 > 0, 'true', 'false');"),
    PREDICATE_FUNCTIONS("SELECT isnotnull(1);"),
    CSV_FUNCTIONS("SELECT from_csv(array('a', 'b', 'c'), ',');"),
    MISC_FUNCTIONS("SELECT current_user();"),

    // Aggregate-like Functions
    AGGREGATE_FUNCTIONS("SELECT count(*), max(age), min(age) FROM my_table;"),
    WINDOW_FUNCTIONS("SELECT name, age, rank() OVER (ORDER BY age DESC) FROM my_table;"),

    // Generator Functions
    GENERATOR_FUNCTIONS("SELECT explode(array(1, 2, 3));"),

    // UDFs (User-Defined Functions)
    SCALAR_USER_DEFINED_FUNCTIONS("SELECT my_udf(name) FROM my_table;"),
    USER_DEFINED_AGGREGATE_FUNCTIONS("SELECT my_udaf(age) FROM my_table GROUP BY name;"),
    INTEGRATION_WITH_HIVE_UDFS_UDAFS_UDTFS("SELECT my_hive_udf(name) FROM my_table;");

    private final String query;

    @Override
    public String toString() {
      return query;
    }
  }

  @Test
  void s3glueQueries() {
    VerifyValidator v = new VerifyValidator(sqlQueryValidator, DataSourceType.S3GLUE);
    // DDL Statements
    v.ok(TestQuery.ALTER_DATABASE);
    v.ok(TestQuery.ALTER_TABLE);
    v.ng(TestQuery.ALTER_VIEW);
    v.ok(TestQuery.CREATE_DATABASE);
    v.ng(TestQuery.CREATE_FUNCTION);
    v.ok(TestQuery.CREATE_TABLE);
    v.ng(TestQuery.CREATE_VIEW);
    v.ok(TestQuery.DROP_DATABASE);
    v.ng(TestQuery.DROP_FUNCTION);
    v.ok(TestQuery.DROP_TABLE);
    v.ng(TestQuery.DROP_VIEW);
    v.ok(TestQuery.REPAIR_TABLE);
    v.ok(TestQuery.TRUNCATE_TABLE);

    // DML Statements
    v.ng(TestQuery.INSERT_TABLE);
    v.ng(TestQuery.INSERT_OVERWRITE_DIRECTORY);
    v.ng(TestQuery.LOAD);

    // Data Retrieval
    v.ok(TestQuery.SELECT);
    v.ok(TestQuery.EXPLAIN);
    v.ok(TestQuery.COMMON_TABLE_EXPRESSION);
    v.ng(TestQuery.CLUSTER_BY_CLAUSE);
    v.ng(TestQuery.DISTRIBUTE_BY_CLAUSE);
    v.ok(TestQuery.GROUP_BY_CLAUSE);
    v.ok(TestQuery.HAVING_CLAUSE);
    v.ng(TestQuery.HINTS);
    v.ng(TestQuery.INLINE_TABLE);
    v.ng(TestQuery.FILE);
    v.ok(TestQuery.INNER_JOIN);
    v.ng(TestQuery.CROSS_JOIN);
    v.ok(TestQuery.LEFT_OUTER_JOIN);
    v.ng(TestQuery.LEFT_SEMI_JOIN);
    v.ng(TestQuery.RIGHT_OUTER_JOIN);
    v.ng(TestQuery.FULL_OUTER_JOIN);
    v.ng(TestQuery.LEFT_ANTI_JOIN);
    v.ok(TestQuery.LIKE_PREDICATE);
    v.ok(TestQuery.LIMIT_CLAUSE);
    v.ok(TestQuery.OFFSET_CLAUSE);
    v.ok(TestQuery.ORDER_BY_CLAUSE);
    v.ok(TestQuery.SET_OPERATORS);
    v.ok(TestQuery.SORT_BY_CLAUSE);
    v.ng(TestQuery.TABLESAMPLE);
    v.ng(TestQuery.TABLE_VALUED_FUNCTION);
    v.ok(TestQuery.WHERE_CLAUSE);
    v.ok(TestQuery.AGGREGATE_FUNCTION);
    v.ok(TestQuery.WINDOW_FUNCTION);
    v.ok(TestQuery.CASE_CLAUSE);
    v.ok(TestQuery.PIVOT_CLAUSE);
    v.ok(TestQuery.UNPIVOT_CLAUSE);
    v.ok(TestQuery.LATERAL_VIEW_CLAUSE);
    v.ok(TestQuery.LATERAL_SUBQUERY);
    v.ng(TestQuery.TRANSFORM_CLAUSE);

    // Auxiliary Statements
    v.ng(TestQuery.ADD_FILE);
    v.ng(TestQuery.ADD_JAR);
    v.ok(TestQuery.ANALYZE_TABLE);
    v.ok(TestQuery.CACHE_TABLE);
    v.ok(TestQuery.CLEAR_CACHE);
    v.ok(TestQuery.DESCRIBE_DATABASE);
    v.ng(TestQuery.DESCRIBE_FUNCTION);
    v.ok(TestQuery.DESCRIBE_QUERY);
    v.ok(TestQuery.DESCRIBE_TABLE);
    v.ng(TestQuery.LIST_FILE);
    v.ng(TestQuery.LIST_JAR);
    v.ng(TestQuery.REFRESH);
    v.ok(TestQuery.REFRESH_TABLE);
    v.ng(TestQuery.REFRESH_FUNCTION);
    v.ng(TestQuery.RESET);
    v.ng(TestQuery.SET);
    v.ok(TestQuery.SHOW_COLUMNS);
    v.ok(TestQuery.SHOW_CREATE_TABLE);
    v.ok(TestQuery.SHOW_DATABASES);
    v.ng(TestQuery.SHOW_FUNCTIONS);
    v.ok(TestQuery.SHOW_PARTITIONS);
    v.ok(TestQuery.SHOW_TABLE_EXTENDED);
    v.ok(TestQuery.SHOW_TABLES);
    v.ok(TestQuery.SHOW_TBLPROPERTIES);
    v.ng(TestQuery.SHOW_VIEWS);
    v.ok(TestQuery.UNCACHE_TABLE);

    // Functions
    v.ok(TestQuery.ARRAY_FUNCTIONS);
    v.ok(TestQuery.MAP_FUNCTIONS);
    v.ok(TestQuery.DATE_AND_TIMESTAMP_FUNCTIONS);
    v.ok(TestQuery.JSON_FUNCTIONS);
    v.ok(TestQuery.MATHEMATICAL_FUNCTIONS);
    v.ok(TestQuery.STRING_FUNCTIONS);
    v.ok(TestQuery.BITWISE_FUNCTIONS);
    v.ok(TestQuery.CONVERSION_FUNCTIONS);
    v.ok(TestQuery.CONDITIONAL_FUNCTIONS);
    v.ok(TestQuery.PREDICATE_FUNCTIONS);
    v.ok(TestQuery.CSV_FUNCTIONS);
    v.ng(TestQuery.MISC_FUNCTIONS);

    // Aggregate-like Functions
    v.ok(TestQuery.AGGREGATE_FUNCTIONS);
    v.ok(TestQuery.WINDOW_FUNCTIONS);

    // Generator Functions
    v.ok(TestQuery.GENERATOR_FUNCTIONS);

    // UDFs
    v.ng(TestQuery.SCALAR_USER_DEFINED_FUNCTIONS);
    v.ng(TestQuery.USER_DEFINED_AGGREGATE_FUNCTIONS);
    v.ng(TestQuery.INTEGRATION_WITH_HIVE_UDFS_UDAFS_UDTFS);
  }

  @Test
  void securityLakeQueries() {
    VerifyValidator v = new VerifyValidator(sqlQueryValidator, DataSourceType.SECURITY_LAKE);
    // DDL Statements
    v.ng(TestQuery.ALTER_DATABASE);
    v.ng(TestQuery.ALTER_TABLE);
    v.ng(TestQuery.ALTER_VIEW);
    v.ng(TestQuery.CREATE_DATABASE);
    v.ng(TestQuery.CREATE_FUNCTION);
    v.ng(TestQuery.CREATE_TABLE);
    v.ng(TestQuery.CREATE_VIEW);
    v.ng(TestQuery.DROP_DATABASE);
    v.ng(TestQuery.DROP_FUNCTION);
    v.ng(TestQuery.DROP_TABLE);
    v.ng(TestQuery.DROP_VIEW);
    v.ng(TestQuery.REPAIR_TABLE);
    v.ng(TestQuery.TRUNCATE_TABLE);

    // DML Statements
    v.ng(TestQuery.INSERT_TABLE);
    v.ng(TestQuery.INSERT_OVERWRITE_DIRECTORY);
    v.ng(TestQuery.LOAD);

    // Data Retrieval
    v.ok(TestQuery.SELECT);
    v.ok(TestQuery.EXPLAIN);
    v.ok(TestQuery.COMMON_TABLE_EXPRESSION);
    v.ng(TestQuery.CLUSTER_BY_CLAUSE);
    v.ng(TestQuery.DISTRIBUTE_BY_CLAUSE);
    v.ok(TestQuery.GROUP_BY_CLAUSE);
    v.ok(TestQuery.HAVING_CLAUSE);
    v.ng(TestQuery.HINTS);
    v.ng(TestQuery.INLINE_TABLE);
    v.ng(TestQuery.FILE);
    v.ok(TestQuery.INNER_JOIN);
    v.ng(TestQuery.CROSS_JOIN);
    v.ok(TestQuery.LEFT_OUTER_JOIN);
    v.ng(TestQuery.LEFT_SEMI_JOIN);
    v.ng(TestQuery.RIGHT_OUTER_JOIN);
    v.ng(TestQuery.FULL_OUTER_JOIN);
    v.ng(TestQuery.LEFT_ANTI_JOIN);
    v.ok(TestQuery.LIKE_PREDICATE);
    v.ok(TestQuery.LIMIT_CLAUSE);
    v.ok(TestQuery.OFFSET_CLAUSE);
    v.ok(TestQuery.ORDER_BY_CLAUSE);
    v.ok(TestQuery.SET_OPERATORS);
    v.ok(TestQuery.SORT_BY_CLAUSE);
    v.ng(TestQuery.TABLESAMPLE);
    v.ng(TestQuery.TABLE_VALUED_FUNCTION);
    v.ok(TestQuery.WHERE_CLAUSE);
    v.ok(TestQuery.AGGREGATE_FUNCTION);
    v.ok(TestQuery.WINDOW_FUNCTION);
    v.ok(TestQuery.CASE_CLAUSE);
    v.ok(TestQuery.PIVOT_CLAUSE);
    v.ok(TestQuery.UNPIVOT_CLAUSE);
    v.ok(TestQuery.LATERAL_VIEW_CLAUSE);
    v.ok(TestQuery.LATERAL_SUBQUERY);
    v.ng(TestQuery.TRANSFORM_CLAUSE);

    // Auxiliary Statements
    v.ng(TestQuery.ADD_FILE);
    v.ng(TestQuery.ADD_JAR);
    v.ng(TestQuery.ANALYZE_TABLE);
    v.ng(TestQuery.CACHE_TABLE);
    v.ng(TestQuery.CLEAR_CACHE);
    v.ng(TestQuery.DESCRIBE_DATABASE);
    v.ng(TestQuery.DESCRIBE_FUNCTION);
    v.ng(TestQuery.DESCRIBE_QUERY);
    v.ng(TestQuery.DESCRIBE_TABLE);
    v.ng(TestQuery.LIST_FILE);
    v.ng(TestQuery.LIST_JAR);
    v.ng(TestQuery.REFRESH);
    v.ng(TestQuery.REFRESH_TABLE);
    v.ng(TestQuery.REFRESH_FUNCTION);
    v.ng(TestQuery.RESET);
    v.ng(TestQuery.SET);
    v.ng(TestQuery.SHOW_COLUMNS);
    v.ng(TestQuery.SHOW_CREATE_TABLE);
    v.ng(TestQuery.SHOW_DATABASES);
    v.ng(TestQuery.SHOW_FUNCTIONS);
    v.ng(TestQuery.SHOW_PARTITIONS);
    v.ng(TestQuery.SHOW_TABLE_EXTENDED);
    v.ng(TestQuery.SHOW_TABLES);
    v.ng(TestQuery.SHOW_TBLPROPERTIES);
    v.ng(TestQuery.SHOW_VIEWS);
    v.ng(TestQuery.UNCACHE_TABLE);

    // Functions
    v.ok(TestQuery.ARRAY_FUNCTIONS);
    v.ok(TestQuery.MAP_FUNCTIONS);
    v.ok(TestQuery.DATE_AND_TIMESTAMP_FUNCTIONS);
    v.ok(TestQuery.JSON_FUNCTIONS);
    v.ok(TestQuery.MATHEMATICAL_FUNCTIONS);
    v.ok(TestQuery.STRING_FUNCTIONS);
    v.ok(TestQuery.BITWISE_FUNCTIONS);
    v.ok(TestQuery.CONVERSION_FUNCTIONS);
    v.ok(TestQuery.CONDITIONAL_FUNCTIONS);
    v.ok(TestQuery.PREDICATE_FUNCTIONS);
    v.ng(TestQuery.CSV_FUNCTIONS);
    v.ng(TestQuery.MISC_FUNCTIONS);

    // Aggregate-like Functions
    v.ok(TestQuery.AGGREGATE_FUNCTIONS);
    v.ok(TestQuery.WINDOW_FUNCTIONS);

    // Generator Functions
    v.ok(TestQuery.GENERATOR_FUNCTIONS);

    // UDFs
    v.ng(TestQuery.SCALAR_USER_DEFINED_FUNCTIONS);
    v.ng(TestQuery.USER_DEFINED_AGGREGATE_FUNCTIONS);
    v.ng(TestQuery.INTEGRATION_WITH_HIVE_UDFS_UDAFS_UDTFS);
  }

  @AllArgsConstructor
  private static class VerifyValidator {
    private final SQLQueryValidator validator;
    private final DataSourceType dataSourceType;

    public void ok(TestQuery query) {
      runValidate(query.toString());
    }

    public void ng(TestQuery query) {
      assertThrows(
          IllegalArgumentException.class,
          () -> runValidate(query.toString()),
          "The query should throw: query=`" + query.toString() + "`");
    }

    void runValidate(String query) {
      validator.validate(query, dataSourceType);
    }

    SingleStatementContext getParser(String query) {
      SqlBaseParser sqlBaseParser =
          new SqlBaseParser(
              new CommonTokenStream(new SqlBaseLexer(new CaseInsensitiveCharStream(query))));
      return sqlBaseParser.singleStatement();
    }
  }
}
