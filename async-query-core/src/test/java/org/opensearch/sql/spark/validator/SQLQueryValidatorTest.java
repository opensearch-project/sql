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

  @AllArgsConstructor
  private enum TestQuery {
    // DDL Statements
    ALTER_DATABASE(
        "ALTER DATABASE inventory SET DBPROPERTIES ('Edited-by' = 'John', 'Edit-date' ="
            + " '01/01/2001');"),
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
    OFFSET_CLAUSE("SELECT * FROM my_table OFFSET 5 ROWS;"),
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
    STRING_FUNCTIONS("SELECT concat('Hello', ' ', 'World');"),
    BITWISE_FUNCTIONS("SELECT bitwiseNOT(42);"),
    CONVERSION_FUNCTIONS("SELECT cast('2023-04-01' as date);"),
    CONDITIONAL_FUNCTIONS("SELECT if(1 > 0, 'true', 'false');"),
    PREDICATE_FUNCTIONS("SELECT array_exists(array(1, 2, 3), x -> x > 2);"),
    CSV_FUNCTIONS("SELECT csv_from_array(array('a', 'b', 'c'), ',');"),
    MISC_FUNCTIONS("SELECT hash('Hello World');"),

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
    SQLQueryValidator v =
        new SQLQueryValidator(factory.getValidatorForDatasource(DataSourceType.S3GLUE));
    verifyValid(v, TestQuery.ALTER_DATABASE);
    verifyValid(v, TestQuery.ALTER_TABLE);
    verifyInvalid(v, TestQuery.ALTER_VIEW);
    verifyValid(v, TestQuery.CREATE_DATABASE);
    verifyInvalid(v, TestQuery.CREATE_FUNCTION);
    verifyValid(v, TestQuery.CREATE_TABLE);
    verifyInvalid(v, TestQuery.CREATE_VIEW);
    verifyValid(v, TestQuery.DROP_DATABASE);
    verifyInvalid(v, TestQuery.DROP_FUNCTION);
    verifyValid(v, TestQuery.DROP_TABLE);
    verifyInvalid(v, TestQuery.DROP_VIEW);
    verifyValid(v, TestQuery.REPAIR_TABLE);
    verifyValid(v, TestQuery.TRUNCATE_TABLE);

    // DML Statements
    verifyInvalid(v, TestQuery.INSERT_TABLE);
    verifyInvalid(v, TestQuery.INSERT_OVERWRITE_DIRECTORY);
    verifyInvalid(v, TestQuery.LOAD);

    // Data Retrieval
    verifyValid(v, TestQuery.SELECT);
    verifyValid(v, TestQuery.EXPLAIN);
    verifyValid(v, TestQuery.COMMON_TABLE_EXPRESSION);
    verifyInvalid(v, TestQuery.CLUSTER_BY_CLAUSE);
    verifyInvalid(v, TestQuery.DISTRIBUTE_BY_CLAUSE);
    verifyValid(v, TestQuery.GROUP_BY_CLAUSE);
    verifyValid(v, TestQuery.HAVING_CLAUSE);
    verifyInvalid(v, TestQuery.HINTS);
    verifyInvalid(v, TestQuery.INLINE_TABLE);
    //    verifyInvalid(v, TestQuery.FILE); TODO: need dive deep
    verifyValid(v, TestQuery.INNER_JOIN);
    verifyInvalid(v, TestQuery.CROSS_JOIN);
    verifyValid(v, TestQuery.LEFT_OUTER_JOIN);
    verifyInvalid(v, TestQuery.LEFT_SEMI_JOIN);
    verifyInvalid(v, TestQuery.RIGHT_OUTER_JOIN);
    verifyInvalid(v, TestQuery.FULL_OUTER_JOIN);
    verifyInvalid(v, TestQuery.LEFT_ANTI_JOIN);
    verifyValid(v, TestQuery.LIKE_PREDICATE);
    verifyValid(v, TestQuery.LIMIT_CLAUSE);
    verifyValid(v, TestQuery.OFFSET_CLAUSE);
    verifyValid(v, TestQuery.ORDER_BY_CLAUSE);
    verifyValid(v, TestQuery.SET_OPERATORS);
    verifyValid(v, TestQuery.SORT_BY_CLAUSE);
    verifyInvalid(v, TestQuery.TABLESAMPLE);
    verifyInvalid(v, TestQuery.TABLE_VALUED_FUNCTION);
    verifyValid(v, TestQuery.WHERE_CLAUSE);
    verifyValid(v, TestQuery.AGGREGATE_FUNCTION);
    verifyValid(v, TestQuery.WINDOW_FUNCTION);
    verifyValid(v, TestQuery.CASE_CLAUSE);
    verifyValid(v, TestQuery.PIVOT_CLAUSE);
    verifyValid(v, TestQuery.UNPIVOT_CLAUSE);
    verifyValid(v, TestQuery.LATERAL_VIEW_CLAUSE);
    verifyValid(v, TestQuery.LATERAL_SUBQUERY);
    verifyInvalid(v, TestQuery.TRANSFORM_CLAUSE);

    // Auxiliary Statements
    verifyInvalid(v, TestQuery.ADD_FILE);
    verifyInvalid(v, TestQuery.ADD_JAR);
    verifyValid(v, TestQuery.ANALYZE_TABLE);
    verifyValid(v, TestQuery.CACHE_TABLE);
    verifyValid(v, TestQuery.CLEAR_CACHE);
    verifyValid(v, TestQuery.DESCRIBE_DATABASE);
    verifyInvalid(v, TestQuery.DESCRIBE_FUNCTION);
    verifyValid(v, TestQuery.DESCRIBE_QUERY);
    verifyValid(v, TestQuery.DESCRIBE_TABLE);
    verifyInvalid(v, TestQuery.LIST_FILE);
    verifyInvalid(v, TestQuery.LIST_JAR);
    verifyInvalid(v, TestQuery.REFRESH);
    //    verifyValid(v, TestQuery.REFRESH_TABLE); TODO: refreshTable rule won't match (matches to
    // refreshResource)
    verifyInvalid(v, TestQuery.REFRESH_FUNCTION);
    verifyInvalid(v, TestQuery.RESET);
    verifyInvalid(v, TestQuery.SET);
    verifyValid(v, TestQuery.SHOW_COLUMNS);
    verifyValid(v, TestQuery.SHOW_CREATE_TABLE);
    verifyValid(v, TestQuery.SHOW_DATABASES);
    verifyInvalid(v, TestQuery.SHOW_FUNCTIONS);
    verifyValid(v, TestQuery.SHOW_PARTITIONS);
    verifyValid(v, TestQuery.SHOW_TABLE_EXTENDED);
    verifyValid(v, TestQuery.SHOW_TABLES);
    verifyValid(v, TestQuery.SHOW_TBLPROPERTIES);
    verifyInvalid(v, TestQuery.SHOW_VIEWS);
    verifyValid(v, TestQuery.UNCACHE_TABLE);

    // Functions
    //    verifyValid(v, TestQuery.ARRAY_FUNCTIONS);
    //    verifyValid(v, TestQuery.MAP_FUNCTIONS);
    //    verifyValid(v, TestQuery.DATE_AND_TIMESTAMP_FUNCTIONS);
    //    verifyValid(v, TestQuery.JSON_FUNCTIONS);
    //    verifyValid(v, TestQuery.MATHEMATICAL_FUNCTIONS);
    //    verifyValid(v, TestQuery.STRING_FUNCTIONS);
    //    verifyValid(v, TestQuery.BITWISE_FUNCTIONS);
    //    verifyValid(v, TestQuery.CONVERSION_FUNCTIONS);
    //    verifyValid(v, TestQuery.CONDITIONAL_FUNCTIONS);
    //    verifyValid(v, TestQuery.PREDICATE_FUNCTIONS);
    //    verifyValid(v, TestQuery.CSV_FUNCTIONS);
    //    verifyValid(v, TestQuery.MISC_FUNCTIONS);

    // Aggregate-like Functions
    //    verifyValid(v, TestQuery.AGGREGATE_FUNCTIONS);
    //    verifyValid(v, TestQuery.WINDOW_FUNCTIONS);

    // Generator Functions
    //    verifyValid(v, TestQuery.GENERATOR_FUNCTIONS);

    // UDFs
    //    verifyInvalid(v, TestQuery.SCALAR_USER_DEFINED_FUNCTIONS);
    //    verifyInvalid(v, TestQuery.USER_DEFINED_AGGREGATE_FUNCTIONS);
    //    verifyInvalid(v, TestQuery.INTEGRATION_WITH_HIVE_UDFS_UDAFS_UDTFS);
  }

  void verifyValid(SQLQueryValidator validator, TestQuery query) {
    runValidate(validator, query.toString());
  }

  void verifyInvalid(SQLQueryValidator validator, TestQuery query) {
    assertThrows(
        IllegalArgumentException.class,
        () -> runValidate(validator, query.toString()),
        "The query should throw: query=`" + query.toString() + "`");
  }

  void runValidate(SQLQueryValidator validator, String query) {
    validator.validate(getParser(query));
  }

  SingleStatementContext getParser(String query) {
    SqlBaseParser sqlBaseParser =
        new SqlBaseParser(
            new CommonTokenStream(new SqlBaseLexer(new CaseInsensitiveCharStream(query))));
    return sqlBaseParser.singleStatement();
  }
}
