/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.validator;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.antlr.v4.runtime.CommonTokenStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.common.antlr.CaseInsensitiveCharStream;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.spark.antlr.parser.SqlBaseLexer;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser;
import org.opensearch.sql.spark.antlr.parser.SqlBaseParser.SingleStatementContext;

@ExtendWith(MockitoExtension.class)
class SQLQueryValidatorTest {
  GrammarElementValidatorFactory factory = new GrammarElementValidatorFactory();
  SQLQueryValidator sqlQueryValidator = new SQLQueryValidator(factory);

  @Mock GrammarElementValidatorFactory mockedFactory;

  private enum TestElement {
    // DDL Statements
    ALTER_DATABASE(
        "ALTER DATABASE inventory SET DBPROPERTIES ('Edit-date' = '01/01/2001');",
        "ALTER DATABASE dbx.tab1 UNSET PROPERTIES ('winner');",
        "ALTER DATABASE dbx.tab1 SET LOCATION '/path/to/part/ways';"),
    ALTER_TABLE(
        "ALTER TABLE default.StudentInfo PARTITION (age='10') RENAME TO PARTITION (age='15');",
        "ALTER TABLE dbx.tab1 UNSET TBLPROPERTIES ('winner');",
        "ALTER TABLE StudentInfo ADD columns (LastName string, DOB timestamp);",
        "ALTER TABLE StudentInfo ADD IF NOT EXISTS PARTITION (age=18);",
        "ALTER TABLE StudentInfo RENAME COLUMN name TO FirstName;",
        "ALTER TABLE StudentInfo RENAME TO newName;",
        "ALTER TABLE StudentInfo DROP columns (LastName, DOB);",
        "ALTER TABLE StudentInfo ALTER COLUMN FirstName COMMENT \"new comment\";",
        "ALTER TABLE StudentInfo REPLACE COLUMNS (name string, ID int COMMENT 'new comment');",
        "ALTER TABLE test_tab SET SERDE 'org.apache.LazyBinaryColumnarSerDe';",
        "ALTER TABLE StudentInfo DROP IF EXISTS PARTITION (age=18);",
        "ALTER TABLE dbx.tab1 PARTITION (a='1', b='2') SET LOCATION '/path/to/part/ways';",
        "ALTER TABLE dbx.tab1 RECOVER PARTITIONS;",
        "ALTER TABLE dbx.tab1 CLUSTER BY NONE;",
        "ALTER TABLE dbx.tab1 SET LOCATION '/path/to/part/ways';"),
    ALTER_VIEW(
        "ALTER VIEW tempdb1.v1 RENAME TO tempdb1.v2;",
        "ALTER VIEW tempdb1.v2 AS SELECT * FROM tempdb1.v1;",
        "ALTER VIEW tempdb1.v2 WITH SCHEMA BINDING"),
    CREATE_DATABASE("CREATE DATABASE IF NOT EXISTS customer_db;\n"),
    CREATE_FUNCTION("CREATE FUNCTION simple_udf AS 'SimpleUdf' USING JAR '/tmp/SimpleUdf.jar';"),
    CREATE_TABLE(
        "CREATE TABLE Student_Dupli like Student;",
        "CREATE TABLE student (id INT, name STRING, age INT) USING CSV;",
        "CREATE TABLE student_copy USING CSV AS SELECT * FROM student;",
        "CREATE TABLE student (id INT, name STRING, age INT);",
        "REPLACE TABLE student (id INT, name STRING, age INT) USING CSV;"),
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
    INSERT_TABLE(
        "INSERT INTO target_table SELECT * FROM source_table;",
        "INSERT INTO persons REPLACE WHERE ssn = 123456789 SELECT * FROM persons2;",
        "INSERT OVERWRITE students VALUES ('Ashua Hill', '456 Erica Ct, Cupertino', 111111);"),
    INSERT_OVERWRITE_DIRECTORY(
        "INSERT OVERWRITE DIRECTORY '/path/to/output' SELECT * FROM source_table;",
        "INSERT OVERWRITE DIRECTORY USING myTable SELECT * FROM source_table;",
        "INSERT OVERWRITE LOCAL DIRECTORY '/tmp/destination' STORED AS orc SELECT * FROM"
            + " test_table;"),
    LOAD("LOAD DATA INPATH '/path/to/data' INTO TABLE target_table;"),

    // Data Retrieval Statements
    SELECT("SELECT 1"),
    EXPLAIN("EXPLAIN SELECT * FROM my_table;"),
    COMMON_TABLE_EXPRESSION(
        "WITH cte AS (SELECT * FROM my_table WHERE age > 30) SELECT * FROM cte;"),
    CLUSTER_BY_CLAUSE(
        "SELECT * FROM my_table CLUSTER BY age;", "ALTER TABLE testTable CLUSTER BY (age);"),
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
        "SELECT * FROM t1, LATERAL (SELECT * FROM t2 WHERE t1.c1 = t2.c1);",
        "SELECT * FROM t1 JOIN LATERAL (SELECT * FROM t2 WHERE t1.c1 = t2.c1);"),
    TRANSFORM_CLAUSE(
        "SELECT transform(zip_code, name, age) USING 'cat' AS (a, b, c) FROM my_table;"),

    // Auxiliary Statements
    ADD_FILE("ADD FILE /tmp/test.txt;"),
    ADD_JAR("ADD JAR /path/to/my.jar;"),
    ANALYZE_TABLE(
        "ANALYZE TABLE my_table COMPUTE STATISTICS;",
        "ANALYZE TABLES IN school_db COMPUTE STATISTICS NOSCAN;"),
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
    RESET("RESET;", "RESET spark.abc;", "RESET `key`;"),
    SET(
        "SET spark.sql.shuffle.partitions=200;",
        "SET -v;",
        "SET;",
        "SET spark.sql.variable.substitute;"),
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
    STRING_FUNCTIONS("SELECT ascii('Hello');"),
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

    @Getter private final String[] queries;

    TestElement(String... queries) {
      this.queries = queries;
    }
  }

  @Test
  void testAllowAllByDefault() {
    VerifyValidator v = new VerifyValidator(sqlQueryValidator, DataSourceType.SPARK);
    Arrays.stream(TestElement.values()).forEach(v::ok);
  }

  @Test
  void testDenyAllValidator() {
    when(mockedFactory.getValidatorForDatasource(any())).thenReturn(element -> false);
    VerifyValidator v =
        new VerifyValidator(new SQLQueryValidator(mockedFactory), DataSourceType.SPARK);
    // The elements which doesn't have validation will be accepted. (That's why there are some 'ok'
    // case)

    // DDL Statements
    v.ng(TestElement.ALTER_DATABASE);
    v.ng(TestElement.ALTER_TABLE);
    v.ng(TestElement.ALTER_VIEW);
    v.ng(TestElement.CREATE_DATABASE);
    v.ng(TestElement.CREATE_FUNCTION);
    v.ng(TestElement.CREATE_TABLE);
    v.ng(TestElement.CREATE_VIEW);
    v.ng(TestElement.DROP_DATABASE);
    v.ng(TestElement.DROP_FUNCTION);
    v.ng(TestElement.DROP_TABLE);
    v.ng(TestElement.DROP_VIEW);
    v.ng(TestElement.REPAIR_TABLE);
    v.ng(TestElement.TRUNCATE_TABLE);

    // DML Statements
    v.ng(TestElement.INSERT_TABLE);
    v.ng(TestElement.INSERT_OVERWRITE_DIRECTORY);
    v.ng(TestElement.LOAD);

    // Data Retrieval
    v.ng(TestElement.EXPLAIN);
    v.ng(TestElement.COMMON_TABLE_EXPRESSION);
    v.ng(TestElement.CLUSTER_BY_CLAUSE);
    v.ng(TestElement.DISTRIBUTE_BY_CLAUSE);
    v.ok(TestElement.GROUP_BY_CLAUSE);
    v.ok(TestElement.HAVING_CLAUSE);
    v.ng(TestElement.HINTS);
    v.ng(TestElement.INLINE_TABLE);
    v.ng(TestElement.FILE);
    v.ng(TestElement.INNER_JOIN);
    v.ng(TestElement.CROSS_JOIN);
    v.ng(TestElement.LEFT_OUTER_JOIN);
    v.ng(TestElement.LEFT_SEMI_JOIN);
    v.ng(TestElement.RIGHT_OUTER_JOIN);
    v.ng(TestElement.FULL_OUTER_JOIN);
    v.ng(TestElement.LEFT_ANTI_JOIN);
    v.ok(TestElement.LIKE_PREDICATE);
    v.ok(TestElement.LIMIT_CLAUSE);
    v.ok(TestElement.OFFSET_CLAUSE);
    v.ok(TestElement.ORDER_BY_CLAUSE);
    v.ok(TestElement.SET_OPERATORS);
    v.ok(TestElement.SORT_BY_CLAUSE);
    v.ng(TestElement.TABLESAMPLE);
    v.ng(TestElement.TABLE_VALUED_FUNCTION);
    v.ok(TestElement.WHERE_CLAUSE);
    v.ok(TestElement.AGGREGATE_FUNCTION);
    v.ok(TestElement.WINDOW_FUNCTION);
    v.ok(TestElement.CASE_CLAUSE);
    v.ok(TestElement.PIVOT_CLAUSE);
    v.ok(TestElement.UNPIVOT_CLAUSE);
    v.ng(TestElement.LATERAL_VIEW_CLAUSE);
    v.ng(TestElement.LATERAL_SUBQUERY);
    v.ng(TestElement.TRANSFORM_CLAUSE);

    // Auxiliary Statements
    v.ng(TestElement.ADD_FILE);
    v.ng(TestElement.ADD_JAR);
    v.ng(TestElement.ANALYZE_TABLE);
    v.ng(TestElement.CACHE_TABLE);
    v.ng(TestElement.CLEAR_CACHE);
    v.ng(TestElement.DESCRIBE_DATABASE);
    v.ng(TestElement.DESCRIBE_FUNCTION);
    v.ng(TestElement.DESCRIBE_QUERY);
    v.ng(TestElement.DESCRIBE_TABLE);
    v.ng(TestElement.LIST_FILE);
    v.ng(TestElement.LIST_JAR);
    v.ng(TestElement.REFRESH);
    v.ng(TestElement.REFRESH_TABLE);
    v.ng(TestElement.REFRESH_FUNCTION);
    v.ng(TestElement.RESET);
    v.ng(TestElement.SET);
    v.ng(TestElement.SHOW_COLUMNS);
    v.ng(TestElement.SHOW_CREATE_TABLE);
    v.ng(TestElement.SHOW_DATABASES);
    v.ng(TestElement.SHOW_FUNCTIONS);
    v.ng(TestElement.SHOW_PARTITIONS);
    v.ng(TestElement.SHOW_TABLE_EXTENDED);
    v.ng(TestElement.SHOW_TABLES);
    v.ng(TestElement.SHOW_TBLPROPERTIES);
    v.ng(TestElement.SHOW_VIEWS);
    v.ng(TestElement.UNCACHE_TABLE);

    // Functions
    v.ok(TestElement.ARRAY_FUNCTIONS);
    v.ng(TestElement.MAP_FUNCTIONS);
    v.ok(TestElement.DATE_AND_TIMESTAMP_FUNCTIONS);
    v.ok(TestElement.JSON_FUNCTIONS);
    v.ok(TestElement.MATHEMATICAL_FUNCTIONS);
    v.ok(TestElement.STRING_FUNCTIONS);
    v.ok(TestElement.BITWISE_FUNCTIONS);
    v.ok(TestElement.CONVERSION_FUNCTIONS);
    v.ok(TestElement.CONDITIONAL_FUNCTIONS);
    v.ok(TestElement.PREDICATE_FUNCTIONS);
    v.ng(TestElement.CSV_FUNCTIONS);
    v.ng(TestElement.MISC_FUNCTIONS);

    // Aggregate-like Functions
    v.ok(TestElement.AGGREGATE_FUNCTIONS);
    v.ok(TestElement.WINDOW_FUNCTIONS);

    // Generator Functions
    v.ok(TestElement.GENERATOR_FUNCTIONS);

    // UDFs
    v.ng(TestElement.SCALAR_USER_DEFINED_FUNCTIONS);
    v.ng(TestElement.USER_DEFINED_AGGREGATE_FUNCTIONS);
    v.ng(TestElement.INTEGRATION_WITH_HIVE_UDFS_UDAFS_UDTFS);
  }

  @Test
  void s3glueQueries() {
    VerifyValidator v = new VerifyValidator(sqlQueryValidator, DataSourceType.S3GLUE);
    // DDL Statements
    v.ok(TestElement.ALTER_DATABASE);
    v.ok(TestElement.ALTER_TABLE);
    v.ng(TestElement.ALTER_VIEW);
    v.ok(TestElement.CREATE_DATABASE);
    v.ng(TestElement.CREATE_FUNCTION);
    v.ok(TestElement.CREATE_TABLE);
    v.ng(TestElement.CREATE_VIEW);
    v.ok(TestElement.DROP_DATABASE);
    v.ng(TestElement.DROP_FUNCTION);
    v.ok(TestElement.DROP_TABLE);
    v.ng(TestElement.DROP_VIEW);
    v.ok(TestElement.REPAIR_TABLE);
    v.ok(TestElement.TRUNCATE_TABLE);

    // DML Statements
    v.ng(TestElement.INSERT_TABLE);
    v.ng(TestElement.INSERT_OVERWRITE_DIRECTORY);
    v.ng(TestElement.LOAD);

    // Data Retrieval
    v.ok(TestElement.SELECT);
    v.ok(TestElement.EXPLAIN);
    v.ok(TestElement.COMMON_TABLE_EXPRESSION);
    v.ng(TestElement.CLUSTER_BY_CLAUSE);
    v.ng(TestElement.DISTRIBUTE_BY_CLAUSE);
    v.ok(TestElement.GROUP_BY_CLAUSE);
    v.ok(TestElement.HAVING_CLAUSE);
    v.ng(TestElement.HINTS);
    v.ng(TestElement.INLINE_TABLE);
    v.ng(TestElement.FILE);
    v.ok(TestElement.INNER_JOIN);
    v.ng(TestElement.CROSS_JOIN);
    v.ok(TestElement.LEFT_OUTER_JOIN);
    v.ng(TestElement.LEFT_SEMI_JOIN);
    v.ng(TestElement.RIGHT_OUTER_JOIN);
    v.ng(TestElement.FULL_OUTER_JOIN);
    v.ng(TestElement.LEFT_ANTI_JOIN);
    v.ok(TestElement.LIKE_PREDICATE);
    v.ok(TestElement.LIMIT_CLAUSE);
    v.ok(TestElement.OFFSET_CLAUSE);
    v.ok(TestElement.ORDER_BY_CLAUSE);
    v.ok(TestElement.SET_OPERATORS);
    v.ok(TestElement.SORT_BY_CLAUSE);
    v.ng(TestElement.TABLESAMPLE);
    v.ng(TestElement.TABLE_VALUED_FUNCTION);
    v.ok(TestElement.WHERE_CLAUSE);
    v.ok(TestElement.AGGREGATE_FUNCTION);
    v.ok(TestElement.WINDOW_FUNCTION);
    v.ok(TestElement.CASE_CLAUSE);
    v.ok(TestElement.PIVOT_CLAUSE);
    v.ok(TestElement.UNPIVOT_CLAUSE);
    v.ok(TestElement.LATERAL_VIEW_CLAUSE);
    v.ok(TestElement.LATERAL_SUBQUERY);
    v.ng(TestElement.TRANSFORM_CLAUSE);

    // Auxiliary Statements
    v.ng(TestElement.ADD_FILE);
    v.ng(TestElement.ADD_JAR);
    v.ok(TestElement.ANALYZE_TABLE);
    v.ok(TestElement.CACHE_TABLE);
    v.ok(TestElement.CLEAR_CACHE);
    v.ok(TestElement.DESCRIBE_DATABASE);
    v.ng(TestElement.DESCRIBE_FUNCTION);
    v.ok(TestElement.DESCRIBE_QUERY);
    v.ok(TestElement.DESCRIBE_TABLE);
    v.ng(TestElement.LIST_FILE);
    v.ng(TestElement.LIST_JAR);
    v.ng(TestElement.REFRESH);
    v.ok(TestElement.REFRESH_TABLE);
    v.ng(TestElement.REFRESH_FUNCTION);
    v.ng(TestElement.RESET);
    v.ng(TestElement.SET);
    v.ok(TestElement.SHOW_COLUMNS);
    v.ok(TestElement.SHOW_CREATE_TABLE);
    v.ok(TestElement.SHOW_DATABASES);
    v.ng(TestElement.SHOW_FUNCTIONS);
    v.ok(TestElement.SHOW_PARTITIONS);
    v.ok(TestElement.SHOW_TABLE_EXTENDED);
    v.ok(TestElement.SHOW_TABLES);
    v.ok(TestElement.SHOW_TBLPROPERTIES);
    v.ng(TestElement.SHOW_VIEWS);
    v.ok(TestElement.UNCACHE_TABLE);

    // Functions
    v.ok(TestElement.ARRAY_FUNCTIONS);
    v.ok(TestElement.MAP_FUNCTIONS);
    v.ok(TestElement.DATE_AND_TIMESTAMP_FUNCTIONS);
    v.ok(TestElement.JSON_FUNCTIONS);
    v.ok(TestElement.MATHEMATICAL_FUNCTIONS);
    v.ok(TestElement.STRING_FUNCTIONS);
    v.ok(TestElement.BITWISE_FUNCTIONS);
    v.ok(TestElement.CONVERSION_FUNCTIONS);
    v.ok(TestElement.CONDITIONAL_FUNCTIONS);
    v.ok(TestElement.PREDICATE_FUNCTIONS);
    v.ok(TestElement.CSV_FUNCTIONS);
    v.ng(TestElement.MISC_FUNCTIONS);

    // Aggregate-like Functions
    v.ok(TestElement.AGGREGATE_FUNCTIONS);
    v.ok(TestElement.WINDOW_FUNCTIONS);

    // Generator Functions
    v.ok(TestElement.GENERATOR_FUNCTIONS);

    // UDFs
    v.ng(TestElement.SCALAR_USER_DEFINED_FUNCTIONS);
    v.ng(TestElement.USER_DEFINED_AGGREGATE_FUNCTIONS);
    v.ng(TestElement.INTEGRATION_WITH_HIVE_UDFS_UDAFS_UDTFS);
  }

  @Test
  void securityLakeQueries() {
    VerifyValidator v = new VerifyValidator(sqlQueryValidator, DataSourceType.SECURITY_LAKE);
    // DDL Statements
    v.ng(TestElement.ALTER_DATABASE);
    v.ng(TestElement.ALTER_TABLE);
    v.ng(TestElement.ALTER_VIEW);
    v.ng(TestElement.CREATE_DATABASE);
    v.ng(TestElement.CREATE_FUNCTION);
    v.ng(TestElement.CREATE_TABLE);
    v.ng(TestElement.CREATE_VIEW);
    v.ng(TestElement.DROP_DATABASE);
    v.ng(TestElement.DROP_FUNCTION);
    v.ng(TestElement.DROP_TABLE);
    v.ng(TestElement.DROP_VIEW);
    v.ng(TestElement.REPAIR_TABLE);
    v.ng(TestElement.TRUNCATE_TABLE);

    // DML Statements
    v.ng(TestElement.INSERT_TABLE);
    v.ng(TestElement.INSERT_OVERWRITE_DIRECTORY);
    v.ng(TestElement.LOAD);

    // Data Retrieval
    v.ok(TestElement.SELECT);
    v.ok(TestElement.EXPLAIN);
    v.ok(TestElement.COMMON_TABLE_EXPRESSION);
    v.ng(TestElement.CLUSTER_BY_CLAUSE);
    v.ng(TestElement.DISTRIBUTE_BY_CLAUSE);
    v.ok(TestElement.GROUP_BY_CLAUSE);
    v.ok(TestElement.HAVING_CLAUSE);
    v.ng(TestElement.HINTS);
    v.ng(TestElement.INLINE_TABLE);
    v.ng(TestElement.FILE);
    v.ok(TestElement.INNER_JOIN);
    v.ng(TestElement.CROSS_JOIN);
    v.ok(TestElement.LEFT_OUTER_JOIN);
    v.ng(TestElement.LEFT_SEMI_JOIN);
    v.ng(TestElement.RIGHT_OUTER_JOIN);
    v.ng(TestElement.FULL_OUTER_JOIN);
    v.ng(TestElement.LEFT_ANTI_JOIN);
    v.ok(TestElement.LIKE_PREDICATE);
    v.ok(TestElement.LIMIT_CLAUSE);
    v.ok(TestElement.OFFSET_CLAUSE);
    v.ok(TestElement.ORDER_BY_CLAUSE);
    v.ok(TestElement.SET_OPERATORS);
    v.ok(TestElement.SORT_BY_CLAUSE);
    v.ng(TestElement.TABLESAMPLE);
    v.ng(TestElement.TABLE_VALUED_FUNCTION);
    v.ok(TestElement.WHERE_CLAUSE);
    v.ok(TestElement.AGGREGATE_FUNCTION);
    v.ok(TestElement.WINDOW_FUNCTION);
    v.ok(TestElement.CASE_CLAUSE);
    v.ok(TestElement.PIVOT_CLAUSE);
    v.ok(TestElement.UNPIVOT_CLAUSE);
    v.ok(TestElement.LATERAL_VIEW_CLAUSE);
    v.ok(TestElement.LATERAL_SUBQUERY);
    v.ng(TestElement.TRANSFORM_CLAUSE);

    // Auxiliary Statements
    v.ng(TestElement.ADD_FILE);
    v.ng(TestElement.ADD_JAR);
    v.ng(TestElement.ANALYZE_TABLE);
    v.ng(TestElement.CACHE_TABLE);
    v.ng(TestElement.CLEAR_CACHE);
    v.ng(TestElement.DESCRIBE_DATABASE);
    v.ng(TestElement.DESCRIBE_FUNCTION);
    v.ng(TestElement.DESCRIBE_QUERY);
    v.ng(TestElement.DESCRIBE_TABLE);
    v.ng(TestElement.LIST_FILE);
    v.ng(TestElement.LIST_JAR);
    v.ng(TestElement.REFRESH);
    v.ng(TestElement.REFRESH_TABLE);
    v.ng(TestElement.REFRESH_FUNCTION);
    v.ng(TestElement.RESET);
    v.ng(TestElement.SET);
    v.ng(TestElement.SHOW_COLUMNS);
    v.ng(TestElement.SHOW_CREATE_TABLE);
    v.ng(TestElement.SHOW_DATABASES);
    v.ng(TestElement.SHOW_FUNCTIONS);
    v.ng(TestElement.SHOW_PARTITIONS);
    v.ng(TestElement.SHOW_TABLE_EXTENDED);
    v.ng(TestElement.SHOW_TABLES);
    v.ng(TestElement.SHOW_TBLPROPERTIES);
    v.ng(TestElement.SHOW_VIEWS);
    v.ng(TestElement.UNCACHE_TABLE);

    // Functions
    v.ok(TestElement.ARRAY_FUNCTIONS);
    v.ok(TestElement.MAP_FUNCTIONS);
    v.ok(TestElement.DATE_AND_TIMESTAMP_FUNCTIONS);
    v.ok(TestElement.JSON_FUNCTIONS);
    v.ok(TestElement.MATHEMATICAL_FUNCTIONS);
    v.ok(TestElement.STRING_FUNCTIONS);
    v.ok(TestElement.BITWISE_FUNCTIONS);
    v.ok(TestElement.CONVERSION_FUNCTIONS);
    v.ok(TestElement.CONDITIONAL_FUNCTIONS);
    v.ok(TestElement.PREDICATE_FUNCTIONS);
    v.ng(TestElement.CSV_FUNCTIONS);
    v.ng(TestElement.MISC_FUNCTIONS);

    // Aggregate-like Functions
    v.ok(TestElement.AGGREGATE_FUNCTIONS);
    v.ok(TestElement.WINDOW_FUNCTIONS);

    // Generator Functions
    v.ok(TestElement.GENERATOR_FUNCTIONS);

    // UDFs
    v.ng(TestElement.SCALAR_USER_DEFINED_FUNCTIONS);
    v.ng(TestElement.USER_DEFINED_AGGREGATE_FUNCTIONS);
    v.ng(TestElement.INTEGRATION_WITH_HIVE_UDFS_UDAFS_UDTFS);
  }

  @AllArgsConstructor
  private static class VerifyValidator {
    private final SQLQueryValidator validator;
    private final DataSourceType dataSourceType;

    public void ok(TestElement query) {
      runValidate(query.getQueries());
    }

    public void ng(TestElement query) {
      assertThrows(
          IllegalArgumentException.class,
          () -> runValidate(query.getQueries()),
          "The query should throw: query=`" + query.toString() + "`");
    }

    void runValidate(String[] queries) {
      Arrays.stream(queries).forEach(query -> validator.validate(query, dataSourceType));
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
