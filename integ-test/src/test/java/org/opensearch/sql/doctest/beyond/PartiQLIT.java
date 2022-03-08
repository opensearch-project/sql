/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.doctest.beyond;

import static org.opensearch.sql.doctest.core.TestData.TEST_DATA_FOLDER_ROOT;
import static org.opensearch.sql.util.TestUtils.getResourceFilePath;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.opensearch.sql.doctest.core.DocTest;
import org.opensearch.sql.doctest.core.annotation.DocTestConfig;
import org.opensearch.sql.doctest.core.annotation.Section;
import org.opensearch.sql.doctest.core.builder.Example;
import org.opensearch.sql.legacy.utils.JsonPrettyFormatter;

@DocTestConfig(template = "beyond/partiql.rst", testData = {"employees_nested.json"})
public class PartiQLIT extends DocTest {

  @Section(1)
  public void showTestData() {
    section(
        title("Test Data"),
        description(
            "The test index ``employees_nested`` used by all examples in this document is very similar to",
            "the one used in official PartiQL documentation."
        ),
        createDummyExampleForTestData("employees_nested.json")
    );
  }

  @Section(2)
  public void queryNestedCollection() {
    section(
        title("Querying Nested Collection"),
        description(
            "In SQL-92, a database table can only have tuples that consists of scalar values.",
            "PartiQL extends SQL-92 to allow you query and unnest nested collection conveniently.",
            "In OpenSearch world, this is very useful for index with object or nested field."
        ),
        example(
            title("Unnesting a Nested Collection"),
            description(
                "In the following example, it finds nested document (project) with field value (name)",
                "that satisfies the predicate (contains 'security'). Note that because each parent document",
                "can have more than one nested documents, the matched nested document is flattened. In other",
                "word, the final result is the Cartesian Product between parent and nested documents."
            ),
            post(multiLine(
                "SELECT e.name AS employeeName,",
                "       p.name AS projectName",
                "FROM employees_nested AS e,",
                "     e.projects AS p",
                "WHERE p.name LIKE '%security%'"
            ))
        ),
            /*
            Issue: https://github.com/opendistro-for-elasticsearch/sql/issues/397
            example(
                title("Preserving Parent Information with LEFT JOIN"),
                description(
                    "The query in the preceding example is very similar to traditional join queries, except ``ON`` clause missing.",
                    "This is because it is implicitly in the nesting of nested documents (projects) into parent (employee). Therefore,",
                    "you can use ``LEFT JOIN`` to preserve the information in parent document associated."
                ),
                post(
                    "SELECT e.id AS id, " +
                    "       e.name AS employeeName, " +
                    "       e.title AS title, " +
                    "       p.name AS projectName " +
                    "FROM employees_nested AS e " +
                    "LEFT JOIN e.projects AS p"
                )
            )*/
        example(
            title("Unnesting in Existential Subquery"),
            description(
                "Alternatively, a nested collection can be unnested in subquery to check if it",
                "satisfies a condition."
            ),
            post(multiLine(
                "SELECT e.name AS employeeName",
                "FROM employees_nested AS e",
                "WHERE EXISTS (",
                "  SELECT *",
                "  FROM e.projects AS p",
                "  WHERE p.name LIKE '%security%'",
                ")"
            ))
        )/*,
            Issue: https://github.com/opendistro-for-elasticsearch/sql/issues/398
            example(
                title("Aggregating over a Nested Collection"),
                description(
                    "After unnested, a nested collection can be aggregated just like a regular field."
                ),
                post(multiLine(
                    "SELECT",
                    "  e.name AS employeeName,",
                    "  COUNT(p) AS cnt",
                    "FROM employees_nested AS e,",
                    "     e.projects AS p",
                    "WHERE p.name LIKE '%security%'",
                    "GROUP BY e.id, e.name",
                    "HAVING COUNT(p) >= 1"
                )
            ))
            */
    );
  }

  private Example createDummyExampleForTestData(String fileName) {
    Example example = new Example();
    example.setTitle("Employees");
    example.setDescription("");
    example.setResult(parseJsonFromTestData(fileName));
    return example;
  }

  /**
   * Concat and pretty format document at odd number line in bulk request file
   */
  private String parseJsonFromTestData(String fileName) {
    Path path = Paths.get(getResourceFilePath(TEST_DATA_FOLDER_ROOT + fileName));
    try {
      List<String> lines = Files.readAllLines(path);
      String json = IntStream.range(0, lines.size()).
          filter(i -> i % 2 == 1).
          mapToObj(lines::get).
          collect(Collectors.joining(",", "{\"employees\":[", "]}"));
      return JsonPrettyFormatter.format(json);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to load test data: " + path, e);
    }
  }

}
