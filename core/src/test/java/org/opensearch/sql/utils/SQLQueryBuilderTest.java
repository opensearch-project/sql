package org.opensearch.sql.utils;
/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import org.junit.jupiter.api.Test;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.opensearch.sql.utils.Builder.allOf;
import static org.opensearch.sql.utils.Builder.and;
import static org.opensearch.sql.utils.Builder.anyOf;
import static org.opensearch.sql.utils.Builder.equalTo;
import static org.opensearch.sql.utils.Builder.exists;
import static org.opensearch.sql.utils.Builder.in;
import static org.opensearch.sql.utils.Builder.notEqualTo;
import static org.opensearch.sql.utils.Builder.notExists;
import static org.opensearch.sql.utils.Builder.notIn;
import static org.opensearch.sql.utils.Builder.or;

public class SQLQueryBuilderTest {
    @Test
    public void testSelect() {
        var queryBuilder = Builder.select(":a as a", "b", "c", "d")
                .from("A")
                .join("B").on("A.id = B.id", and("x = 50"))
                .leftJoin("C").on("B.id = C.id", and("b = :b"))
                .rightJoin("D").on("C.id = D.id", and("c = :c"))
                .where("a > 10", or("b < 200", and("d != ?")))
                .orderBy("a", "b")
                .limit(10)
                .forUpdate()
                .union(Builder.select("a", "b", "c", "d").from("C").where("c = :c"));

        assertEquals(Arrays.asList("a", "b", "c", null, "c"), queryBuilder.getParameters());

        assertEquals("select ? as a, b, c, d from A "
                + "join B on A.id = B.id and x = 50 "
                + "left join C on B.id = C.id and b = ? "
                + "right join D on C.id = D.id and c = ? "
                + "where a > 10 or (b < 200 and d != ?) "
                + "order by a, b "
                + "limit 10 "
                + "for update "
                + "union select a, b, c, d from C where c = ?", queryBuilder.getSQL());
    }

    @Test
    public void testSelectWithSubquery() {
        var queryBuilder = Builder.select("*")
                .from(Builder.select("a, b, c").from("A"), "a")
                .where("d = :d");

        assertEquals(Arrays.asList("d"), queryBuilder.getParameters());

        assertEquals("select * from (select a, b, c from A) a where d = ?", queryBuilder.getSQL());
    }

    @Test
    public void testSelectWithSubqueryJoin() {
        var queryBuilder = Builder.select("*").from("A")
                .join(Builder.select("c, d").from("C"), "c").on("c = :c")
                .where("d = :d");

        assertEquals(Arrays.asList("c", "d"), queryBuilder.getParameters());

        assertEquals("select * from A join (select c, d from C) c on c = ? where d = ?", queryBuilder.getSQL());
    }

    @Test
    public void testInsertInto() {
        var queryBuilder = Builder.insertInto("A").values(mapOf(
                entry("a", 1),
                entry("b", true),
                entry("c", "hello"),
                entry("d", ":d"),
                entry("e", "?"),
                entry("f", Builder.select("f").from("F").where("g = :g"))
        ));
        assertEquals(Arrays.asList("d", null, "g"),queryBuilder.getParameters());
        assertEquals("insert into A (a, b, c, d, e, f) values (1, true, 'hello', ?, ?, (select f from F where g = ?))", queryBuilder.getSQL());
    }

    @Test
    public void testUpdate() {
        var queryBuilder = Builder.update("A").set(mapOf(
                entry("a", 1),
                entry("b", true),
                entry("c", "hello"),
                entry("d", ":d"),
                entry("e", "?"),
                entry("f", Builder.select("f").from("F").where("g = :g")))
        ).where("a is not null");

        assertEquals(Arrays.asList("d", null, "g"), queryBuilder.getParameters());

        assertEquals("update A set a = 1, b = true, c = 'hello', d = ?, e = ?, f = (select f from F where g = ?) where a is not null", queryBuilder.getSQL());
    }

    @Test
    public void testUpdateWithExpression() {
        var queryBuilder = Builder.update("xyz").set(mapOf(
                entry("foo", ":a + b")
        )).where("c = :d");

        assertEquals(Arrays.asList("a", "d"), queryBuilder.getParameters());

        assertEquals("update xyz set foo = ? + b where c = ?", queryBuilder.getSQL());
    }

    @Test
    public void testDelete() {
        var queryBuilder = Builder.deleteFrom("A").where("a < 150");

        assertEquals("delete from A where a < 150", queryBuilder.getSQL());
    }

    @Test
    public void testConditionalGroups() {
        var queryBuilder = Builder.select("*").from("xyz").where(allOf("a = 1", "b = 2", "c = 3"), and(anyOf("d = 4", "e = 5")));

        assertEquals("select * from xyz where (a = 1 and b = 2 and c = 3) and (d = 4 or e = 5)", queryBuilder.getSQL());
    }

    @Test
    public void testEqualToConditional() {
        var queryBuilder = Builder.select("*")
                .from("A")
                .where("b", equalTo(
                        Builder.select("b").from("B").where("c = :c")
                ));

        assertEquals("select * from A where b = (select b from B where c = ?)", queryBuilder.getSQL());
    }

    @Test
    public void testNotEqualToConditional() {
        var queryBuilder = Builder.select("*")
                .from("A")
                .where("b", notEqualTo(
                        Builder.select("b").from("B").where("c = :c")
                ));

        assertEquals("select * from A where b != (select b from B where c = ?)", queryBuilder.getSQL());
    }

    @Test
    public void testInConditional() {
        var queryBuilder = Builder.select("*")
                .from("B")
                .where("c", in(
                        Builder.select("c").from("C").where("d = :d")
                ));

        assertEquals("select * from B where c in (select c from C where d = ?)", queryBuilder.getSQL());
    }

    @Test
    public void testNotInConditional() {
        var queryBuilder = Builder.select("*").from("D").where("e", notIn(
                Builder.select("e").from("E")
        ));

        assertEquals("select * from D where e not in (select e from E)", queryBuilder.getSQL());
    }

    @Test
    public void testExistsConditional() {
        var queryBuilder = Builder.select("*")
                .from("B")
                .where(exists(
                        Builder.select("c").from("C").where("d = :d")
                ));

        assertEquals("select * from B where exists (select c from C where d = ?)", queryBuilder.getSQL());
    }

    @Test
    public void testNotExistsConditional() {
        var queryBuilder = Builder.select("*").from("D").where("e", notExists(
                Builder.select("e").from("E")
        ));

        assertEquals("select * from D where e not exists (select e from E)", queryBuilder.getSQL());
    }

    @Test
    public void testQuotedColon() {
        var queryBuilder = Builder.select("*").from("xyz").where("foo = 'a:b:c'");

        assertEquals("select * from xyz where foo = 'a:b:c'", queryBuilder.getSQL());
    }

    @Test
    public void testQuotedQuestionMark() {
        var queryBuilder = Builder.select("'?' as q").from("xyz");

        assertEquals("select '?' as q from xyz", queryBuilder.getSQL());
    }

    @Test
    public void testDoubleColon() {
        assertThrows(IllegalArgumentException.class, () -> Builder.select("'ab:c'::varchar(16) as abc"));
    }

    @Test
    public void testEscapedQuotes() {
        var queryBuilder = Builder.select("xyz.*", "''':z' as z").from("xyz").where("foo = 'a''b'':c'''", and("bar = ''''"));

        assertEquals("select xyz.*, ''':z' as z from xyz where foo = 'a''b'':c''' and bar = ''''", queryBuilder.getSQL());
    }

    @Test
    public void testMissingPredicateParameterName() {
        assertThrows(IllegalArgumentException.class, () -> Builder.select("*").from("xyz").where("foo = :"));
    }

    @Test
    public void testMissingValueParameterName() {
        assertThrows(IllegalArgumentException.class, () -> Builder.insertInto("xyz").values(mapOf(entry("foo", ":"))
        ));
    }

    @Test
    public void testExistingSQL() {
        var queryBuilder = new Builder("select a, 'b''c:d' as b from foo where bar = :x");

        assertEquals(Arrays.asList("x"), queryBuilder.getParameters());

        assertEquals("select a, 'b''c:d' as b from foo where bar = ?", queryBuilder.getSQL());
    }

    @Test
    public void testToString() {
        var queryBuilder = Builder.select("*").from("xyz").where("foo = :a", and("bar = :b", or("bar = :c")));

        assertEquals("select * from xyz where foo = :a and (bar = :b or bar = :c)", queryBuilder.toString());
    }


    @SafeVarargs
    public static <K, V> Map<K, V> mapOf(Map.Entry<K, V>... entries) {
        if (entries == null) {
            throw new IllegalArgumentException();
        }

        Map<K, V> map = new LinkedHashMap<>();

        for (var entry : entries) {
            map.put(entry.getKey(), entry.getValue());
        }

        return java.util.Collections.unmodifiableMap(map);
    }

    /**
     * Creates an immutable map entry.
     *
     * @param <K>   The key type.
     * @param <V>   The value type.
     * @param key   The entry key.
     * @param value The entry value.
     * @return An immutable map entry containing the provided key/value pair.
     */
    public static <K, V> Map.Entry<K, V> entry(K key, V value) {
        return new AbstractMap.SimpleImmutableEntry<>(key, value);
    }


}

