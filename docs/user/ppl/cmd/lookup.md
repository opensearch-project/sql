# lookup

The `lookup` command enriches your search results with fields from a second index (a "dimension table" or "lookup table"). For each row in your search results, `lookup` finds the matching row in the lookup index and copies fields from it onto your result.

Think of it as a left join tuned for enrichment: every row from your search is kept, and matched fields from the lookup index are added or overwritten. If there is no match, the added fields are `null`. Compared with the `join` command, `lookup` is simpler and better suited for attaching a static reference dataset (region names, user departments, product categories) to streaming data.

## How to read a lookup clause

A lookup clause has three parts, read left to right:

```
lookup  <lookupIndex>   <matchFields>   [<strategy> <copyFields>]
        ─────────────   ─────────────   ─────────────────────────
        which index     how to match    what to copy over
        to enrich from  rows            (optional)
```

### Match fields: `lookupField AS sourceField`

The match fields tell `lookup` how to pair a row in your results with a row in the lookup index.

> **The name on the left of `AS` is a field in the lookup index. The name on the right is the field in the search results.**

So `lookup work_information uid AS id` reads as:

> "Match each result row where **my** `id` equals `work_information`'s `uid`."

The lookup field comes first because you are describing the lookup index (`uid`) and then saying which of your own fields it lines up with (`id`). If both indexes already use the same field name, you can drop the `AS` entirely: `lookup work_information id` matches `id` to `id`.

You can match on several fields at once with a comma-separated list; a row matches only when all of them are equal. Fields without `AS` match by the same name; you can mix remapped and same-name fields:

```
lookup work_information uid AS id, name            -- match work_information.uid = worker.id AND work_information.name = worker.name
lookup work_information uid AS id, dept AS department  -- both fields remapped
```

### Copy fields: `inputField AS outputField`

After the match fields, you optionally list which fields to copy from the lookup index onto your results, and optionally rename them. This follows the same left-to-right pattern as match fields:

> **The name on the left of `AS` is a field in the LOOKUP index (the source of the value). The name on the right is the field name it has in the results.**

So `replace department AS dept` reads as "copy `work_information.department` onto my results under the name `dept`."

If you list no copy fields at all, `lookup` copies all fields from the lookup index except the ones used for matching.

### Strategy: `replace` vs. `append`

The strategy controls what happens when the output field already exists on your result row:

| Strategy | Behavior when the output field already has a value | Behavior when it is `null` / missing |
| --- | --- | --- |
| `replace` (default) | Overwrites it with the value from the lookup index (even overwriting with `null` on no match) | Fills it in from the lookup index |
| `append` | Keeps your existing value | Fills it in from the lookup index |

`replace` wins; `append` only fills gaps. If the output field does not exist on your results yet, both strategies simply add it. `output` is an accepted synonym for `replace`, provided for SPL compatibility.

## Syntax

```syntax
lookup <lookupIndex> (<lookupMappingField> [as <sourceMappingField>])... [(replace | append | output) (<inputField> [as <outputField>])...]
```

Reading the grammar against the plain-language rules above:

```syntax
source = table1 | lookup table2 id                                     -- match table2.id = table1.id, copy all other table2 fields
source = table1 | lookup table2 id, name                               -- match on both id and name
source = table1 | lookup table2 id as cid                              -- match table2.id = table1.cid
source = table1 | lookup table2 id as cid replace dept as department   -- ...and copy table2.dept into a field named department, overwriting
source = table1 | lookup table2 id as cid append dept as department    -- ...but only fill department where it is currently empty
```

## Parameters

| Parameter | Required/Optional | Description |
| --- | --- | --- |
| `<lookupIndex>` | Required | The lookup index (dimension table) to enrich from. |
| `<lookupMappingField>` | Required | A field in the **lookup index** used for matching. With no `as` clause, a field of the same name is expected in your search results. List several as a comma-separated set; all must match. |
| `<sourceMappingField>` | Optional | The field in **your search results** that `<lookupMappingField>` is matched against. Defaults to the same name as `<lookupMappingField>`. |
| `<inputField>` | Optional | A field in the **lookup index** whose matched value is copied onto your results. List several as a comma-separated set. If omitted, every field in the lookup index except the match fields is copied. |
| `<outputField>` | Optional | The field name on **your results** where the copied value lands. Defaults to `<inputField>`. `replace` can create new fields or overwrite existing ones; `append` fills existing fields only. |
| `(replace \| append \| output)` | Optional | How copied values are applied. `replace` (default) overwrites; `append` fills only missing/`null` values; `output` is a synonym for `replace` (SPL compatibility). |

## Examples

The examples below enrich a `worker` index with a `work_information` lookup index:

`worker` (your search results):

```text
+------+-------+------------+---------+--------+
| id   | name  | occupation | country | salary |
|------+-------+------------+---------+--------|
| 1000 | Jake  | Engineer   | England | 100000 |
| 1001 | Hello | Artist     | USA     | 70000  |
| 1002 | John  | Doctor     | Canada  | 120000 |
| 1003 | David | Doctor     | null    | 120000 |
| 1004 | David | null       | Canada  | 0      |
| 1005 | Jane  | Scientist  | Canada  | 90000  |
+------+-------+------------+---------+--------+
```

`work_information` (the lookup index — note it keys on `uid`, and has no row for `id` 1001 or 1004):

```text
+------+-------+------------+------------+
| uid  | name  | department | occupation |
|------+-------+------------+------------|
| 1000 | Jake  | IT         | Engineer   |
| 1002 | John  | DATA       | Scientist  |
| 1003 | David | HR         | Doctor     |
| 1005 | Jane  | DATA       | Engineer   |
| 1006 | Tom   | SALES      | Artist     |
+------+-------+------------+------------+
```

### Example 1: Copy one field, matching on differently-named keys

Match each worker where `worker.id` equals `work_information.uid`, then copy `department` onto the results. Because `department` does not already exist on `worker`, `replace` simply adds it (and leaves `null` where there was no match).

```ppl
source = worker
  | LOOKUP work_information uid AS id REPLACE department
  | fields id, name, occupation, country, salary, department
  | sort id
```

```text
fetched rows / total rows = 6/6
+------+-------+------------+---------+--------+------------+
| id   | name  | occupation | country | salary | department |
|------+-------+------------+---------+--------+------------|
| 1000 | Jake  | Engineer   | England | 100000 | IT         |
| 1001 | Hello | Artist     | USA     | 70000  | null       |
| 1002 | John  | Doctor     | Canada  | 120000 | DATA       |
| 1003 | David | Doctor     | null    | 120000 | HR         |
| 1004 | David | null       | Canada  | 0      | null       |
| 1005 | Jane  | Scientist  | Canada  | 90000  | DATA       |
+------+-------+------------+---------+--------+------------+
```

### Example 2: `replace` vs. `append` on a field that already has values

The difference between `replace` and `append` only shows when the output field **already holds values**. Here we copy the lookup's `department` onto the existing `country` field to demonstrate overwrite vs. fill-gaps behavior.

With `replace`, the lookup value overwrites `country` for every matched row (and overwrites with `null` where there is no match):

```ppl
source = worker
  | LOOKUP work_information uid AS id REPLACE department AS country
  | fields id, name, occupation, salary, country
  | sort id
```

```text
fetched rows / total rows = 6/6
+------+-------+------------+--------+---------+
| id   | name  | occupation | salary | country |
|------+-------+------------+--------+---------|
| 1000 | Jake  | Engineer   | 100000 | IT      |
| 1001 | Hello | Artist     | 70000  | null    |
| 1002 | John  | Doctor     | 120000 | DATA    |
| 1003 | David | Doctor     | 120000 | HR      |
| 1004 | David | null       | 0      | null    |
| 1005 | Jane  | Scientist  | 90000  | DATA    |
+------+-------+------------+--------+---------+
```

With `append`, the original `country` is kept wherever it already had a value; the lookup value only fills the rows where `country` was `null` (worker 1003):

```ppl
source = worker
  | LOOKUP work_information uid AS id APPEND department AS country
  | fields id, name, occupation, salary, country
  | sort id
```

```text
fetched rows / total rows = 6/6
+------+-------+------------+--------+---------+
| id   | name  | occupation | salary | country |
|------+-------+------------+--------+---------|
| 1000 | Jake  | Engineer   | 100000 | England |
| 1001 | Hello | Artist     | 70000  | USA     |
| 1002 | John  | Doctor     | 120000 | Canada  |
| 1003 | David | Doctor     | 120000 | HR      |
| 1004 | David | null       | 0      | Canada  |
| 1005 | Jane  | Scientist  | 90000  | Canada  |
+------+-------+------------+--------+---------+
```

Only worker 1003 changed: its `country` was `null`, so `append` filled it with `HR` from the lookup. Every other row kept its original `country`.

### Example 3: Copy every field (no copy fields specified)

Omit the copy fields and `lookup` brings over every field in the lookup index except the match fields. Here matching on both `uid AS id` and `name`, so `department` and `occupation` are copied:

```ppl
source = worker
  | LOOKUP work_information uid AS id, name
  | fields id, name, occupation, country, salary, department
  | sort id
```

```text
fetched rows / total rows = 6/6
+------+-------+------------+---------+--------+------------+
| id   | name  | occupation | country | salary | department |
|------+-------+------------+---------+--------+------------|
| 1000 | Jake  | Engineer   | England | 100000 | IT         |
| 1001 | Hello | null       | USA     | 70000  | null       |
| 1002 | John  | Scientist  | Canada  | 120000 | DATA       |
| 1003 | David | Doctor     | null    | 120000 | HR         |
| 1004 | David | null       | Canada  | 0      | null       |
| 1005 | Jane  | Engineer   | Canada  | 90000  | DATA       |
+------+-------+------------+---------+--------+------------+
```

`occupation` already existed on `worker`; because the default strategy is `replace`, it is overwritten by the lookup's `occupation` (worker 1002's `Doctor` becomes `Scientist`, worker 1001's `Artist` becomes `null`).

### Example 4: Copy into a brand-new field

Set the output field to a name that does not exist on your results, and `lookup` adds it as a new column while leaving the original untouched. Here `occupation` from the lookup lands in a new field `new_col`, matching on `name`:

```ppl
source = worker
  | LOOKUP work_information name REPLACE occupation AS new_col
  | fields id, name, occupation, country, salary, new_col
  | sort id
```

```text
fetched rows / total rows = 6/6
+------+-------+------------+---------+--------+-----------+
| id   | name  | occupation | country | salary | new_col   |
|------+-------+------------+---------+--------+-----------|
| 1000 | Jake  | Engineer   | England | 100000 | Engineer  |
| 1001 | Hello | Artist     | USA     | 70000  | null      |
| 1002 | John  | Doctor     | Canada  | 120000 | Scientist |
| 1003 | David | Doctor     | null    | 120000 | Doctor    |
| 1004 | David | null       | Canada  | 0      | Doctor    |
| 1005 | Jane  | Scientist  | Canada  | 90000  | Engineer  |
+------+-------+------------+---------+--------+-----------+
```

Note both David rows (1003, 1004) get `Doctor` in `new_col` because they match the same lookup row by `name`.

### Example 5: `output` keyword

`output` is a synonym for `replace`, provided for compatibility with SPL. The following produces the same result as Example 1:

```ppl
source = worker
  | LOOKUP work_information uid AS id OUTPUT department
  | fields id, name, occupation, country, salary, department
  | sort id
```

> **Note:** `append` works with existing fields only. To copy a value into a new field with a different name, use `replace`.
