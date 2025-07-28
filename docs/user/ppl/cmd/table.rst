=============
table
=============

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2


Description
============
The ``table`` command provides field selection operations. It returns search results with only the specified fields.

This command is useful for:

* Selecting specific fields from search results
* Filtering result sets to include only fields of interest
* Controlling field order in results
* Using wildcard patterns for field selection

For optimal performance, place the ``table`` command at the end of your search pipeline.


Syntax
============
table <wc-field-list>


Arguments
============
<wc-field-list>
  A space or comma-delimited list of field names with wildcard support. Supports various wildcard patterns using asterisk (*):
  
  * Prefix wildcard: ``EMP*`` (matches fields starting with "EMP")
  * Suffix wildcard: ``*NO`` (matches fields ending with "NO")
  * Contains wildcard: ``*E*`` (matches fields containing "E")
  * Mixed explicit and wildcard: ``ENAME, EMP*, JOB``


Usage
============
The ``table`` command processes the entire result set and filters it to include only the specified fields. Duplicate field names in the field list are automatically de-duplicated.


Field Renaming
============
The ``table`` command returns fields with their original names and does not support field renaming within the command itself. To rename fields, use the ``rename`` command before ``table``.


Comparison with fields Command
==============================

The ``table`` and ``fields`` commands are both used for field selection but have key differences:

**Similarities:**

* Both commands filter search results to include specific fields
* Both can be used to reduce result set size for better performance

**Differences:**

+------------------+------------------------+------------------------+
| Aspect           | table                  | fields                 |
+==================+========================+========================+
| **Syntax**       | ``table <field-list>``| ``fields [+|-] <field-list>`` |
+------------------+------------------------+------------------------+
| **Primary Use**  | Field selection only  | Keep (+) or remove (-) fields |
+------------------+------------------------+------------------------+
| **Field List Format** | Space or comma-delimited | Comma-delimited only |
+------------------+------------------------+------------------------+
| **Wildcard Support** | Yes (*, prefix, suffix, contains) | No |
+------------------+------------------------+------------------------+
| **Field Removal**| Not supported          | Supported with ``-`` prefix |
+------------------+------------------------+------------------------+
| **Deduplication**| Automatic              | Not applicable         |
+------------------+------------------------+------------------------+
| **Default Behavior** | Always keeps specified fields | Keeps fields (+ is default) |
+------------------+------------------------+------------------------+

**When to use table:**

* When you want to control field order in results
* When you only need to keep specific fields (no removal needed)

**When to use fields:**

* When you need to remove specific fields from results
* When you want explicit control over keep/remove behavior


Best Practices
==============

* Place the ``table`` command at the end of search pipelines for optimal performance
* Use wildcards to select groups of related fields efficiently
* Perform field renaming before using the ``table`` command
* For large result sets, consider limiting the number of fields to improve performance
* Choose ``table`` over ``fields`` when you want to control field ordering
* Choose ``fields`` over ``table`` when you need to remove specific fields


Examples
========

Example 1: Basic field selection
--------------------------------

PPL query::

    os> source=accounts | table account_number, firstname, lastname;
    fetched rows / total rows = 4/4
    +----------------+-----------+----------+
    | account_number | firstname | lastname |
    |----------------+-----------+----------|
    | 1              | Amber     | Duke     |
    | 6              | Hattie    | Bond     |
    | 13             | Nanette   | Bates    |
    | 18             | Dale      | Adams    |
    +----------------+-----------+----------+


Example 2: Using wildcards and field deduplication
---------------------------------------------------

This example shows wildcard usage and automatic field deduplication. The field ``ENAME`` appears in both the wildcard pattern ``*NAME`` and is explicitly specified, but only appears once in the result::

    os> source=employees | table *NAME, ENAME, JOB, DEPT*;
    fetched rows / total rows = 4/4
    +--------+----------+-----------+--------+----------+
    | ENAME  | DEPTNAME | JOB       | DEPTNO | DEPTNAME |
    |--------+----------+-----------+--------+----------|
    | SMITH  | RESEARCH | CLERK     | 20     | RESEARCH |
    | ALLEN  | SALES    | SALESMAN  | 30     | SALES    |
    | WARD   | SALES    | SALESMAN  | 30     | SALES    |
    | JONES  | RESEARCH | MANAGER   | 20     | RESEARCH |
    +--------+----------+-----------+--------+----------+


Example 3: Using renamed fields
-------------------------------

PPL query::

    os> source=employees | rename EMPNO as emp_id, ENAME as emp_name | table emp_id, emp_name, JOB;
    fetched rows / total rows = 4/4
    +--------+----------+-----------+
    | emp_id | emp_name | JOB       |
    |--------+----------+-----------|
    | 7369   | SMITH    | CLERK     |
    | 7499   | ALLEN    | SALESMAN  |
    | 7521   | WARD     | SALESMAN  |
    | 7566   | JONES    | MANAGER   |
    +--------+----------+-----------+


Example 4: Sorting and filtering with table
-------------------------------------------

PPL query::

    os> source=employees | where SAL > 1000 | sort - SAL | table ENAME, SAL, DEPTNO | head 3;
    fetched rows / total rows = 3/3
    +-------+------+--------+
    | ENAME | SAL  | DEPTNO |
    |-------+------+--------|
    | KING  | 5000 | 10     |
    | SCOTT | 3000 | 20     |
    | FORD  | 3000 | 20     |
    +-------+------+--------+


Example 5: Multiple wildcard patterns
-------------------------------------

PPL query::

    os> source=employees | table *NAME, *NO, JOB;
    fetched rows / total rows = 4/4
    +--------+--------+--------+-----------+
    | ENAME  | EMPNO  | DEPTNO | JOB       |
    |--------+--------+--------+-----------|
    | SMITH  | 7369   | 20     | CLERK     |
    | ALLEN  | 7499   | 30     | SALESMAN  |
    | WARD   | 7521   | 30     | SALESMAN  |
    | JONES  | 7566   | 20     | MANAGER   |
    +--------+--------+--------+-----------+


Example 6: Field deduplication with explicit field selection
------------------------------------------------------------

This example demonstrates deduplication when the same field is explicitly listed multiple times::

    os> source=employees | table ENAME, JOB, ENAME, DEPTNO;
    fetched rows / total rows = 4/4
    +--------+-----------+--------+
    | ENAME  | JOB       | DEPTNO |
    |--------+-----------+--------|
    | SMITH  | CLERK     | 20     |
    | ALLEN  | SALESMAN  | 30     |
    | WARD   | SALESMAN  | 30     |
    | JONES  | MANAGER   | 20     |
    +--------+-----------+--------+


Example 7: Table with evaluation
-------------------------------

PPL query::

    os> source=employees | dedup DEPTNO | eval dept_type=case(DEPTNO=10, 'accounting' else 'other') | table EMPNO, dept_type;
    fetched rows / total rows = 3/3
    +-------+------------+
    | EMPNO | dept_type  |
    |-------+------------|
    | 7782  | accounting |
    | 7369  | other      |
    | 7499  | other      |
    +-------+------------+


Example 8: Comparison with fields command
-----------------------------------------

Using ``table`` to select fields::

    os> source=accounts | table account_number, firstname, lastname;
    fetched rows / total rows = 4/4
    +----------------+-----------+----------+
    | account_number | firstname | lastname |
    |----------------+-----------+----------|
    | 1              | Amber     | Duke     |
    | 6              | Hattie    | Bond     |
    | 13             | Nanette   | Bates    |
    | 18             | Dale      | Adams    |
    +----------------+-----------+----------+

Equivalent using ``fields`` command::

    os> source=accounts | fields account_number, firstname, lastname;
    fetched rows / total rows = 4/4
    +----------------+-----------+----------+
    | account_number | firstname | lastname |
    |----------------+-----------+----------|
    | 1              | Amber     | Duke     |
    | 6              | Hattie    | Bond     |
    | 13             | Nanette   | Bates    |
    | 18             | Dale      | Adams    |
    +----------------+-----------+----------+

Using ``fields`` to remove fields (not possible with ``table``)::

    os> source=accounts | fields - account_number;
    fetched rows / total rows = 4/4
    +-----------+----------+-----+--------+-------+---------+
    | firstname | lastname | age | city   | state | balance |
    |-----------+----------+-----+--------+-------+---------|
    | Amber     | Duke     | 32  | Brogan | IL    | 39225   |
    | Hattie    | Bond     | 36  | Dante  | TN    | 5686    |
    | Nanette   | Bates    | 28  | Nogal  | VA    | 32838   |
    | Dale      | Adams    | 33  | Orick  | MD    | 4180    |
    +-----------+----------+-----+--------+-------+---------+
