=============
table
=============

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2


Description
============
The ``table`` command provides tabular data presentation and field selection operations. It displays search results in a tabular format with only the specified fields, creating focused tabular views for better data visualization and readability.

This command is useful for:

* Creating focused tabular views of search results with only relevant fields
* Presenting data in a structured format for better readability
* Simplifying complex data sets by displaying only fields of interest
* Controlling column order in result presentation

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
The ``table`` command processes the entire result set and formats it for tabular presentation.


Field Renaming
============
The ``table`` command displays fields with their original names and does not support field renaming within the command itself. To rename fields, use the ``rename`` command before ``table``.


Best Practices
==============

* Place the ``table`` command at the end of search pipelines for optimal performance
* Use wildcards to select groups of related fields efficiently
* Perform field renaming before using the ``table`` command
* Use the ``fields`` command for filtering operations and ``table`` for presentation
* For large result sets, consider limiting the number of fields to improve performance


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


Example 2: Using wildcards
--------------------------

PPL query::

    os> source=employees | table ENAME, JOB, DEPT*;
    fetched rows / total rows = 5/5
    +--------+----------+--------+
    | ENAME  | JOB      | DEPTNO |
    |--------+----------+--------|
    | SMITH  | CLERK    | 20     |
    | ALLEN  | SALESMAN | 30     |
    | WARD   | SALESMAN | 30     |
    | JONES  | MANAGER  | 20     |
    | MARTIN | SALESMAN | 30     |
    +--------+----------+--------+


Example 3: Using renamed fields
-------------------------------

PPL query::

    os> source=employees | rename EMPNO as emp_id, ENAME as emp_name | table emp_id, emp_name, JOB;
    fetched rows / total rows = 5/5
    +--------+----------+----------+
    | emp_id | emp_name | JOB      |
    |--------+----------+----------|
    | 7369   | SMITH    | CLERK    |
    | 7499   | ALLEN    | SALESMAN |
    | 7521   | WARD     | SALESMAN |
    | 7566   | JONES    | MANAGER  |
    | 7654   | MARTIN   | SALESMAN |
    +--------+----------+----------+


Example 4: Sorting and filtering with table
-------------------------------------------

PPL query::

    os> source=employees | where SAL > 1000 | sort - SAL | table ENAME, SAL, DEPTNO | head 3;
    fetched rows / total rows = 3/3
    +-------+--------+--------+
    | ENAME | SAL    | DEPTNO |
    |-------+--------+--------|
    | KING  | 5000.0 | 10     |
    | SCOTT | 3000.0 | 20     |
    | FORD  | 3000.0 | 20     |
    +-------+--------+--------+


Example 5: Multiple wildcard patterns
-------------------------------------

PPL query::

    os> source=employees | table *NAME, *NO, JOB;
    fetched rows / total rows = 5/5
    +--------+--------+----------+
    | ENAME  | DEPTNO | JOB      |
    |--------+--------+----------|
    | SMITH  | 20     | CLERK    |
    | ALLEN  | 30     | SALESMAN |
    | WARD   | 30     | SALESMAN |
    | JONES  | 20     | MANAGER  |
    | MARTIN | 30     | SALESMAN |
    +--------+--------+----------+


Example 6: Table with evaluation
-------------------------------

PPL query::

    os> source=employees | dedup DEPTNO | eval dept_type=case(DEPTNO=10, 'accounting' else 'other') | table EMPNO, dept_type;
    fetched rows / total rows = 3/3
    +-------+------------+
    | EMPNO | dept_type  |
    |-------+------------|
    | 7782  | accounting |
    | 7369  | other      |
    | 7839  | other      |
    +-------+------------+
