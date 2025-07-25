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
The ``table`` command processes the entire result set and filters it to include only the specified fields.


Field Renaming
============
The ``table`` command returns fields with their original names and does not support field renaming within the command itself. To rename fields, use the ``rename`` command before ``table``.


Best Practices
==============

* Place the ``table`` command at the end of search pipelines for optimal performance
* Use wildcards to select groups of related fields efficiently
* Perform field renaming before using the ``table`` command
* For large result sets, consider limiting the number of fields to improve performance


Examples
========

Example 1: Basic field selection
--------------------------------

PPL query::

    os> source=accounts | table account_number, firstname, lastname;

Returns results containing only the account_number, firstname, and lastname fields.


Example 2: Using wildcards
--------------------------

PPL query::

    os> source=employees | table ENAME, JOB, DEPT*;

Returns results containing ENAME, JOB, and any fields matching the DEPT* pattern.


Example 3: Using renamed fields
-------------------------------

PPL query::

    os> source=employees | rename EMPNO as emp_id, ENAME as emp_name | table emp_id, emp_name, JOB;

Returns results containing only the emp_id, emp_name, and JOB fields.


Example 4: Sorting and filtering with table
-------------------------------------------

PPL query::

    os> source=employees | where SAL > 1000 | sort - SAL | table ENAME, SAL, DEPTNO | head 3;

Returns the top 3 results containing only ENAME, SAL, and DEPTNO fields.


Example 5: Multiple wildcard patterns
-------------------------------------

PPL query::

    os> source=employees | table *NAME, *NO, JOB;

Returns results containing fields matching *NAME, *NO patterns, and the JOB field.


Example 6: Table with evaluation
-------------------------------

PPL query::

    os> source=employees | dedup DEPTNO | eval dept_type=case(DEPTNO=10, 'accounting' else 'other') | table EMPNO, dept_type;

Returns results containing only EMPNO and dept_type fields.
