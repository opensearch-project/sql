=============
table
=============

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2


Description
============
The ``table`` command returns a table that is formed by only the fields that you specify in the arguments. Columns are displayed in the same order that fields are specified. Column headers are the field names. Rows are the field values. Each row represents an event.

The ``table`` command is similar to the ``fields`` command in that it lets you specify the fields you want to keep in your results. Use ``table`` command when you want to retain data in tabular format.

With the exception of a scatter plot to show trends in the relationships between discrete values of your data, you should not use the ``table`` command for charts.

To optimize searches, avoid putting the ``table`` command in the middle of your searches and instead, put it at the end of your searches.


Syntax
============
table <wc-field-list>


Arguments
============
<wc-field-list>
  A list of valid field names. The list can be space-delimited or comma-delimited. You can use the asterisk (``*``) as a wildcard to specify a list of fields with similar names. For example, if you want to specify all fields that start with "value", you can use a wildcard such as ``value*``.


Usage
============
The ``table`` command is a transforming command.


Command Type
============
The ``table`` command is a non-streaming command. If you are looking for a streaming command similar to the ``table`` command, use the ``fields`` command.


Field Renaming
============
The ``table`` command doesn't let you rename fields, only specify the fields that you want to show in your tabulated results. If you're going to rename a field, do it before piping the results to ``table``.


Field Selection Patterns
========================

* ``field*``: Fields starting with "field"
* ``*_time``: Fields ending with "_time"
* ``log_*_data``: Fields matching pattern with wildcards


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


Example 2: Wildcard field selection
----------------------------------

PPL query::

    os> source=logs | table time, host*, error_code;
    fetched rows / total rows = 3/3
    +----------+-----------+------------+------------+
    | time     | host_name | host_ip    | error_code |
    |----------+-----------+------------+------------|
    | 10:30:00 | server1   | 10.0.0.1   | 404        |
    | 10:31:00 | server2   | 10.0.0.2   | 500        |
    | 10:32:00 | server1   | 10.0.0.1   | 200        |
    +----------+-----------+------------+------------+


Example 3: Network classification
--------------------------------

PPL query::

    os> source=access_logs | dedup clientip | eval network=if(cidrmatch("192.0.0.0/16", clientip), "local", "other") | table clientip, network;
    fetched rows / total rows = 5/5
    +---------------+---------+
    | clientip      | network |
    |---------------+---------|
    | 192.0.1.51    | other   |
    | 192.168.11.33 | other   |
    | 192.168.11.44 | other   |
    | 192.1.2.40    | other   |
    | 192.0.1.39    | local   |
    +---------------+---------+


Example 4: Multiple field pattern selection
-----------------------------------------

PPL query::

    os> source=access_logs | table host, action, date_m*;
    fetched rows / total rows = 5/5
    +------+------------+----------+------------+-----------+
    | host | action     | date_mday | date_minute | date_month |
    |------+------------+----------+------------+-----------|
    | www1 |            | 20       | 51         | july      |
    | www1 |            | 20       | 48         | july      |
    | www1 |            | 20       | 48         | july      |
    | www1 | addtocart  | 20       | 48         | july      |
    | www1 |            | 20       | 48         | july      |
    +------+------------+----------+------------+-----------+
