===
top
===

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2


Description
===========
| The ``top`` command finds the most common tuple of values of all fields in the field list.

Syntax
======
top [N] [top-options] <field-list> [by-clause]

* N: optional. number of results to return. **Default**: 10
* top-options: optional. options for the top command. Supported syntax is [countfield=<string>] [showcount=<bool>].
    * showcount=<bool>: optional. whether to create a field in output that represent a count of the tuple of values. **Default:** true.
    * countfield=<string>: optional. the name of the field that contains count. **Default:** 'count'.
    * usenull=<bool>: optional (since 3.4.0). whether to output the null value. **Default:** Determined by ``plugins.ppl.syntax.legacy.preferred``:

        * When ``plugins.ppl.syntax.legacy.preferred=true``, ``usenull`` defaults to ``true``
        * When ``plugins.ppl.syntax.legacy.preferred=false``, ``usenull`` defaults to ``false``

* field-list: mandatory. comma-delimited list of field names.
* by-clause: optional. one or more fields to group the results by.

Example 1: Find the most common values in a field
=================================================

This example finds the most common gender of all the accounts.

PPL query::

    os> source=accounts | top showcount=false gender;
    fetched rows / total rows = 2/2
    +--------+
    | gender |
    |--------|
    | M      |
    | F      |
    +--------+

Example 2: Limit results to top N values
========================================

This example finds the most common gender and limits results to 1 value.

PPL query::

    os> source=accounts | top 1 showcount=false gender;
    fetched rows / total rows = 1/1
    +--------+
    | gender |
    |--------|
    | M      |
    +--------+

Example 3: Find the most common values grouped by field
=======================================================

This example finds the most common age of all the accounts grouped by gender.

PPL query::

    os> source=accounts | top 1 showcount=false age by gender;
    fetched rows / total rows = 2/2
    +--------+-----+
    | gender | age |
    |--------+-----|
    | F      | 28  |
    | M      | 32  |
    +--------+-----+

Example 4: Top command with count field
=======================================

This example finds the most common gender of all the accounts and includes the count.

PPL query::

    os> source=accounts | top gender;
    fetched rows / total rows = 2/2
    +--------+-------+
    | gender | count |
    |--------+-------|
    | M      | 3     |
    | F      | 1     |
    +--------+-------+


Example 5: Specify the count field option
=========================================

This example specifies a custom name for the count field.

PPL query::

    os> source=accounts | top countfield='cnt' gender;
    fetched rows / total rows = 2/2
    +--------+-----+
    | gender | cnt |
    |--------+-----|
    | M      | 3   |
    | F      | 1   |
    +--------+-----+


Example 5: Specify the usenull field option
===========================================

PPL query::

    os> source=accounts | top usenull=false email;
    fetched rows / total rows = 3/3
    +-----------------------+-------+
    | email                 | count |
    |-----------------------+-------|
    | amberduke@pyrami.com  | 1     |
    | daleadams@boink.com   | 1     |
    | hattiebond@netagy.com | 1     |
    +-----------------------+-------+

PPL query::

    os> source=accounts | top usenull=true email;
    fetched rows / total rows = 4/4
    +-----------------------+-------+
    | email                 | count |
    |-----------------------+-------|
    | null                  | 1     |
    | amberduke@pyrami.com  | 1     |
    | daleadams@boink.com   | 1     |
    | hattiebond@netagy.com | 1     |
    +-----------------------+-------+


Limitations
===========
The ``top`` command is not rewritten to OpenSearch DSL, it is only executed on the coordination node.
