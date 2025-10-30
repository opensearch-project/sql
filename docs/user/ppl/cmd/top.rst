===
top
===

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2


Description
===========
| Using ``top`` command to find the most common tuple of values of all fields in the field list.


Syntax
======
top [N] <field-list> [by-clause]

top [N] [top-options] <field-list> [by-clause] ``(available from 3.1.0+)``

* N: number of results to return. **Default**: 10
* field-list: mandatory. comma-delimited list of field names.
* by-clause: optional. one or more fields to group the results by.
* top-options: optional. options for the top command. Supported syntax is [countfield=<string>] [showcount=<bool>].
* showcount=<bool>: optional. whether to create a field in output that represent a count of the tuple of values. Default value is ``true``.
* countfield=<string>: optional. the name of the field that contains count. Default value is ``'count'``.
* usenull=<bool>: optional (since 3.4.0). whether to output the null value. The default value of ``usenull`` is determined by ``plugins.ppl.syntax.legacy.preferred``:

 * When ``plugins.ppl.syntax.legacy.preferred=true``, ``usenull`` defaults to ``true``
 * When ``plugins.ppl.syntax.legacy.preferred=false``, ``usenull`` defaults to ``false``

Example 1: Find the most common values in a field
=================================================

The example finds most common gender of all the accounts.

PPL query::

    os> source=accounts | top showcount=false gender;
    fetched rows / total rows = 2/2
    +--------+
    | gender |
    |--------|
    | M      |
    | F      |
    +--------+

Example 2: Find the most common values in a field
=================================================

The example finds most common gender of all the accounts.

PPL query::

    os> source=accounts | top 1 showcount=false gender;
    fetched rows / total rows = 1/1
    +--------+
    | gender |
    |--------|
    | M      |
    +--------+

Example 2: Find the most common values organized by gender
==========================================================

The example finds most common age of all the accounts group by gender.

PPL query::

    os> source=accounts | top 1 showcount=false age by gender;
    fetched rows / total rows = 2/2
    +--------+-----+
    | gender | age |
    |--------+-----|
    | F      | 28  |
    | M      | 32  |
    +--------+-----+

Example 3: Top command with Calcite enabled
===========================================

PPL query::

    os> source=accounts | top gender;
    fetched rows / total rows = 2/2
    +--------+-------+
    | gender | count |
    |--------+-------|
    | M      | 3     |
    | F      | 1     |
    +--------+-------+


Example 4: Specify the count field option
=========================================

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
