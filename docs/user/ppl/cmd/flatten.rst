=============
flatten
=============

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2

Description
===========

Use ``flatten`` command to flatten a struct or an object field into separate
fields in a document.

The flattened fields will be ordered **lexicographically** by their original
key names in the struct. I.e. if the struct has keys ``b``, ``c`` and ``Z``,
the flattened fields will be ordered as ``Z``, ``b``, ``c``.

Note that ``flatten`` should not be applied to arrays. Please use ``expand``
command to expand an array field into multiple rows instead. However, since
an array can be stored in a non-array field in OpenSearch, when expanding a
field storing a nested array, only the first element of the array will be
flattened.

Version
=======
3.1.0

Syntax
======

flatten <field> [as (<alias-list>)]

* field: The field to be flattened. Only object and nested fields are
  supported.
* alias-list: (Optional) The names to use instead of the original key names.
  Names are separated by commas. It is advised to put the alias-list in
  parentheses if there is more than one alias. E.g. both
  ``country, state, city`` and ``(country, state, city)`` are supported,
  but the latter is advised. Its length must match the number of keys in the
  struct field.  Please note that the provided alias names **must** follow
  the lexicographical order of the corresponding original keys in the struct.

Example: flatten an object field with aliases
=============================================

Given the following index ``my-index``

.. code-block::

    {"message":{"info":"a","author":"e","dayOfWeek":1},"myNum":1}
    {"message":{"info":"b","author":"f","dayOfWeek":2},"myNum":2}

with the following mapping:

.. code-block:: json

    {
      "mappings": {
        "properties": {
          "message": {
            "type": "object",
            "properties": {
              "info": {
                "type": "keyword",
                "index": "true"
              },
              "author": {
                "type": "keyword",
                "fields": {
                  "keyword": {
                    "type": "keyword",
                    "ignore_above": 256
                  }
                },
                "index": "true"
              },
              "dayOfWeek": {
                "type": "long"
              }
            }
          },
          "myNum": {
            "type": "long"
          }
        }
      }
    }


The following query flattens the ``message`` field and renames the keys to
``creator, dow, info``:

PPL query::

    PPL> source=my-index | flatten message as (creator, dow, info);
    fetched rows / total rows = 2/2
    +-----------------------------------------+--------+---------+-----+------+
    | message                                 | myNum  | creator | dow | info |
    |-----------------------------------------|--------|---------|-----|------|
    | {"info":"a","author":"e","dayOfWeek":1} | 1      | e       | 1   | a    |
    | {"info":"b","author":"f","dayOfWeek":2} | 2      | f       | 2   | b    |
    +-----------------------------------------+--------+---------+-----+------+

Limitations
===========
* ``flatten`` after ``fields`` is not supported under the current
  implementation.
  E.g. ``source=my-index | fields message | flatten message`` will not work.
  This is because the current implementation relies on the flattened fields
  like ``message.info``, ``message.author``, etc. to be present in the read
  result.
* The command works only with Calcite enabled. This can be set with the
  following command:

  .. code-block::

    PUT /_cluster/settings
    {
      "persistent":{
          "plugins.calcite.enabled": true
      }
    }
