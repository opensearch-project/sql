=============
Syntax
=============

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2

Command Order
=============
The PPL query starts with either the ``search`` command to reference a table to search from, or the ``describe`` command to reference a table to get its metadata. All the following command could be in any order. In the following example, ``search`` command refer the accounts index as the source, then using fields and where command to do the further processing.

.. code-block::

   search source=accounts
   | where age > 18
   | fields firstname, lastname


Required arguments
==================
Required arguments are shown in angle brackets < >.


Optional arguments
==================
Optional arguments are enclosed in square brackets [ ].

