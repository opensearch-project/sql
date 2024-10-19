========
Comments
========

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2


Comments are not evaluated texts. PPL supports both line comments and block comments.

Line Comments
-------------
Line comments begin with two slashes ( // ) and end with a new line.

Example::

    os> source=accounts | top gender // finds most common gender of all the accounts
    fetched rows / total rows = 2/2
    +----------+
    | gender   |
    |----------|
    | M        |
    | F        |
    +----------+

Block Comments
--------------
Block comments begin with a slash followed by an asterisk ( /\* ) and end with an asterisk followed by a slash ( \*/ ).

Example::

    os> source=accounts | dedup 2 gender /* dedup the document with gender field keep 2 duplication */ | fields account_number, gender
    fetched rows / total rows = 3/3
    +------------------+----------+
    | account_number   | gender   |
    |------------------+----------|
    | 1                | M        |
    | 6                | M        |
    | 13               | F        |
    +------------------+----------+

