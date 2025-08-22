===========================
STRFTIME Format Specifiers
===========================

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2

Overview
========

The ``strftime`` function uses POSIX-style format specifiers, which are different from the MySQL-style format specifiers used by ``DATE_FORMAT``. This document provides a comprehensive reference for all supported format specifiers in the ``strftime`` function.

.. note::

    The strftime function requires the Calcite engine to be enabled. It is not available in the legacy engine.

Format Specifier Categories
============================

Date and Time Variables
-----------------------

.. list-table::
   :widths: 15 85
   :header-rows: 1

   * - Specifier
     - Description
   * - %c
     - The date and time in the current locale's format (e.g., Thu Jul 18 09:30:00 2019 for US English)
   * - %+
     - The date and time with time zone in the current locale's format (e.g., Thu Jul 18 09:30:00 PDT 2019)

Time Variables
--------------

.. list-table::
   :widths: 15 85
   :header-rows: 1

   * - Specifier
     - Description
   * - %Ez
     - Timezone offset in minutes
   * - %f
     - Microseconds as a decimal number (000000..999999)
   * - %H
     - Hour (24-hour clock) as a decimal number (00..23)
   * - %I
     - Hour (12-hour clock) with values 01 to 12
   * - %k
     - Like %H, hour (24-hour clock), space-padded ( 0..23)
   * - %M
     - Minute as a decimal number (00..59)
   * - %N
     - The number of subsecond digits. Default is %9N (nanoseconds). You can specify %3N = milliseconds, %6N = microseconds, %9N = nanoseconds
   * - %p
     - AM or PM. Use with %I for 12-hour clock
   * - %Q
     - The subsecond component of a UTC timestamp. Default is milliseconds (%3Q). Valid values: %3Q = milliseconds (000-999), %6Q = microseconds (000000-999999), %9Q = nanoseconds (000000000-999999999)
   * - %S
     - Second as a decimal number (00..59)
   * - %s
     - The UNIX Epoch Time timestamp (seconds since 1970-01-01 00:00:00 UTC)
   * - %T
     - The time in 24-hour notation (%H:%M:%S)
   * - %X
     - The time in the format for the current locale (e.g., 09:30:00)
   * - %Z
     - The timezone abbreviation (e.g., EST, PDT)
   * - %z
     - The timezone offset from UTC: +hhmm or -hhmm
   * - %:z
     - Timezone offset with colon: +hh:mm or -hh:mm
   * - %::z
     - Timezone offset with colons: +hh:mm:ss
   * - %:::z
     - Timezone offset hour only: +hh or -hh
   * - %%
     - A literal "%" character

Date Variables
--------------

.. list-table::
   :widths: 15 85
   :header-rows: 1

   * - Specifier
     - Description
   * - %F
     - Equivalent to %Y-%m-%d (ISO 8601 date format)
   * - %x
     - The date in the format of the current locale (e.g., 07/13/2019 for US English)

Days and Weeks
--------------

.. list-table::
   :widths: 15 85
   :header-rows: 1

   * - Specifier
     - Description
   * - %A
     - Full weekday name (Sunday, ..., Saturday)
   * - %a
     - Abbreviated weekday name (Sun, ..., Sat)
   * - %d
     - Day of the month as a decimal number with leading zero (01..31)
   * - %e
     - Day of the month as a decimal number, space-padded ( 1..31)
   * - %j
     - Day of year as a decimal number with leading zeros (001..366)
   * - %V
     - ISO 8601 week number of the year (01..53). Week 1 is the first week with 4 or more days in the new year
   * - %U
     - Week of the year starting from 0 (00..53). Sunday is the first day of the week
   * - %w
     - Weekday as a decimal number (0 = Sunday, ..., 6 = Saturday)

Months
------

.. list-table::
   :widths: 15 85
   :header-rows: 1

   * - Specifier
     - Description
   * - %b
     - Abbreviated month name (Jan, Feb, etc.)
   * - %B
     - Full month name (January, February, etc.)
   * - %m
     - Month as a decimal number (01..12)

Year
----

.. list-table::
   :widths: 15 85
   :header-rows: 1

   * - Specifier
     - Description
   * - %C
     - The century as a 2-digit decimal number
   * - %g
     - ISO 8601 year without century as a 2-digit decimal number (00..99)
   * - %G
     - ISO 8601 year with century as a 4-digit decimal number
   * - %y
     - Year without century as a decimal number (00..99)
   * - %Y
     - Year with century as a decimal number (e.g., 2025)

Examples
========

Basic Date and Time Formatting
-------------------------------

Converting UNIX timestamp to various date formats::

    -- ISO 8601 format
    os> source=people | eval result = strftime(1521467703, '%F %T')
    2018-03-19 13:55:03
    
    -- Custom format with full names
    os> source=people | eval result = strftime(1521467703, '%A, %B %d, %Y')
    Monday, March 19, 2018
    
    -- 12-hour format with AM/PM
    os> source=people | eval result = strftime(1521467703, '%I:%M:%S %p')
    01:55:03 PM

Working with Subseconds
------------------------

Handling timestamps with millisecond/microsecond precision::

    -- Extract milliseconds from nanosecond timestamp
    os> source=people | eval result = strftime(1521467703049000000, '%Y-%m-%dT%H:%M:%S.%3Q')
    2018-03-19T12:35:03.049
    
    -- Show microseconds
    os> source=people | eval result = strftime(1521467703, '%S.%f')
    03.000000

Week and Day Information
-------------------------

Getting week numbers and day information::

    -- ISO week number
    os> source=people | eval result = strftime(1521467703, 'Week %V of %G')
    Week 12 of 2018
    
    -- Day of year
    os> source=people | eval result = strftime(1521467703, 'Day %j of %Y')
    Day 078 of 2018
    
    -- Weekday name and number
    os> source=people | eval result = strftime(1521467703, '%A (%w)')
    Monday (1)

UNIX Timestamp Conversion
--------------------------

Converting between formats and UNIX timestamps::

    -- Get UNIX timestamp
    os> source=people | eval result = strftime(1521467703, '%s')
    1521467703
    
    -- Combine with other formats
    os> source=people | eval result = strftime(1521467703, 'Timestamp: %s = %F %T')
    Timestamp: 1521467703 = 2018-03-19 12:35:03

Differences from DATE_FORMAT
=============================

Key Differences
---------------

The ``strftime`` function uses POSIX-style format specifiers, while ``DATE_FORMAT`` uses MySQL-style specifiers. Here are the main differences:

.. list-table::
   :widths: 20 20 60
   :header-rows: 1

   * - Purpose
     - STRFTIME
     - DATE_FORMAT
   * - Month numeric
     - %m (01-12)
     - %c (0-12) or %m
   * - Weekday name
     - %A (full), %a (abbr)
     - %W (full), %a (abbr)
   * - Hour 24h
     - %H (00-23)
     - %H (00-23)
   * - Hour 12h
     - %I (01-12)
     - %h or %I (01-12)
   * - Microseconds
     - %f (6 digits)
     - %f (6 digits)
   * - UNIX timestamp
     - %s (epoch seconds)
     - Not available
   * - ISO week
     - %V (01-53)
     - %v (01-53)
   * - Timezone
     - %z, %Z (multiple formats)
     - Not supported
   * - ISO date
     - %F (YYYY-MM-DD)
     - Not available
   * - Subsecond precision
     - %N, %Q (configurable)
     - %f only (fixed 6 digits)

When to Use Each Function
--------------------------

- Use ``strftime`` when:
  - You need POSIX-compatible format strings
  - You need timezone information in output
  - You need configurable subsecond precision
  - You need to output UNIX timestamps
  - Working with systems that use standard strftime formatting

- Use ``DATE_FORMAT`` when:
  - You need MySQL-compatible format strings
  - Working with existing queries using MySQL syntax
  - You don't need timezone information

Migration Guide
---------------

Common format string conversions:

.. list-table::
   :widths: 40 30 30
   :header-rows: 1

   * - Purpose
     - DATE_FORMAT
     - STRFTIME
   * - ISO 8601 datetime
     - '%Y-%m-%d %H:%i:%s'
     - '%F %T' or '%Y-%m-%d %H:%M:%S'
   * - US date format
     - '%m/%d/%Y'
     - '%m/%d/%Y'
   * - Full date with weekday
     - '%W, %M %d, %Y'
     - '%A, %B %d, %Y'
   * - 12-hour time
     - '%h:%i:%s %p'
     - '%I:%M:%S %p'
   * - Year and week
     - '%X-%v'
     - '%G-%V'

See Also
========

- :doc:`datetime` - Complete datetime functions reference
- `FROM_UNIXTIME`_ - Convert UNIX timestamp using DATE_FORMAT style
- `DATE_FORMAT`_ - Format dates using MySQL-style specifiers
- `UNIX_TIMESTAMP`_ - Convert to UNIX timestamp

.. _FROM_UNIXTIME: datetime.html#from-unixtime
.. _DATE_FORMAT: datetime.html#date-format
.. _UNIX_TIMESTAMP: datetime.html#unix-timestamp