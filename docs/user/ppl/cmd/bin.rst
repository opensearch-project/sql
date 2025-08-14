=============
bin
=============

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2


Description
============
| The ``bin`` command groups numeric values into buckets of equal intervals, making it useful for creating histograms and analyzing data distribution. It takes a numeric field and generates a new field with values that represent the lower bound of each bucket.

Syntax
============
bin <field> [span=<interval>] [minspan=<interval>] [bins=<count>] [aligntime=(earliest | latest | <time-specifier>)] [start=<value>] [end=<value>] [AS <alias>]

* field: mandatory. The numeric field to bin.
* span: optional. The interval size for each bin. Cannot be used with bins or minspan parameters.
* minspan: optional. The minimum interval size for automatic span calculation. Cannot be used with span or bins parameters.
* bins: optional. The number of equal-width bins to create. Cannot be used with span or minspan parameters.
* aligntime: optional. Align the bin times for time-based fields. Valid only for time-based discretization. Options:
  - earliest: Align bins to the earliest timestamp in the data
  - latest: Align bins to the latest timestamp in the data  
  - <time-specifier>: Align bins to a specific epoch time value or time modifier expression
* start: optional. The starting value for binning range. If not specified, uses the minimum field value.
* end: optional. The ending value for binning range. If not specified, uses the maximum field value.
* alias: optional. Custom name for the binned field. **Default:** The original field will be replaced by the binned field

Parameters
============

span Parameter
--------------
Specifies the width of each bin interval with support for multiple span types:

**1. Numeric Span (existing behavior)**
- ``span=1000`` - Creates bins of width 1000 for numeric fields
- Calculation: ``floor(field / span) * span``
- Dynamic binning: No artificial limits on number of bins, no "Other" category

**2. Log-based Span (logarithmic binning)**
- **Syntax**: ``[<coefficient>]log[<base>]`` or ``logN`` where N is the base
- **Examples**:
  - ``span=log10`` - Base 10 logarithmic bins (coefficient=1)
  - ``span=2log10`` - Base 10 with coefficient 2
  - ``span=log2`` - Base 2 logarithmic bins
  - ``span=log3`` - Base 3 logarithmic bins (arbitrary base)
  - ``span=1.5log3`` - Base 3 with coefficient 1.5
- **Algorithm**:
  - For each value: ``bin_number = floor(log_base(value/coefficient))``
  - Bin boundaries: ``[coefficient * base^n, coefficient * base^(n+1))``
  - Only creates bins where data exists (data-driven approach)
- **Rules**:
  - Coefficient: Real number ≥ 1.0 and < base (optional, defaults to 1)
  - Base: Real number > 1.0 (required)
  - Creates logarithmic bin boundaries instead of linear

**3. Time Scale Span (comprehensive time units)**
- **Subseconds**: ``us`` (microseconds), ``ms`` (milliseconds), ``cs`` (centiseconds), ``ds`` (deciseconds)
- **Seconds**: ``s``, ``sec``, ``secs``, ``second``, ``seconds``
- **Minutes**: ``m``, ``min``, ``mins``, ``minute``, ``minutes``
- **Hours**: ``h``, ``hr``, ``hrs``, ``hour``, ``hours``
- **Days**: ``d``, ``day``, ``days`` - **Uses precise daily binning algorithm**
- **Months**: ``M``, ``mon``, ``month``, ``months`` - **Uses precise monthly binning algorithm**
- **Examples**:
  - ``span=30seconds``
  - ``span=15minutes``
  - ``span=2hours``
  - ``span=7days``
  - ``span=4months``
  - ``span=500ms``
  - ``span=100us``
  - ``span=50cs`` (centiseconds)
  - ``span=2ds`` (deciseconds)

**Daily Binning Algorithm (for day-based spans)**

For daily spans (``1days``, ``7days``, ``30days``), the implementation uses a **precise daily binning algorithm** with Unix epoch reference:

1. **Unix Epoch Reference**: Uses January 1, 1970 as the fixed reference point for all daily calculations
2. **Modular Arithmetic**: Calculates ``days_since_epoch % span_days`` to find position within span cycle
3. **Consistent Alignment**: Ensures identical input dates always produce identical bin start dates
4. **Date String Output**: Returns formatted date strings (``YYYY-MM-DD``) instead of timestamps

**Algorithm Example**: For July 28, 2025 (day 20,297 since Unix epoch):
- ``span=6days``: 20,297 % 6 = 5 → bin starts July 23, 2025 (``"2025-07-23"``)
- ``span=7days``: 20,297 % 7 = 4 → bin starts July 24, 2025 (``"2025-07-24"``)

**Monthly Binning Algorithm (for month-based spans)**

For monthly spans (``1months``, ``4months``, ``6months``), the implementation uses a **precise monthly binning algorithm** with Unix epoch reference:

1. **Unix Epoch Reference**: Uses January 1970 as the fixed reference point for all monthly calculations
2. **Modular Arithmetic**: Calculates ``months_since_epoch % span_months`` to find position within span cycle
3. **Consistent Alignment**: Ensures identical input dates always produce identical bin start months
4. **Month String Output**: Returns formatted month strings (``YYYY-MM``) instead of timestamps

**Algorithm Example**: For July 2025 (666 months since Unix epoch):
- ``span=4months``: 666 % 4 = 2 → bin starts at month 664 = May 2025 (``"2025-05"``)
- ``span=6months``: 666 % 6 = 0 → bin starts at month 666 = July 2025 (``"2025-07"``)

This ensures precise and consistent behavior for both daily and monthly binning operations.

minspan Parameter
-----------------
Specifies the minimum allowed interval size using a magnitude-based algorithm. The algorithm works as follows:

1. **Calculate default width**: ``10^FLOOR(LOG10(data_range))`` - the largest power of 10 that fits within the data range
2. **Apply minspan constraint**: 
   - If ``default_width >= minspan``: use the default width
   - If ``default_width < minspan``: use ``10^CEIL(LOG10(minspan))``

This ensures bins use human-readable widths (powers of 10) while respecting the minimum span requirement.

**Example**: For age data with range 20-40 (range=20) and minspan=11:
- Default width = 10^FLOOR(LOG10(20)) = 10^1 = 10
- Since minspan=11 > 10, use 10^CEIL(LOG10(11)) = 10^2 = 100
- Result: Single bin "0-100" covering all age values

aligntime Parameter
-------------------
For time-based fields, aligntime allows you to specify how bins should be aligned. This parameter is essential for creating consistent time-based bins that align to meaningful boundaries like start of day, hour, etc.

**IMPORTANT: Alignment Rule**

**Aligntime is ignored when span is in days, months, or years.** Longer-term spans (``1d``, ``2M``, ``1y``) automatically align to natural boundaries (midnight, month start, year start) regardless of aligntime settings.

**Alignment Options:**

* ``earliest``: Aligns bins to the earliest timestamp in the dataset
* ``latest``: Aligns bins to the latest timestamp in the dataset
* ``<epoch-timestamp>``: Aligns bins to a specific epoch timestamp (e.g., 1640995200)
* ``<time-modifier>``: Aligns bins using time modifier expressions (standard-compatible)

**Time Modifier Expressions:**

Time modifiers provide a flexible way to align bins to specific time boundaries:

* ``@d``: Align to start of day (00:00:00)
* ``@d+<offset>``: Align to start of day plus offset (e.g., ``@d+3h`` = 03:00:00)
* ``@d-<offset>``: Align to start of day minus offset (e.g., ``@d-1h`` = 23:00:00 previous day)

**Supported Time Spans:**

**Aligntime applies to:**
* ``us``, ``ms``, ``cs``, ``ds``: Subsecond units (microseconds, milliseconds, centiseconds, deciseconds)
* ``s``, ``sec``, ``secs``, ``seconds``: Seconds
* ``m``, ``min``, ``mins``, ``minutes``: Minutes 
* ``h``, ``hr``, ``hrs``, ``hours``: Hours

**Aligntime ignored for:**
* ``d``, ``days``: Days - automatically aligns to midnight using daily binning algorithm
* ``M``, ``months``: Months - automatically aligns to month start using monthly binning algorithm

**How Aligntime Works:**

The aligntime parameter modifies the binning calculation:
* **Without aligntime**: ``floor(timestamp / span) * span``
* **With aligntime**: ``floor((timestamp - aligntime) / span) * span + aligntime``
* **With day/month spans**: Aligntime is ignored, natural boundaries used via specialized algorithms

This ensures that bins are aligned to meaningful time boundaries rather than arbitrary epoch-based intervals.

bins Parameter
--------------
Automatically calculates the span using a "nice number" algorithm to create human-readable bin widths. 

**Validation**: The bins parameter must be between 2 and 50000 (inclusive). Values outside this range will result in an error.

The algorithm works as follows:

1. **Validate bins**: Ensure ``2 ≤ bins ≤ 50000``
2. **Calculate data range**: ``max_value - min_value``
3. **Test nice widths**: Iterate through powers of 10 from smallest to largest: [0.001, 0.01, 0.1, 1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000]
4. **Select optimal width**: Choose the **first** width where ``CEIL(data_range / width) ≤ requested_bins``
5. **Account for boundaries**: If the maximum value falls exactly on a bin boundary, add one extra bin

This prioritizes creating the **maximum number of bins** within the requested limit while using human-readable widths.

**Example**: For age data with range 20-50 (range=30) and bins=3:
- Test width=1: CEIL(30/1) = 30 bins > 3 ❌
- Test width=10: CEIL(30/10) = 3 bins ≤ 3 ✅
- Result: Use width=10, creating bins "20-30", "30-40", "40-50"

**Error Examples**:
- ``bins=1`` → Error: "The bins parameter must be at least 2, got: 1"
- ``bins=50001`` → Error: "The bins parameter must not exceed 50000, got: 50001"

start and end Parameters
-------------------------
Define the range for binning using an effective range expansion algorithm. The key insight is that start/end parameters affect the **width calculation**, not just the binning boundaries.

**Algorithm:**
1. **Calculate effective range**: Only expand, never shrink the data range
   - ``effective_min = MIN(start, data_min)`` if start specified
   - ``effective_max = MAX(end, data_max)`` if end specified
   - ``effective_range = effective_max - effective_min``

2. **Apply magnitude-based width calculation** with boundary handling:
   - If ``effective_range`` is exactly a power of 10: ``width = 10^(FLOOR(LOG10(effective_range)) - 1)``
   - Otherwise: ``width = 10^FLOOR(LOG10(effective_range))``

3. **Create bins** using the calculated width

**Examples**: 

- **end=100000**: effective_range = 100,000 (exact power of 10)
  - Width = 10^(5-1) = 10^4 = 10,000  
  - Result: 5 bins "0-10000", "10000-20000", ..., "40000-50000"

- **end=100001**: effective_range = 100,001 (not exact power of 10)
  - Width = 10^FLOOR(LOG10(100,001)) = 10^5 = 100,000
  - Result: Single bin "0-100000" with count 1000

This boundary handling ensures proper bin granularity for common range specifications.

alias Parameter
---------------
Provides a custom name for the new binned field. If not specified, the field name will be appended with "_bin".

Example 1: Basic binning with span
===================================

The example shows binning account balances into $5000 intervals.

PPL query::

    os> source=accounts | bin balance span=5000 | fields balance | head 5;
    fetched rows / total rows = 5/5
    +---------------+
    | balance       |
    |---------------|
    | 35000-40000   |
    | 5000-10000    |
    | 30000-35000   |
    | 0-5000        |
    | 15000-20000   |
    +---------------+

Example 2: Binning with custom alias
=====================================

The example shows binning with a custom field name.

PPL query::

    os> source=accounts | bin balance span=10000 AS balance_range | fields balance_range | head 3;
    fetched rows / total rows = 3/3
    +---------------+
    | balance_range |
    |---------------|
    | 30000-40000   |
    | 0-10000       |
    | 30000-40000   |
    +---------------+

Example 3: Binning with bins parameter
=======================================

The example shows creating bins using nice number algorithm for age field.

PPL query::

    os> source=accounts | bin age span=10 | stats count() by age | sort age;
    fetched rows / total rows = 3/3
    +---------+-------+
    | count() | age   |
    |---------|-------|
    | 451     | 20-30 |
    | 504     | 30-40 |
    | 45      | 40-50 |
    +---------+-------+

**Explanation**: With span=10 and age data ranging from 20-40, this creates bins of width 10, resulting in 3 bins with actual data: 20-30 (451 records), 30-40 (504 records), and 40-50 (45 records).

Example 4: Binning with stats aggregation
==========================================

The example shows using bin command with stats to create a histogram.

PPL query::

    os> source=accounts | bin balance span=10000 AS balance_bucket | stats count() by balance_bucket | sort balance_bucket;
    fetched rows / total rows = 5/5
    +---------+----------------+
    | count() | balance_bucket |
    |---------+----------------|
    | 168     | 0-10000        |
    | 213     | 10000-20000    |
    | 217     | 20000-30000    |
    | 187     | 30000-40000    |
    | 215     | 40000-50000    |
    +---------+----------------+

Example 5: Binning with decimal span
=====================================

The example shows binning with decimal interval values.

PPL query::

    os> source=accounts | bin balance span=7500.5 AS balance_group | fields balance_group | head 3;
    fetched rows / total rows = 3/3
    +---------------+
    | balance_group |
    |---------------|
    | 37500-45000   |
    | 0-7500        |
    | 30000-37500   |
    +---------------+

Example 6: Binning with minspan parameter
==========================================

The example shows binning with magnitude-based minspan algorithm.

PPL query::

    os> source=accounts | bin age minspan=11 | stats count() by age | sort age;
    fetched rows / total rows = 1/1
    +---------+-------+
    | count() | age   |
    |---------|-------|
    | 1000    | 0-100 |
    +---------+-------+

**Explanation**: For age range ~20 with minspan=11:
- Default width = 10^FLOOR(LOG10(20)) = 10
- Since minspan=11 > 10, use 10^CEIL(LOG10(11)) = 100
- Result: Single bin "0-100" covering all ages

Example 7: Default binning behavior (magnitude-based algorithm)
==============================================================

The example shows bin command without parameters using magnitude-based default width algorithm.

PPL query::

    os> source=accounts | bin age | stats count() by age | sort age;
    fetched rows / total rows = 3/3
    +---------+-------+
    | count() | age   |
    |---------|-------|
    | 451     | 20-30 |
    | 504     | 30-40 |
    | 45      | 40-50 |
    +---------+-------+

**Explanation**: For age field with range ~20 (20-40), the algorithm calculates:
- Default width = 10^FLOOR(LOG10(20)) = 10^1 = 10
- Creates bins with width=10: "20-30", "30-40", "40-50"

This demonstrates magnitude-based algorithm that automatically selects appropriate bin widths based on the data range.

Time Field Support
==================

The bin command supports time-based binning on **any field with a time-based data type**, not just the traditional ``@timestamp`` field. Supported time field types include:

* **timestamp** - Full datetime with timezone information
* **datetime** - Date and time without timezone 
* **date** - Date only (year, month, day)
* **time** - Time only (hour, minute, second)

**Examples of valid time field binning:**

.. code-block:: ppl

   # Using custom timestamp field
   source=events | bin event_time span=1h | stats count() by event_time
   
   # Using date field
   source=transactions | bin transaction_date span=1d | stats sum(amount) by transaction_date
   
   # Using datetime field  
   source=logs | bin created_at span=30m aligntime="@d" | stats count() by created_at
   
   # Using time field for daily patterns
   source=activity | bin activity_time span=2h | stats avg(duration) by activity_time

**Key Benefits:**

* **Flexibility**: Work with your existing field names and schemas
* **Multi-field Support**: Bin different time fields in the same dataset
* **Type Safety**: Automatic detection of time-based fields for appropriate binning
* **Consistent Behavior**: Same aligntime and span functionality across all time field types

Example 8: Time-based binning with real data
===========================================

The example shows time-based binning using real time_test dataset.

PPL query::

    os> source=time_test | bin @timestamp span=2hour | fields @timestamp, value | head 5;
    fetched rows / total rows = 5/5
    +---------------------+-------+
    | @timestamp          | value |
    |---------------------|-------|
    | 2025-07-28 00:00:00 | 8945  |
    | 2025-07-28 00:00:00 | 7623  |
    | 2025-07-28 02:00:00 | 9187  |
    | 2025-07-28 02:00:00 | 6834  |
    | 2025-07-28 04:00:00 | 8291  |
    +---------------------+-------+

**Explanation**: This shows 2-hour time binning, grouping timestamps into 2-hour intervals starting from midnight.

Example 10: Binning with @d time modifier (start of day)
========================================================

The example shows aligning bins to start of day using the @d time modifier.

PPL query::

    os> source=time_test | bin @timestamp span=2h aligntime="@d" | fields @timestamp, value, category, timestamp | head 4;
    fetched rows / total rows = 4/4
    +---------------------+-------+----------+---------------------+
    | @timestamp          | value | category | timestamp           |
    |---------------------|-------|----------|---------------------|
    | 2025-07-28 00:00:00 | 8945  | A        | 2025-07-28 00:15:23 |
    | 2025-07-28 00:00:00 | 7623  | B        | 2025-07-28 01:42:15 |
    | 2025-07-28 02:00:00 | 9187  | C        | 2025-07-28 02:28:45 |
    | 2025-07-28 02:00:00 | 6834  | A        | 2025-07-28 03:56:20 |
    +---------------------+-------+----------+---------------------+

Example 11: Binning with @d+3h time modifier (3 AM alignment)
=============================================================

The example shows aligning bins to 3 AM (start of day + 3 hours) with 12-hour intervals.

PPL query::

    os> source=time_test | bin @timestamp span=12h aligntime="@d+3h" | fields @timestamp, value, category, timestamp | head 4;
    fetched rows / total rows = 4/4
    +---------------------+-------+----------+---------------------+
    | @timestamp          | value | category | timestamp           |
    |---------------------|-------|----------|---------------------|
    | 2025-07-27 15:00:00 | 8945  | A        | 2025-07-28 00:15:23 |
    | 2025-07-27 15:00:00 | 7623  | B        | 2025-07-28 01:42:15 |
    | 2025-07-27 15:00:00 | 9187  | C        | 2025-07-28 02:28:45 |
    | 2025-07-28 03:00:00 | 6834  | A        | 2025-07-28 03:56:20 |
    +---------------------+-------+----------+---------------------+

**Explanation**: With ``aligntime="@d+3h"`` and ``span=12h``, bins are created as:
- 15:00 (previous day) to 03:00 (current day) → displays as "2025-07-27 15:00"
- 03:00 (current day) to 15:00 (current day) → displays as "2025-07-28 03:00"

Example 12: Binning with @d-1h time modifier (11 PM alignment)
==============================================================

The example shows aligning bins to 11 PM (start of day - 1 hour) with 4-hour intervals.

PPL query::

    os> source=time_test | bin @timestamp span=4h aligntime="@d-1h" | fields @timestamp, value, category, timestamp | head 3;
    fetched rows / total rows = 3/3
    +---------------------+-------+----------+---------------------+
    | @timestamp          | value | category | timestamp           |
    |---------------------|-------|----------|---------------------|
    | 2025-07-27 23:00:00 | 8945  | A        | 2025-07-28 00:15:23 |
    | 2025-07-27 23:00:00 | 7623  | B        | 2025-07-28 01:42:15 |
    | 2025-07-27 23:00:00 | 9187  | C        | 2025-07-28 02:28:45 |
    +---------------------+-------+----------+---------------------+

**Explanation**: With ``aligntime="@d-1h"`` and ``span=4h``, bins are created as:
- 23:00 (previous day) to 03:00 (current day) → displays as "2025-07-27 23:00"
- 03:00 to 07:00 → displays as "2025-07-28 03:00"

Example 13: Binning with range specification (start/end parameters)
====================================================================

The example shows binning with start and end parameters using effective range expansion.

PPL query::

    os> source=accounts | bin age span=5 start=25 end=35 | fields age | head 10;
    fetched rows / total rows = 10/10
    +-------+
    | age   |
    |-------|
    | 30-35 |
    | 35-40 |
    | 25-30 |
    | 30-35 |
    | 35-40 |
    | 35-40 |
    | 30-35 |
    | 35-40 |
    | 35-40 |
    | 20-25 |
    +-------+

**Explanation**: Using span=5 with start=25 and end=35 creates 5-unit bins. Even though the range is specified as 25-35, data outside this range (like age 20) still gets binned appropriately into the 20-25 range.

Example 14: Using bin for data analysis
=====================================

The example shows how binning can be used to analyze data distribution patterns.

PPL query::

    os> source=time_test | bin value span=500 | stats count() by value | sort value;
    fetched rows / total rows = 20/20
    +---------+-------------+
    | count() | value       |
    |---------|-------------|
    | 5       | 6000-6500   |
    | 6       | 6500-7000   |
    | 8       | 7000-7500   |
    | 12      | 7500-8000   |
    | 15      | 8000-8500   |
    | 18      | 8500-9000   |
    | 21      | 9000-9500   |
    | 15      | 9500-10000  |
    +---------+-------------+

**Explanation**: This creates a histogram of the numeric `value` field, showing the distribution of values in 500-unit bins. This is useful for understanding data patterns and identifying outliers.

Example 17: Extended time scale units
=====================================

The example shows binning with extended time scale unit support.

PPL query::

    os> source=time_test | bin @timestamp span=30seconds | fields @timestamp, value | sort @timestamp | head 10;
    fetched rows / total rows = 10/10
    +---------------------+-------+
    | @timestamp          | value |
    |---------------------|-------|
    | 2025-07-28 00:15:00 | 8945  |
    | 2025-07-28 01:42:00 | 7623  |
    | 2025-07-28 02:28:30 | 9187  |
    | 2025-07-28 03:56:00 | 6834  |
    | 2025-07-28 04:33:00 | 8291  |
    | 2025-07-28 05:17:30 | 7456  |
    | 2025-07-28 06:04:30 | 9012  |
    | 2025-07-28 07:51:00 | 6589  |
    | 2025-07-28 08:38:00 | 8736  |
    | 2025-07-28 09:15:00 | 7198  |
    +---------------------+-------+

**Explanation**: The ``30seconds`` span creates 30-second interval bins, demonstrating support for extended time scale units beyond the basic ``s``, ``m``, ``h`` format. Each record's timestamp is binned to the nearest 30-second boundary.

Example 17.1: Binning numeric values
====================================

The example shows binning numeric values into ranges.

PPL query::

    os> source=time_test | bin value span=1000 | fields value | head 5;
    fetched rows / total rows = 5/5
    +-------------+
    | value       |
    |-------------|
    | 8000-9000   |
    | 7000-8000   |
    | 9000-10000  |
    | 6000-7000   |
    | 8000-9000   |
    +-------------+

**Explanation**: The numeric field ``value`` is binned into 1000-unit intervals, creating ranges like 6000-7000, 7000-8000, etc.

Example 18: Time-based data aggregation
=====================================

The example shows combining bin with stats for time-series analysis.

PPL query::

    os> source=time_test | bin @timestamp span=5minutes | stats count() by @timestamp | sort @timestamp | head 5;
    fetched rows / total rows = 5/5
    +---------+---------------------+
    | count() | @timestamp          |
    |---------+---------------------|
    | 1       | 2025-07-28 00:15:00 |
    | 1       | 2025-07-28 01:40:00 |
    | 1       | 2025-07-28 02:25:00 |
    | 1       | 2025-07-28 03:55:00 |
    | 1       | 2025-07-28 04:30:00 |
    +---------+---------------------+

**Explanation**: This shows how to create time-series aggregations by binning timestamps into 5-minute intervals and counting events in each bin.

Example 20: Daily Binning Algorithm
===================================

The example shows precise daily binning algorithm with Unix epoch reference.

PPL query::

    os> source=time_test | bin @timestamp span=6days | fields @timestamp, value | sort @timestamp | head 5;
    fetched rows / total rows = 5/5
    +-------------+-------+
    | @timestamp  | value |
    |-------------|-------|
    | 2025-07-23  | 8945  |
    | 2025-07-23  | 7623  |
    | 2025-07-23  | 9187  |
    | 2025-07-23  | 6834  |
    | 2025-07-23  | 8291  |
    +-------------+-------+

PPL query::

    os> source=time_test | bin @timestamp span=7days | fields @timestamp, value | sort @timestamp | head 5;
    fetched rows / total rows = 5/5
    +-------------+-------+
    | @timestamp  | value |
    |-------------|-------|
    | 2025-07-24  | 8945  |
    | 2025-07-24  | 7623  |
    | 2025-07-24  | 9187  |
    | 2025-07-24  | 6834  |
    | 2025-07-24  | 8291  |
    +-------------+-------+

**Explanation**: For original timestamps on July 28, 2025:
- **6-day binning**: Day 20,297 % 6 = 5 → bin starts at day 20,292 = **2025-07-23**
- **7-day binning**: Day 20,297 % 7 = 4 → bin starts at day 20,293 = **2025-07-24**

This demonstrates precise daily binning algorithm using Unix epoch (1970-01-01) as reference point for consistent bin alignment across all dates.

Example 19: Daily vs Hourly Binning Comparison
===============================================

The example shows the difference between daily binning and hourly binning.

PPL query (daily binning - returns timestamps aligned to midnight)::

    os> source=time_test | bin @timestamp span=1days | fields @timestamp | head 3;
    fetched rows / total rows = 3/3
    +---------------------+
    | @timestamp          |
    |---------------------|
    | 2025-07-28 00:00:00 |
    | 2025-07-28 00:00:00 |
    | 2025-07-28 00:00:00 |
    +---------------------+

PPL query (hourly binning - returns timestamps)::

    os> source=time_test | bin @timestamp span=24hours | fields @timestamp, value | head 3;
    +---------------------+-------+
    | @timestamp          | value |
    |---------------------|-------|
    | 2025-07-28 00:00:00 | 8945  |
    | 2025-07-28 00:00:00 | 7623  |
    | 2025-07-28 00:00:00 | 9187  |
    +---------------------+-------+

**Explanation**: 
- **Daily spans** (``1days``, ``7days``) align timestamps to midnight (00:00:00) of each day
- **Hour spans** (``1hour``, ``24hours``) create regular hourly intervals

Standard Compatibility
======================

The bin command implements industry-standard syntax and behavior patterns:

**Supported Features:**
* Time modifier expressions (``@d``, ``@d+3h``, ``@d-1h``)
* Aligntime parameter for timestamp alignment
* In-place field transformation (original field is replaced with binned values)
* SPAN function for time-based binning
* Consistent binning behavior across multiple rows
* **Precise Daily Binning Algorithm**:
  - Unix epoch (1970-01-01) reference point for all daily calculations
  - Modular arithmetic for consistent bin alignment: ``days_since_epoch % span_days``
  - Date string output format (``YYYY-MM-DD``) for daily spans
  - Ensures consistent daily binning behavior
* **Precise Monthly Binning Algorithm**:
  - Unix epoch (January 1970) reference point for all monthly calculations
  - Modular arithmetic for consistent bin alignment: ``months_since_epoch % span_months``
  - Month string output format (``YYYY-MM``) for monthly spans
  - Ensures consistent monthly binning behavior
* **Extended span options**:
  - Logarithmic binning (``span=log10``, ``span=2log10``, ``span=log3``, arbitrary bases)
  - **Comprehensive time scale units**: ``seconds``, ``minutes``, ``hours``, ``days``, ``months``
  - **Full timescale specification support**: ``us``, ``ms``, ``cs``, ``ds``, ``sec``, ``secs``, ``seconds``, ``min``, ``mins``, ``minutes``, ``hr``, ``hrs``, ``hours``, ``day``, ``days``, ``mon``, ``month``, ``months``
  - Subsecond precision (``us``, ``ms``, ``cs``, ``ds``)
  - Case-sensitive month/minute distinction (``M`` = months, ``m`` = minutes)
* **Standard algorithm compatibility**: ``aligntime``, ``bins``, ``minspan``, ``start``, ``end``
* **Nice Number Algorithm**: Optimal width selection logic for ``bins`` parameter
* **Magnitude-Based Algorithms**: For ``minspan``, ``start/end``, and default binning

**Key Differences from Standard SQL:**
* PPL bin command transforms the original field in-place (industry-standard behavior)
* Time modifier expressions provide flexible time alignment
* Aligntime works with any time-based fields (timestamp, date, time, datetime types)
* Binned timestamp values show the bin start time (e.g., "2025-07-28 03:00")

Best Practices
==============

Choosing Bin Parameters
------------------------
* Use ``span`` when you know the exact desired interval size (e.g., $1000 for financial data, 2h for time data)
* Use ``minspan`` when you want to ensure bins are at least a certain size but allow automatic optimization
* Use ``bins`` when you want a specific number of buckets for visualization
* Use ``aligntime`` for time-based data when you need bins aligned to specific boundaries
* Consider your data range when choosing span values to avoid too many or too few bins

Time-Based Binning Best Practices
----------------------------------
* For any time-based fields (timestamp, date, time, datetime), always consider using ``aligntime`` to ensure meaningful bin boundaries
* Use ``@d`` aligntime for daily patterns starting at midnight
* Use ``@d+3h`` for business hours analysis (e.g., 3 AM to 3 PM, 3 PM to 3 AM)
* Combine appropriate span values with aligntime (e.g., ``span=12h aligntime="@d+3h"``)
* Time modifier expressions are more readable than epoch timestamps

Logarithmic Binning Best Practices
-----------------------------------
* Use logarithmic binning (``span=log10``, ``span=log2``) for data spanning multiple orders of magnitude
* Consider coefficient adjustments (``span=2log10``) when your data doesn't start at 1
* Choose base according to your domain: ``log2`` for computer science, ``log10`` for scientific data
* Logarithmic binning is ideal for financial data, population data, or any exponentially distributed values

Performance Considerations
--------------------------
* Binning is performed during query execution and may impact performance on large datasets
* Consider using appropriate span sizes to balance detail and performance
* Use with ``stats`` command for efficient histogram generation

Common Use Cases
================
* **Histograms**: Combine with ``stats count()`` to create frequency distributions
* **Time-series Analysis**: Bin any time-based fields with aligntime for consistent time boundaries
* **Business Hours Analysis**: Use ``aligntime="@d+9h"`` with appropriate spans for business day patterns
* **Daily/Weekly Patterns**: Align bins to meaningful time boundaries (midnight, noon, etc.)
* **Data Categorization**: Group continuous values into discrete categories
* **Outlier Detection**: Identify unusual value distributions
* **Log Analysis**: Group log events by time intervals aligned to operational schedules
* **Financial Analysis**: Use logarithmic binning for price data, trading volumes
* **Scientific Data**: Group measurements into appropriate magnitude ranges

**Time-Series Examples:**
* **Hourly Analysis**: ``span=1h aligntime="@d"`` for hourly bins starting at midnight
* **Business Shifts**: ``span=8h aligntime="@d+6h"`` for 8-hour shifts starting at 6 AM
* **Weekly Reports**: ``span=7d aligntime="@d"`` for weekly bins starting on Sunday midnight
* **High-Frequency Trading**: ``span=100ms`` for sub-second analysis

**Logarithmic Examples:**
* **Financial Portfolio**: ``span=log10`` for asset values spanning $1 to $1M+
* **Web Traffic**: ``span=2log10`` for request counts from 2/sec to 200K/sec
* **Scientific Measurements**: ``span=log2`` for binary-scaling phenomena

Relationship to span() Function
================================
The ``bin`` command is similar to using the ``span()`` function in stats aggregations, but with key differences:

* ``bin`` creates a new field that can be used in subsequent commands
* ``span()`` is used within stats aggregations for grouping
* ``bin`` supports the ``bins`` parameter for automatic span calculation
* ``bin`` allows more flexible field naming with aliases

Comparison::

    # Using bin command
    source=accounts | bin balance span=5000 | stats count() by balance_bin
    
    # Using span() function  
    source=accounts | stats count() by span(balance, 5000)

Both approaches create similar results, but ``bin`` provides more flexibility for complex queries where the binned field needs to be used in multiple places.

Algorithm Details
==================

Standard Binning Algorithms
---------------------------

The bin command implements seven distinct algorithms depending on the parameters used:

**1. Bins Parameter Algorithm (Nice Number Selection)**

The bins parameter uses a "nice number" algorithm to create human-readable bin widths:

.. code-block:: none

   Nice widths array (tested in order):
   [0.001, 0.01, 0.1, 1, 10, 100, 1000, 10000, 100000, 
    1000000, 10000000, 100000000, 1000000000]
   
   Algorithm:
   1. Calculate data_range = MAX(field) - MIN(field)
   2. For each width in nice_widths (smallest to largest):
      a. Calculate theoretical_bins = CEIL(data_range / width)
      b. If max_value % width == 0: actual_bins = theoretical_bins + 1
      c. Else: actual_bins = theoretical_bins
      d. If actual_bins <= requested_bins: SELECT this width and BREAK
   3. Create bins using selected width

**2. Minspan Parameter Algorithm (Magnitude-Based Selection)**

The minspan parameter uses a magnitude-based algorithm for default width calculation:

.. code-block:: none

   Algorithm:
   1. Calculate data_range = MAX(field) - MIN(field)
   2. Calculate default_width = 10^FLOOR(LOG10(data_range))
   3. If default_width >= minspan:
      - Use default_width
   4. Else:
      - Use width = 10^CEIL(LOG10(minspan))
   5. Create bins starting at FLOOR(MIN(field) / width) * width

**3. Span Parameter Algorithm (Fixed Width)**

The span parameter uses a simple fixed-width algorithm:

.. code-block:: none

   Algorithm:
   1. Use the specified span as bin width
   2. Create bins using: FLOOR(field / span) * span
   3. Generate range strings: "binStart-binEnd"
   4. No artificial limits - creates as many bins as needed
   5. No "Other" category for values outside arbitrary ranges

**4. Default Binning Algorithm (Magnitude-Based Width)**

When no parameters are specified, uses a magnitude-based default width algorithm:

.. code-block:: none

   Algorithm:
   1. Calculate data_range = MAX(field) - MIN(field)
   2. Calculate magnitude = FLOOR(LOG10(data_range))
   3. Calculate default_width = 10^magnitude
   4. Create bins using: FLOOR(field / default_width) * default_width
   5. Generate range strings: "binStart-binEnd"
   
   Examples:
   - Age range 20-40 (range=20) → width = 10^FLOOR(LOG10(20)) = 10^1 = 10
   - Balance range 1K-50K (range=49K) → width = 10^FLOOR(LOG10(49000)) = 10^4 = 10000

**5. Start/End Parameter Algorithm (Effective Range Expansion)**

The start/end parameters use effective range expansion with boundary handling:

.. code-block:: none

   Algorithm:
   1. Calculate effective_range:
      - effective_min = MIN(start, data_min) if start specified, else data_min
      - effective_max = MAX(end, data_max) if end specified, else data_max
      - effective_range = effective_max - effective_min
   
   2. Apply magnitude-based width calculation with boundary handling:
      - If LOG10(effective_range) == FLOOR(LOG10(effective_range)):
        * width = 10^(FLOOR(LOG10(effective_range)) - 1)  [exact power of 10]
      - Else:
        * width = 10^FLOOR(LOG10(effective_range))  [normal case]
   
   3. Create bins starting at FLOOR(effective_min / width) * width
   
   Examples:
   - end=100000: effective_range=100,000 → width=10,000 (5 bins)
   - end=100001: effective_range=100,001 → width=100,000 (1 bin)

**6. Daily Binning Algorithm (for day-based spans)**

For daily spans (``1days``, ``7days``, ``30days``), uses a precise daily binning algorithm:

.. code-block:: none

   Algorithm:
   1. Extract date from timestamp (ignore time component)
   2. Calculate days_since_epoch = DATEDIFF(input_date, '1970-01-01')
   3. Find position in cycle: position = days_since_epoch % span_days
   4. Calculate bin start: bin_start_days = days_since_epoch - position
   5. Convert to date: bin_start_date = ADDDATE('1970-01-01', bin_start_days)
   6. Format as YYYY-MM-DD string
   
   Examples (for July 28, 2025 = day 20,297):
   - span=6days: 20,297 % 6 = 5 → bin_start = 20,292 → 2025-07-23
   - span=7days: 20,297 % 7 = 4 → bin_start = 20,293 → 2025-07-24
   
   Key Features:
   - Uses Unix epoch (1970-01-01) as fixed reference point
   - Modular arithmetic ensures consistent bin alignment
   - Returns date strings instead of timestamps
   - Ensures consistent daily binning behavior

**7. Monthly Binning Algorithm (for month-based spans)**

For monthly spans (``1months``, ``4months``, ``6months``), uses a precise monthly binning algorithm:

.. code-block:: none

   Algorithm:
   1. Extract date from timestamp (ignore time component)
   2. Calculate months_since_epoch = (YEAR(input_date) - 1970) * 12 + (MONTH(input_date) - 1)
   3. Find position in cycle: position = months_since_epoch % span_months
   4. Calculate bin start: bin_start_months = months_since_epoch - position
   5. Convert to year/month: bin_year = 1970 + (bin_start_months / 12), bin_month = (bin_start_months % 12) + 1
   6. Format as YYYY-MM string with proper zero-padding
   
   Examples (for July 2025 = month 666):
   - span=4months: 666 % 4 = 2 → bin_start = 664 → May 2025 → 2025-05
   - span=6months: 666 % 6 = 0 → bin_start = 666 → July 2025 → 2025-07
   
   Key Features:
   - Uses Unix epoch (January 1970) as fixed reference point
   - Modular arithmetic ensures consistent bin alignment
   - Returns month strings instead of timestamps
   - Ensures consistent monthly binning behavior

**8. Logarithmic Binning Algorithm (for log-based spans)**

For log-based spans (``log10``, ``2log10``, ``log3``, etc.), uses data-driven logarithmic binning:

.. code-block:: none

   Algorithm:
   1. Parse span: coefficient=1.0, base=10.0 (defaults) from "log10"
   2. For span="2log3": coefficient=2.0, base=3.0  
   3. Validate: coefficient >= 1.0 and coefficient < base, base > 1.0
   4. For each data value:
      a. Calculate bin_number = FLOOR(LOG_base(value / coefficient))
      b. Calculate bin_start = coefficient * base^bin_number
      c. Calculate bin_end = coefficient * base^(bin_number + 1)
   5. Generate range strings: "bin_start-bin_end"
   6. Only create bins where data actually exists (data-driven)
   
   Examples:
   - span=log10, value=150: bin_number=FLOOR(LOG10(150/1))=2, bin="100-1000"
   - span=2log3, value=50: bin_number=FLOOR(LOG3(50/2))=3, bin="54-162"
   
   Key Features:
   - Exponentially increasing bin widths
   - Perfect for data spanning multiple orders of magnitude
   - Supports arbitrary bases and coefficients
   - Data-driven approach (no empty bins)

**Bin Boundary Calculation**

Most algorithms use the standard binning formula:

.. code-block:: none

   bin_value = FLOOR((field_value - first_bin_start) / width) * width + first_bin_start
   bin_range = "bin_value-(bin_value + width)"

**Exceptions**: 
- Daily spans use the specialized daily binning algorithm
- Monthly spans use the specialized monthly binning algorithm  
- Log spans use logarithmic boundary calculation

**Range String Format**

Range strings are formatted based on the width magnitude:

.. code-block:: none

   - Width >= 1: Integer format (e.g., "20-30", "1000-2000")
   - Width < 1: Decimal format with appropriate precision (e.g., "0.1-0.2")
   - Date spans: Date format (e.g., "2025-07-23")
   - Month spans: Month format (e.g., "2025-05")

Technical Implementation Details
===============================

**Architecture:**
* Uses Apache Calcite query planning engine for optimized execution
* Implements standard SPAN function for time-based binning
* Dynamic MIN/MAX calculation using window functions: ``MIN() OVER()`` and ``MAX() OVER()``
* Thread-local storage ensures consistent aligntime across multiple rows
* TimestampRounding class handles complex time alignment calculations

**Dynamic Data Analysis:**
* Algorithms calculate actual data range at query execution time
* No hardcoded values - works with any numeric dataset
* MIN/MAX values determined from actual field data using window functions

**Field Type Support:**
* **Time-based fields (timestamp, date, time, datetime)**: Full aligntime support with time modifiers, daily/monthly binning
* **Numeric fields**: Standard binning without aligntime, logarithmic binning support
* **String fields**: Not supported for binning operations

Limitations
===========
* The ``span``, ``minspan``, and ``bins`` parameters are mutually exclusive
* The ``aligntime`` parameter is only valid for time-based fields (timestamp, date, time, datetime types)
* For non-time fields, ``aligntime`` is ignored without error
* Only numeric and time fields can be binned
* Requires Calcite engine (not supported in legacy engine)
* Time modifier expressions currently support limited formats (``@d``, ``@d+<offset>``, ``@d-<offset>``)
* Aligntime expressions are case-sensitive

**Span Options Limitations:**
* **Log spans**: Only work with positive numeric values (logarithm of negative numbers is undefined)
* **Log coefficient**: Must be ≥ 1.0 and < base
* **Log base**: Must be > 1.0
* **Time scale units**: Case-sensitive for ``M`` (months) vs ``m`` (minutes)
* **Subsecond units**: Converted to millisecond precision internally, may have rounding limitations

**Supported Time Units (Full Timescale Specification):**
* **Subseconds**: ``us`` (microseconds), ``ms`` (milliseconds), ``cs`` (centiseconds), ``ds`` (deciseconds)
* **Seconds**: ``s``, ``sec``, ``secs``, ``second``, ``seconds``
* **Minutes**: ``m``, ``min``, ``mins``, ``minute``, ``minutes``
* **Hours**: ``h``, ``hr``, ``hrs``, ``hour``, ``hours``
* **Days**: ``d``, ``day``, ``days`` (uses precise daily binning algorithm)
* **Months**: ``M``, ``mon``, ``month``, ``months`` (uses precise monthly binning algorithm, case-sensitive: ``M`` = months, ``m`` = minutes)

**Daily and Monthly Binning Special Behavior:**
* **Daily spans** (``1days``, ``7days``, ``30days``) use precise daily binning algorithm
  - Returns date strings (``YYYY-MM-DD``) instead of timestamps
  - Uses Unix epoch (1970-01-01) as reference point for consistent alignment
  - Modular arithmetic ensures identical results for identical input dates
* **Monthly spans** (``1months``, ``4months``, ``6months``) use precise monthly binning algorithm
  - Returns month strings (``YYYY-MM``) instead of timestamps
  - Uses Unix epoch (January 1970) as reference point for consistent alignment
  - Modular arithmetic ensures identical results for identical input months




