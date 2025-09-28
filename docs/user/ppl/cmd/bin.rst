=============
bin
=============

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2


.. note::
   
   Available since version 3.3


Description
============
| The ``bin`` command groups numeric values into buckets of equal intervals, making it useful for creating histograms and analyzing data distribution. It takes a numeric field and generates a new field with values that represent the lower bound of each bucket.

Syntax
============
bin <field> [span=<interval>] [minspan=<interval>] [bins=<count>] [aligntime=(earliest | latest | <time-specifier>)] [start=<value>] [end=<value>]

* field: mandatory. The numeric field to bin.
* span: optional. The interval size for each bin. Cannot be used with bins or minspan parameters.
* minspan: optional. The minimum interval size for automatic span calculation. Cannot be used with span or bins parameters.
* bins: optional. The maximum number of equal-width bins to create. Cannot be used with span or minspan parameters.
* aligntime: optional. Align the bin times for time-based fields. Valid only for time-based discretization. Options:
  - earliest: Align bins to the earliest timestamp in the data
  - latest: Align bins to the latest timestamp in the data  
  - <time-specifier>: Align bins to a specific epoch time value or time modifier expression
* start: optional. The starting value for binning range. If not specified, uses the minimum field value.
* end: optional. The ending value for binning range. If not specified, uses the maximum field value.

Parameter Priority Order
========================
When multiple parameters are specified, the bin command follows this priority order:

1. **span** (highest priority) - Set the interval for binning
2. **minspan** (second priority) - Set the Minimum span for binning
3. **bins** (third priority) - Sets the maximum amount of bins
4. **start/end** (fourth priority) - Expand the range for binning
5. **default** (lowest priority) - Automatic magnitude-based binning

**Note**: The **aligntime** parameter is a modifier that only applies to span-based binning (when using **span**) for time-based fields. It does not affect the priority order for bin type selection.

Parameters
============

span Parameter
--------------
Specifies the width of each bin interval with support for multiple span types:

**1. Numeric Span **
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
- **Months**: ``mon``, ``month``, ``months`` - **Uses precise monthly binning algorithm**
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
Automatically calculates the span using a mathematical O(1) algorithm to create human-readable bin widths based on powers of 10.

**Validation**: The bins parameter must be between 2 and 50000 (inclusive). Values outside this range will result in an error.

The algorithm uses **mathematical optimization** instead of iteration for O(1) performance:

1. **Validate bins**: Ensure ``2 ≤ bins ≤ 50000``
2. **Calculate data range**: ``data_range = max_value - min_value``
3. **Calculate target width**: ``target_width = data_range / requested_bins``
4. **Find optimal starting point**: ``exponent = CEIL(LOG10(target_width))``
5. **Select optimal width**: ``optimal_width = 10^exponent``
6. **Account for boundaries**: If ``max_value % optimal_width == 0``, add one extra bin
7. **Adjust if needed**: If ``actual_bins > requested_bins``, use ``10^(exponent + 1)``

**Mathematical Formula**:
- ``optimal_width = 10^CEIL(LOG10(data_range / requested_bins))``
- **Boundary condition**: ``actual_bins = CEIL(data_range / optimal_width) + (max_value % optimal_width == 0 ? 1 : 0)``

**Example**: For age data with range 20-50 (range=30) and bins=3:
- ``target_width = 30 / 3 = 10``
- ``exponent = CEIL(LOG10(10)) = CEIL(1.0) = 1``
- ``optimal_width = 10^1 = 10``
- ``actual_bins = CEIL(30/10) = 3`` ≤ 3
- Result: Use width=10, creating bins "20-30", "30-40", "40-50"

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

Examples
========

Span Parameter Examples
=======================

Example 1: Basic numeric span
==============================

PPL query::

    os> source=accounts | bin age span=10 | fields age, account_number | head 3;
    fetched rows / total rows = 3/3
    +-------+----------------+
    | age   | account_number |
    |-------+----------------|
    | 30-40 | 1              |
    | 30-40 | 6              |
    | 20-30 | 13             |
    +-------+----------------+

Example 2: Large numeric span
==============================

PPL query::

    os> source=accounts | bin balance span=25000 | fields balance | head 2;
    fetched rows / total rows = 2/2
    +-------------+
    | balance     |
    |-------------|
    | 25000-50000 |
    | 0-25000     |
    +-------------+


Example 3: Logarithmic span (log10)
====================================

PPL query::

    os> source=accounts | bin balance span=log10 | fields balance | head 2;
    fetched rows / total rows = 2/2
    +------------------+
    | balance          |
    |------------------|
    | 10000.0-100000.0 |
    | 1000.0-10000.0   |
    +------------------+

Example 4: Logarithmic span with coefficient
=============================================

PPL query::

    os> source=accounts | bin balance span=2log10 | fields balance | head 3;
    fetched rows / total rows = 3/3
    +------------------+
    | balance          |
    |------------------|
    | 20000.0-200000.0 |
    | 2000.0-20000.0   |
    | 20000.0-200000.0 |
    +------------------+

Bins Parameter Examples
=======================

Example 5: Basic bins parameter
================================

PPL query::

    os> source=time_test | bin value bins=5 | fields value | head 3;
    fetched rows / total rows = 3/3
    +------------+
    | value      |
    |------------|
    | 8000-9000  |
    | 7000-8000  |
    | 9000-10000 |
    +------------+

Example 6: Low bin count
=========================

PPL query::

    os> source=accounts | bin age bins=2 | fields age | head 1;
    fetched rows / total rows = 1/1
    +-------+
    | age   |
    |-------|
    | 30-40 |
    +-------+

Example 7: High bin count
==========================

PPL query::

    os> source=accounts | bin age bins=21 | fields age, account_number | head 3;
    fetched rows / total rows = 3/3
    +-------+----------------+
    | age   | account_number |
    |-------+----------------|
    | 32-33 | 1              |
    | 36-37 | 6              |
    | 28-29 | 13             |
    +-------+----------------+

Minspan Parameter Examples
==========================

Example 8: Basic minspan
=========================

PPL query::

    os> source=accounts | bin age minspan=5 | fields age, account_number | head 3;
    fetched rows / total rows = 3/3
    +-------+----------------+
    | age   | account_number |
    |-------+----------------|
    | 30-40 | 1              |
    | 30-40 | 6              |
    | 20-30 | 13             |
    +-------+----------------+

Example 9: Large minspan
==========================

PPL query::

    os> source=accounts | bin age minspan=101 | fields age | head 1;
    fetched rows / total rows = 1/1
    +--------+
    | age    |
    |--------|
    | 0-1000 |
    +--------+

Start/End Parameter Examples
============================

Example 10: Start and end range
================================

PPL query::

    os> source=accounts | bin age start=0 end=101 | fields age | head 1;
    fetched rows / total rows = 1/1
    +-------+
    | age   |
    |-------|
    | 0-100 |
    +-------+

Example 11: Large end range
============================

PPL query::

    os> source=accounts | bin balance start=0 end=100001 | fields balance | head 1;
    fetched rows / total rows = 1/1
    +----------+
    | balance  |
    |----------|
    | 0-100000 |
    +----------+

Example 12: Span with start/end
================================

PPL query::

    os> source=accounts | bin age span=1 start=25 end=35 | fields age | head 6;
    fetched rows / total rows = 4/4
    +-------+
    | age   |
    |-------|
    | 32-33 |
    | 36-37 |
    | 28-29 |
    | 33-34 |
    +-------+

Time-based Examples
===================

Example 13: Hour span
======================

PPL query::

    os> source=time_test | bin @timestamp span=1h | fields @timestamp, value | head 3;
    fetched rows / total rows = 3/3
    +---------------------+-------+
    | @timestamp          | value |
    |---------------------+-------|
    | 2025-07-28 00:00:00 | 8945  |
    | 2025-07-28 01:00:00 | 7623  |
    | 2025-07-28 02:00:00 | 9187  |
    +---------------------+-------+

Example 14: Minute span
========================

PPL query::

    os> source=time_test | bin @timestamp span=45minute | fields @timestamp, value | head 3;
    fetched rows / total rows = 3/3
    +---------------------+-------+
    | @timestamp          | value |
    |---------------------+-------|
    | 2025-07-28 00:00:00 | 8945  |
    | 2025-07-28 01:30:00 | 7623  |
    | 2025-07-28 02:15:00 | 9187  |
    +---------------------+-------+

Example 15: Second span
========================

PPL query::

    os> source=time_test | bin @timestamp span=30seconds | fields @timestamp, value | head 3;
    fetched rows / total rows = 3/3
    +---------------------+-------+
    | @timestamp          | value |
    |---------------------+-------|
    | 2025-07-28 00:15:30 | 8945  |
    | 2025-07-28 01:42:00 | 7623  |
    | 2025-07-28 02:28:30 | 9187  |
    +---------------------+-------+

Example 16: Daily span
=======================

PPL query::

    os> source=time_test | bin @timestamp span=7day | fields @timestamp, value | head 3;
    fetched rows / total rows = 3/3
    +---------------------+-------+
    | @timestamp          | value |
    |---------------------+-------|
    | 2025-07-24 00:00:00 | 8945  |
    | 2025-07-24 00:00:00 | 7623  |
    | 2025-07-24 00:00:00 | 9187  |
    +---------------------+-------+

Aligntime Parameter Examples
============================

Example 17: Aligntime with time modifier
=========================================

PPL query::

    os> source=time_test | bin @timestamp span=2h aligntime='@d+3h' | fields @timestamp, value | head 3;
    fetched rows / total rows = 3/3
    +---------------------+-------+
    | @timestamp          | value |
    |---------------------+-------|
    | 2025-07-27 23:00:00 | 8945  |
    | 2025-07-28 01:00:00 | 7623  |
    | 2025-07-28 01:00:00 | 9187  |
    +---------------------+-------+

Example 18: Aligntime with epoch timestamp
===========================================

PPL query::

    os> source=time_test | bin @timestamp span=2h aligntime=1500000000 | fields @timestamp, value | head 3;
    fetched rows / total rows = 3/3
    +---------------------+-------+
    | @timestamp          | value |
    |---------------------+-------|
    | 2025-07-27 22:40:00 | 8945  |
    | 2025-07-28 00:40:00 | 7623  |
    | 2025-07-28 00:40:00 | 9187  |
    +---------------------+-------+

Default Binning Example
=======================

Example 19: Default behavior (no parameters)
==============================================

PPL query::

    os> source=accounts | bin age | fields age, account_number | head 3;
    fetched rows / total rows = 3/3
    +-----------+----------------+
    | age       | account_number |
    |-----------+----------------|
    | 32.0-33.0 | 1              |
    | 36.0-37.0 | 6              |
    | 28.0-29.0 | 13             |
    +-----------+----------------+

