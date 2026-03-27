
# timewrap

The `timewrap` command reshapes `timechart` output by wrapping each time period into a separate data series. This enables side-by-side comparisons of the same metric across recurring time intervals, such as day-over-day or week-over-week analysis.

## Syntax

The `timewrap` command has the following syntax:

```syntax
... | timechart <aggregation_function> ... | timewrap <span> [align=end|now]
```

## Parameters

The `timewrap` command supports the following parameters.

| Parameter | Required/Optional | Description |
| --- | --- | --- |
| `<span>` | Required | The wrapping interval, in the form `[int]<timescale>`. If the integer is omitted, `1` is assumed. For example, `1day`, `2week`, or just `day`. For a complete list of supported time units, see [Time units](#time-units). |
| `align` | Optional | Controls the reference point for period alignment and column naming. Default is `end`. `end` aligns to the search end time (the upper bound of the `where` clause on `@timestamp`, or current time if no time filter). `now` always aligns to the current query execution time. |

## Notes

The following considerations apply when using the `timewrap` command:

* The `timewrap` command must follow a `timechart` command. It is a post-processing command that reshapes timechart output.
* Column names follow the pattern `<aggregation>_<N><unit>_before` for periods before the reference point, `<aggregation>_latest_<unit>` for the period containing the reference point, and `<aggregation>_<N><unit>_after` for periods after the reference point.
* Column order is oldest first (leftmost) to newest (rightmost).
* Only columns with data are included in the output. Unused period columns are automatically removed.
* Incomplete periods (where data does not span the full wrap interval) show `null` for missing time offsets.
* Only `timechart` without the `BY` clause is currently supported. The `BY` clause is a future enhancement.

### Time units

The following time units are available for the `<span>` parameter:

* Seconds (`s`, `sec`, `second`, `secs`, `seconds`)
* Minutes (`m`, `min`, `minute`, `mins`, `minutes`) --- note: `m` means minutes, not months
* Hours (`h`, `hr`, `hour`, `hrs`, `hours`)
* Days (`d`, `day`, `days`)
* Weeks (`w`, `week`, `weeks`)

Variable-length time units (`month`, `quarter`, `year`) are not yet supported.

### Column naming

Column names are constructed from the aggregation function name and an absolute time offset from the reference point:

| Position relative to reference | Column name format | Example |
| --- | --- | --- |
| Before the reference point | `<agg>_<N><unit>_before` | `sum(requests)_2days_before` |
| At the reference point | `<agg>_latest_<unit>` | `sum(requests)_latest_day` |
| After the reference point | `<agg>_<N><unit>_after` | `sum(requests)_1day_after` |

## Example 1: Day-over-day comparison

The following query compares the sum of requests per 6-hour interval across 3 days:

```ppl
source=timewrap_test
| where @timestamp >= '2024-07-01 00:00:00' and @timestamp <= '2024-07-03 18:00:00'
| timechart span=6h sum(requests)
| timewrap 1day
```

```text
fetched rows / total rows = 4/4
+---------------------+----------------------------+---------------------------+--------------------------+
| @timestamp          | sum(requests)_2days_before | sum(requests)_1day_before | sum(requests)_latest_day |
|---------------------+----------------------------+---------------------------+--------------------------|
| 2024-07-03 00:00:00 | 180                        | 205                       | 165                      |
| 2024-07-03 06:00:00 | 240                        | 260                       | 225                      |
| 2024-07-03 12:00:00 | 310                        | 330                       | 285                      |
| 2024-07-03 18:00:00 | 190                        | 215                       | 165                      |
+---------------------+----------------------------+---------------------------+--------------------------+
```

Each column represents one day of data. The `latest_day` column contains the most recent period (July 3). The `2days_before` column contains the oldest period (July 1).

## Example 2: Comparing averages across 2 days

The following query compares the average requests per 6-hour interval:

```ppl
source=timewrap_test
| where @timestamp >= '2024-07-01 00:00:00' and @timestamp <= '2024-07-02 18:00:00'
| timechart span=6h avg(requests)
| timewrap 1day
```

```text
fetched rows / total rows = 4/4
+---------------------+---------------------------+--------------------------+
| @timestamp          | avg(requests)_1day_before | avg(requests)_latest_day |
|---------------------+---------------------------+--------------------------|
| 2024-07-02 00:00:00 | 90.0                      | 102.5                    |
| 2024-07-02 06:00:00 | 120.0                     | 130.0                    |
| 2024-07-02 12:00:00 | 155.0                     | 165.0                    |
| 2024-07-02 18:00:00 | 95.0                      | 107.5                    |
+---------------------+---------------------------+--------------------------+
```

## Example 3: Single day produces one period

When all data fits within a single wrap interval, only one period column is produced:

```ppl
source=timewrap_test
| where @timestamp >= '2024-07-01 00:00:00' and @timestamp <= '2024-07-01 18:00:00'
| timechart span=6h sum(requests)
| timewrap 1day
```

```text
fetched rows / total rows = 4/4
+---------------------+--------------------------+
| @timestamp          | sum(requests)_latest_day |
|---------------------+--------------------------|
| 2024-07-01 00:00:00 | 180                      |
| 2024-07-01 06:00:00 | 240                      |
| 2024-07-01 12:00:00 | 310                      |
| 2024-07-01 18:00:00 | 190                      |
+---------------------+--------------------------+
```

## Example 4: Count events day-over-day

```ppl
source=timewrap_test
| where @timestamp >= '2024-07-01 00:00:00' and @timestamp <= '2024-07-03 18:00:00'
| timechart span=6h count()
| timewrap 1day
```

```text
fetched rows / total rows = 4/4
+---------------------+----------------------+---------------------+--------------------+
| @timestamp          | count()_2days_before | count()_1day_before | count()_latest_day |
|---------------------+----------------------+---------------------+--------------------|
| 2024-07-03 00:00:00 | 2                    | 2                   | 2                  |
| 2024-07-03 06:00:00 | 2                    | 2                   | 2                  |
| 2024-07-03 12:00:00 | 2                    | 2                   | 2                  |
| 2024-07-03 18:00:00 | 2                    | 2                   | 2                  |
+---------------------+----------------------+---------------------+--------------------+
```

## Example 5: Comparing errors across 2 days

```ppl
source=timewrap_test
| where @timestamp >= '2024-07-02 00:00:00' and @timestamp <= '2024-07-03 18:00:00'
| timechart span=6h sum(errors)
| timewrap 1day
```

```text
fetched rows / total rows = 4/4
+---------------------+-------------------------+------------------------+
| @timestamp          | sum(errors)_1day_before | sum(errors)_latest_day |
|---------------------+-------------------------+------------------------|
| 2024-07-03 00:00:00 | 4                       | 1                      |
| 2024-07-03 06:00:00 | 6                       | 3                      |
| 2024-07-03 12:00:00 | 9                       | 6                      |
| 2024-07-03 18:00:00 | 3                       | 1                      |
+---------------------+-------------------------+------------------------+
```

## Limitations

The `timewrap` command has the following limitations:

* The `timewrap` command must follow a `timechart` command. Using it after any other command results in an error.
* Only `timechart` without the `BY` clause is supported. The `BY` clause (column split) is a future enhancement.
* Variable-length time units (`month`, `quarter`, `year`) are not yet supported. Use fixed-length units (`s`, `m`, `h`, `d`, `w`).
* The maximum number of period columns is 20. If the data contains more than 20 periods, only the 20 most recent are shown.
