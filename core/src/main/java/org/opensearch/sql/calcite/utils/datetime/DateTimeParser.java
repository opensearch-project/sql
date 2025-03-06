package org.opensearch.sql.calcite.utils.datetime;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

public class DateTimeParser {
    /**
     * Parse a string as MySQL would parse into a LocalDateTime
     *   - If it's purely numeric, interpret based on length (YYYYMMDD, YYYYMMDDHHMMSS, etc.).
     *   - If it contains standard delimiters, try date/time/datetime patterns.
     *   - If only date is found, time is set to 00:00:00.
     *   - If only time is found, date is set to 1970-01-01.
     *
     * @param input the date/time/timestamp string
     * @return a LocalDateTime
     * @throws IllegalArgumentException if parsing fails
     */
    public static LocalDateTime parse(String input) {
        if (input == null || input.trim().isEmpty()) {
            throw new IllegalArgumentException("Cannot parse a null/empty date-time string.");
        }
        input = input.trim();

        // 1) Try purely numeric approach (e.g., 20230305, 20230305121015, etc.)
        if (isAllDigits(input)) {
            LocalDateTime numericParsed = tryParseNumeric(input);
            if (numericParsed != null) {
                return numericParsed;
            }
        }

        // 2) Try standard patterns with delimiters/spaces
        LocalDateTime patternParsed = tryParseWithPatterns(input);
        if (patternParsed != null) {
            return patternParsed;
        }

        throw new IllegalArgumentException("Unable to parse date/time string:  " + input);
    }

    /**
     * Checks if a string is all digits (no sign, no decimal).
     */
    private static boolean isAllDigits(String s) {
        for (char c : s.toCharArray()) {
            if (!Character.isDigit(c)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Attempts to parse a purely numeric string (without delimiters),
     *
     *   Length | Interpretation           | Example
     *   -------|--------------------------|----------------
     *     4    | YYMM                    | "0512" => ambiguous
     *     6    | YYMMDD                  | "990101" => 1999-01-01 00:00:00
     *     8    | YYYYMMDD                | "20230305" => 2023-03-05 00:00:00
     *    12    | YYYYMMDDHHMM            | "202303051210" => 2023-03-05 12:10:00
     *    14    | YYYYMMDDHHMMSS          | "20230305121030" => 2023-03-05 12:10:30
     *
     * Return null if none match.
     */
    private static LocalDateTime tryParseNumeric(String digits) {
        int len = digits.length();

        // 6 digits => YYMMDD
        if (len == 6) {
            return parseYYMMDD(digits);
        }
        // 8 digits => YYYYMMDD
        else if (len == 8) {
            return parseYYYYMMDD(digits);
        }
        // 12 digits => YYYYMMDDHHMM
        else if (len == 12) {
            return parseYYYYMMDDHHMM(digits);
        }
        // 14 digits => YYYYMMDDHHMMSS
        else if (len == 14) {
            return parseYYYYMMDDHHMMSS(digits);
        }
        // others - not handled in this simplified approach, return null
        return null;
    }

    private static LocalDateTime parseYYMMDD(String digits) {
        int yy = Integer.parseInt(digits.substring(0, 2));
        int mm = Integer.parseInt(digits.substring(2, 4));
        int dd = Integer.parseInt(digits.substring(4, 6));

        int fullYear = (yy >= 70 ? (1900 + yy) : (2000 + yy));

        return LocalDateTime.of(fullYear, mm, dd, 0, 0, 0);
    }

    private static LocalDateTime parseYYYYMMDD(String digits) {
        int yyyy = Integer.parseInt(digits.substring(0, 4));
        int mm   = Integer.parseInt(digits.substring(4, 6));
        int dd   = Integer.parseInt(digits.substring(6, 8));

        return LocalDateTime.of(yyyy, mm, dd, 0, 0, 0);
    }

    private static LocalDateTime parseYYYYMMDDHHMM(String digits) {
        int yyyy = Integer.parseInt(digits.substring(0, 4));
        int mm   = Integer.parseInt(digits.substring(4, 6));
        int dd   = Integer.parseInt(digits.substring(6, 8));
        int hh   = Integer.parseInt(digits.substring(8, 10));
        int min  = Integer.parseInt(digits.substring(10, 12));

        return LocalDateTime.of(yyyy, mm, dd, hh, min, 0);
    }

    private static LocalDateTime parseYYYYMMDDHHMMSS(String digits) {
        int yyyy = Integer.parseInt(digits.substring(0, 4));
        int mm   = Integer.parseInt(digits.substring(4, 6));
        int dd   = Integer.parseInt(digits.substring(6, 8));
        int hh   = Integer.parseInt(digits.substring(8, 10));
        int min  = Integer.parseInt(digits.substring(10, 12));
        int ss   = Integer.parseInt(digits.substring(12, 14));

        return LocalDateTime.of(yyyy, mm, dd, hh, min, ss);
    }

    /**
     * Try a set of date/time/datetime patterns with delimiters/spaces.
     * Return the first successful parse or null otherwise.
     */
    private static LocalDateTime tryParseWithPatterns(String input) {
        // Separate patterns into "datetime", "date", and "time" sets.
        // This ensures we know exactly how to handle the parsed result.

        DateTimeFormatter[] dateTimePatterns = {
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"),
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"),
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm")
        };

        for (DateTimeFormatter dtf : dateTimePatterns) {
            try {
                return LocalDateTime.parse(input, dtf);
            } catch (DateTimeParseException e) {
                // ignored, try next
            }
        }

        DateTimeFormatter[] datePatterns = {
                DateTimeFormatter.ofPattern("yyyy-MM-dd")
        };

        for (DateTimeFormatter dp : datePatterns) {
            try {
                LocalDate date = LocalDate.parse(input, dp);
                // If parsed as date, attach 00:00:00
                return date.atStartOfDay();
            } catch (DateTimeParseException e) {
                // ignored, try next
            }
        }

        DateTimeFormatter[] timePatterns = {
                DateTimeFormatter.ofPattern("HH:mm:ss.SSS"),
                DateTimeFormatter.ofPattern("HH:mm:ss"),
                DateTimeFormatter.ofPattern("HH:mm")
        };

        for (DateTimeFormatter tp : timePatterns) {
            try {
                LocalTime time = LocalTime.parse(input, tp);
                // If parsed as time, attach date 1970-01-01
                return LocalDateTime.of(LocalDate.now(ZoneId.of("UTC")), time);
            } catch (DateTimeParseException e) {
                // ignored, try next
            }
        }

        // If none matched:
        return null;
    }
}
