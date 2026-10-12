// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Java temporal string formatting shared by casts and partition paths.

use chrono::{Datelike, NaiveDateTime, Timelike};

/// Format millis-since-midnight to `HH:mm:ss[.f...]` using Java `DateTimeUtils` semantics.
///
/// Matches Java `DateTimeUtils.formatTimestampMillis(time, precision)`:
/// outputs up to `precision` fractional digits, stopping after the first digit whose remaining fraction is zero.
pub(crate) fn format_time(millis_of_day: i32, precision: u32) -> String {
    let mut ms = millis_of_day;
    // Handle negative millis (same guard as Java)
    while ms < 0 {
        ms += 86_400_000;
    }
    let ms = ms as u32;
    let h = ms / 3_600_000 % 100;
    let m = (ms % 3_600_000) / 60_000;
    let s = (ms % 60_000) / 1_000;
    let mut frac_ms = ms % 1_000;

    let hms = format!("{h:02}:{m:02}:{s:02}");
    if precision == 0 {
        return hms;
    }

    // Emit up to `precision` digits, each extracted from the ms value.
    // Matches Java's digit-by-digit loop with trailing-zero break.
    let mut frac = String::with_capacity(precision as usize);
    let mut remaining = precision;
    while remaining > 0 {
        frac.push((b'0' + (frac_ms / 100) as u8) as char);
        frac_ms = (frac_ms % 100) * 10;
        if frac_ms == 0 {
            break;
        }
        remaining -= 1;
    }

    format!("{hms}.{frac}")
}

/// Format a timestamp using non-legacy `DateTimeUtils.formatTimestamp()` semantics.
///
/// Always uses space separator: `yyyy-MM-dd HH:mm:ss[.fraction]`.
/// Fraction: pad nano to 9 digits, strip trailing zeros while keeping at least `precision` digits.
pub(crate) fn format_timestamp(dt: NaiveDateTime, precision: u32) -> String {
    format_timestamp_parts(
        (dt.year(), dt.month(), dt.day()),
        (dt.hour(), dt.minute(), dt.second()),
        dt.nanosecond(),
        precision,
    )
}

/// Java Timestamp.toLocalDateTime stores its epoch day in a signed int.
/// Use Gregorian arithmetic so that this also covers dates beyond chrono's range.
pub(crate) fn format_epoch_timestamp(seconds: i64, nanos: u32, precision: u32) -> String {
    let (year, month, day) = epoch_day_to_date(seconds.div_euclid(86_400) as i32);
    let time = seconds.rem_euclid(86_400) as u32;
    format_timestamp_parts(
        (year, month, day),
        (time / 3600, time / 60 % 60, time % 60),
        nanos,
        precision,
    )
}

fn epoch_day_to_date(day: i32) -> (i32, u32, u32) {
    // Gregorian eras start on March 1, so the leap day is at the end of a year.
    let days = i64::from(day) + 719_468;
    let era = days.div_euclid(146_097);
    let day_of_era = days - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let mut year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let march_month = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * march_month + 2) / 5 + 1;
    let month = march_month + if march_month < 10 { 3 } else { -9 };
    year += i64::from(month <= 2);
    (year as i32, month as u32, day as u32)
}

fn format_timestamp_parts(
    date: (i32, u32, u32),
    time: (u32, u32, u32),
    nano: u32,
    precision: u32,
) -> String {
    let (year, month, day) = date;
    let (hour, minute, second) = time;
    // DateTimeUtils.int4 writes exactly four digits using signed division and
    // remainder, including years outside 0000..9999.
    let year = [1000, 100, 10, 1]
        .into_iter()
        .map(|divisor| (i32::from(b'0') + year / divisor % 10) as u8 as char)
        .collect::<String>();
    let ymdhms = format!(
        "{year}-{:02}-{:02} {:02}:{:02}:{:02}",
        month, day, hour, minute, second
    );

    // Whole seconds keep the declared precision; nonzero fractions are never truncated.
    let nano_str = format!("{nano:09}");
    let mut fraction = &nano_str[..];

    // Strip trailing zeros, but don't go below the precision boundary.
    while fraction.len() > precision as usize && fraction.ends_with('0') {
        fraction = &fraction[..fraction.len() - 1];
    }

    if fraction.is_empty() {
        ymdhms
    } else {
        format!("{ymdhms}.{fraction}")
    }
}
