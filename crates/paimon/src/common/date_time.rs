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

//! Java temporal calendars, local-zone conversions and string formatting.

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
    let (year, month, day) = epoch_day_to_date(i64::from(seconds.div_euclid(86_400) as i32));
    let time = seconds.rem_euclid(86_400) as u32;
    format_timestamp_parts(
        (year, month, day),
        (time / 3600, time / 60 % 60, time % 60),
        nanos,
        precision,
    )
}

/// SQL timestamp CAST from Paimon's full millisecond and sub-millisecond storage.
pub(crate) fn format_timestamp_millis(
    millis: i64,
    sub_milli: i64,
    precision: u32,
    zone: Option<&jiff::tz::TimeZone>,
) -> String {
    let millis = match zone {
        Some(zone) => {
            millis.wrapping_add(i64::from(local_offset(millis.div_euclid(1000), zone)) * 1000)
        }
        None => millis,
    };
    let nanos = (millis.rem_euclid(1000) * 1_000_000 + sub_milli) as u32;
    format_epoch_timestamp(millis.div_euclid(1000), nanos, precision)
}

/// Java Timestamp.toString uses LocalDateTime's ISO form, not a SQL CAST form.
pub fn format_iso_timestamp(seconds: i64, nanos: u32) -> String {
    let (year, month, day) = epoch_day_to_date(i64::from(seconds.div_euclid(86_400) as i32));
    let year = if (0..=9999).contains(&year) {
        format!("{year:04}")
    } else if year < 0 {
        format!("-{:04}", -year)
    } else {
        format!("+{year}")
    };
    let time = seconds.rem_euclid(86_400);
    let second = time % 60;
    let mut text = format!(
        "{year}-{month:02}-{day:02}T{:02}:{:02}",
        time / 3600,
        time / 60 % 60
    );
    if second != 0 || nanos != 0 {
        text.push_str(&format!(":{second:02}"));
    }
    if nanos != 0 {
        let precision = if nanos.is_multiple_of(1_000_000) {
            3
        } else if nanos.is_multiple_of(1000) {
            6
        } else {
            9
        };
        text.push('.');
        text.push_str(&format!("{nanos:09}")[..precision]);
    }
    text
}

pub(crate) fn epoch_day_to_date(day: i64) -> (i32, u32, u32) {
    // Gregorian eras start on March 1, so the leap day is at the end of a year.
    let days = day + 719_468;
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

/// Java ZoneRules uses its earliest wall offset before recorded history and
/// repeats its final transition rules on the Gregorian 400-year cycle.
pub(crate) fn local_offset(seconds: i64, zone: &jiff::tz::TimeZone) -> i32 {
    let timestamp = jiff::Timestamp::from_second(seconds).unwrap_or_else(|_| {
        if seconds < 0 {
            jiff::Timestamp::MIN
        } else {
            jiff::Timestamp::from_second(project_future_seconds(seconds)).unwrap()
        }
    });
    zone.to_offset(timestamp).seconds()
}

fn project_future_seconds(seconds: i64) -> i64 {
    const CYCLE_SECONDS: i64 = 146_097 * 86_400;
    seconds.rem_euclid(CYCLE_SECONDS) + CYCLE_SECONDS
}

/// LocalDateTime.atZone: earlier offset in an overlap; shift forward in a gap.
pub(crate) fn local_to_utc_seconds(seconds: i64, zone: &jiff::tz::TimeZone) -> crate::Result<i64> {
    let projected = match jiff::Timestamp::from_second(seconds) {
        Ok(timestamp) => timestamp,
        Err(_) if seconds < 0 => return Ok(seconds - i64::from(local_offset(seconds, zone))),
        Err(_) => jiff::Timestamp::from_second(project_future_seconds(seconds)).unwrap(),
    };
    let datetime = jiff::tz::TimeZone::UTC.to_datetime(projected);
    let utc = match zone.to_timestamp(datetime) {
        Ok(utc) => utc,
        Err(_) if seconds < 0 => {
            // A valid boundary wall time can have a UTC instant below jiff's
            // minimum. Java can still resolve it with the earliest offset.
            return Ok(seconds - i64::from(local_offset(seconds, zone)));
        }
        Err(_) => {
            // The same edge exists at the upper bound. Resolve an equivalent
            // future calendar date before applying its offset to the wide value.
            let projected = jiff::Timestamp::from_second(project_future_seconds(seconds)).unwrap();
            let datetime = jiff::tz::TimeZone::UTC.to_datetime(projected);
            let utc = zone
                .to_timestamp(datetime)
                .map_err(|error| crate::Error::DataInvalid {
                    message: format!("Cannot resolve local timestamp in {zone:?}: {error}"),
                    source: None,
                })?;
            return Ok(seconds + utc.as_second() - projected.as_second());
        }
    };
    Ok(seconds + utc.as_second() - projected.as_second())
}

/// DateTimeUtils.toInternal(LocalDate), including Java int overflow and signed division.
pub(crate) fn epoch_day_to_java_date(day: i64) -> i32 {
    let (year, month, day) = epoch_day_to_date(day);
    ymd_to_java_date(year, month as i32, day as i32)
}

pub(crate) fn ymd_to_java_date(year: i32, month: i32, day: i32) -> i32 {
    let a = (14 - month) / 12;
    let y = year + 4800 - a;
    let m = month + 12 * a - 3;
    (day + (153 * m + 2) / 5)
        .wrapping_add(365_i32.wrapping_mul(y))
        .wrapping_add(y / 4 - y / 100 + y / 400 - 32_045)
        .wrapping_sub(2_440_588)
}

/// DateTimeUtils.toLocalDate uses signed int Julian arithmetic. Validate the
/// resulting date as LocalDate.of does, then recover its proleptic epoch day.
pub(crate) fn java_date_to_epoch_day(date: i32) -> crate::Result<i64> {
    let (year, month, day) = java_date_parts(date);
    let leap = year % 400 == 0 || year % 4 == 0 && year % 100 != 0;
    let days = match month {
        2 => {
            if leap {
                29
            } else {
                28
            }
        }
        4 | 6 | 9 | 11 => 30,
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        _ => 0,
    };
    if day < 1 || day > days {
        return Err(crate::Error::DataInvalid {
            message: format!("Invalid Java DATE conversion: {year}-{month}-{day}"),
            source: None,
        });
    }
    Ok(ymd_to_epoch_day(year, month, day))
}

pub(crate) fn java_date_parts(date: i32) -> (i32, i32, i32) {
    let j = date.wrapping_add(2_440_588).wrapping_add(32_044);
    let g = j / 146_097;
    let dg = j % 146_097;
    let c = (dg / 36_524 + 1) * 3 / 4;
    let dc = dg - c * 36_524;
    let b = dc / 1461;
    let db = dc % 1461;
    let a = (db / 365 + 1) * 3 / 4;
    let da = db - a * 365;
    let y = g * 400 + c * 100 + b * 4 + a;
    let m = (da * 5 + 308) / 153 - 2;
    let d = da - (m + 4) * 153 / 5 + 122;
    let year = y - 4800 + (m + 2) / 12;
    let month = (m + 2) % 12 + 1;
    let day = d + 1;
    (year, month, day)
}

pub(crate) fn ymd_to_epoch_day(year: i32, month: i32, day: i32) -> i64 {
    let year = i64::from(year) - i64::from(month <= 2);
    let era = year.div_euclid(400);
    let yoe = year - era * 400;
    let march = i64::from(month) + if month > 2 { -3 } else { 9 };
    era * 146_097 + yoe * 365 + yoe / 4 - yoe / 100 + (153 * march + 2) / 5 + i64::from(day)
        - 1
        - 719_468
}
