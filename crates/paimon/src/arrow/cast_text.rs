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

//! BinaryStringUtils date/timestamp text and DateTimeUtils DATE formatting.

use crate::common::date_time::{
    java_date_parts, local_to_utc_seconds, ymd_to_epoch_day, ymd_to_java_date,
};
use crate::common::number::{java_decimal_digit, parse_java_integer};

fn invalid(text: &str) -> crate::Error {
    crate::Error::DataInvalid {
        message: format!("Invalid Java temporal value: '{text}'"),
        source: None,
    }
}

fn numeric(text: &str) -> bool {
    let text = text.strip_prefix('-').unwrap_or(text);
    !text.is_empty() && text.chars().all(|ch| java_decimal_digit(ch).is_some())
}

pub(super) fn format_date(value: i32) -> String {
    let (year, month, day) = java_date_parts(value);
    let mut text = String::with_capacity(10);
    for (value, divisors) in [
        (year, &[1000, 100, 10, 1][..]),
        (month, &[10, 1][..]),
        (day, &[10, 1][..]),
    ] {
        if !text.is_empty() {
            text.push('-');
        }
        for divisor in divisors {
            text.push((i32::from(b'0') + value / divisor % 10) as u8 as char);
        }
    }
    text
}

fn days_in_month(year: i32, month: i32) -> i32 {
    match month {
        2 => {
            if year % 400 == 0 || year % 4 == 0 && year % 100 != 0 {
                29
            } else {
                28
            }
        }
        4 | 6 | 9 | 11 => 30,
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        _ => 0,
    }
}

pub(super) fn parse_date(text: &str) -> crate::Result<i32> {
    if numeric(text) {
        return parse_java_integer(text, i64::from(i32::MIN), i64::from(i32::MAX))
            .map(|v| v as i32);
    }
    let date = match text.find(' ') {
        Some(position) if position > 0 => &text[..position],
        _ => text,
    };
    let components = date.split('-').collect::<Vec<_>>();
    if components.len() > 3 {
        return Err(invalid(text));
    }
    let mut parsed = [0, 1, 1];
    for (index, component) in components.iter().enumerate() {
        let component = component.trim_matches(|ch| ch <= ' ');
        if component.is_empty() || !component.bytes().all(|ch| ch.is_ascii_digit()) {
            return Err(invalid(text));
        }
        parsed[index] = component.parse::<i32>().map_err(|_| invalid(text))?;
    }
    let [year, month, day] = parsed;
    if !(0..=9999).contains(&year) || day < 1 || day > days_in_month(year, month) {
        return Err(invalid(text));
    }
    Ok(ymd_to_java_date(year, month, day))
}

// DateTimeFormatter's consecutive optional patterns [MM][M], [dd][d],
// [HH][H], etc. may repeat a value, but conflicting repeated fields fail.
fn component(text: &str) -> Option<Option<i32>> {
    if text.is_empty() {
        return Some(None);
    }
    if text.len() > 21 || !text.bytes().all(|ch| ch.is_ascii_digit()) {
        return None;
    }
    if text.len() <= 2 {
        return text.parse().ok().map(Some);
    }
    let first: i32 = text[..2].parse().ok()?;
    let repeated: i32 = text[2..].parse().ok()?;
    (first == repeated).then_some(Some(first))
}

fn time_components(text: &str, space: bool) -> Option<[Option<i32>; 4]> {
    let parts = text.split(':').collect::<Vec<_>>();
    if parts.len() < if space { 3 } else { 2 } || parts.len() > 3 {
        return None;
    }
    let mut time = [None; 4];
    time[0] = component(parts[0])?;
    time[1] = component(parts[1])?;
    if parts.len() == 3 {
        let (second, fraction) = parts[2]
            .split_once('.')
            .map_or((parts[2], None), |(s, f)| (s, Some(f)));
        time[2] = component(second)?;
        if let Some(fraction) = fraction {
            if fraction.len() > 9 || !fraction.bytes().all(|ch| ch.is_ascii_digit()) {
                return None;
            }
            time[3] = Some(if fraction.is_empty() {
                0
            } else {
                fraction.parse::<i32>().ok()? * 10_i32.pow(9 - fraction.len() as u32)
            });
        }
    }
    Some(time)
}

pub(crate) fn parse_timestamp(
    text: &str,
    precision: u32,
    zone: Option<&jiff::tz::TimeZone>,
) -> crate::Result<(i64, u32)> {
    let local = zone.is_some();
    if !local && numeric(text) {
        let ticks = parse_java_integer(text, i64::MIN, i64::MAX)?;
        return Ok(match precision {
            0 => (ticks.wrapping_mul(1000), 0),
            3 => (ticks, 0),
            6 => (
                ticks.div_euclid(1000),
                (ticks.rem_euclid(1000) * 1000) as u32,
            ),
            9 => (
                ticks.div_euclid(1_000_000),
                ticks.rem_euclid(1_000_000) as u32,
            ),
            _ => return Err(invalid(text)),
        });
    }
    let first_time = text.find([' ', 'T']).unwrap_or(text.len());
    let date = &text[..first_time];
    let parts = date.split('-').collect::<Vec<_>>();
    if parts.len() != 3 {
        return Err(invalid(text));
    }
    if parts[1].is_empty() && !parts[2].is_empty() {
        return Err(invalid(text));
    }
    let year_text = parts[0];
    let year_digits = year_text.strip_prefix('+').unwrap_or(year_text);
    if !year_digits.bytes().all(|ch| ch.is_ascii_digit())
        || (if year_text.starts_with('+') {
            year_digits.len() <= 4 || year_digits.len() > 19
        } else {
            year_digits.len() != 4
        })
    {
        return Err(invalid(text));
    }
    let year: i32 = year_digits.parse().map_err(|_| invalid(text))?;
    if !(1..=999_999_999).contains(&year) {
        return Err(invalid(text));
    }
    let complete_date = !parts[1].is_empty() && !parts[2].is_empty();
    let month = component(parts[1])
        .ok_or_else(|| invalid(text))?
        .unwrap_or(1);
    let day = component(parts[2])
        .ok_or_else(|| invalid(text))?
        .unwrap_or(1);
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return Err(invalid(text));
    }
    // The formatter's SMART resolver clamps a day within 1..31 to month end.
    let mut epoch_day = ymd_to_epoch_day(year, month, day.min(days_in_month(year, month)));
    let mut time = [None; 4];
    let mut rest = &text[first_time..];
    if let Some(after_space) = rest.strip_prefix(' ') {
        let end = after_space.find('T').unwrap_or(after_space.len());
        time = time_components(&after_space[..end], true).ok_or_else(|| invalid(text))?;
        rest = &after_space[end..];
    }
    if let Some(after_t) = rest.strip_prefix('T') {
        let other = time_components(after_t, false).ok_or_else(|| invalid(text))?;
        for (old, new) in time.iter_mut().zip(other) {
            if old.is_some() && new.is_some() && *old != new {
                return Err(invalid(text));
            }
            if new.is_some() {
                *old = new;
            }
        }
        rest = "";
    }
    if !rest.is_empty() {
        return Err(invalid(text));
    }
    if time[0] == Some(24)
        && (time[1].is_none() && time[2].is_some() || time[2].is_none() && time[3].is_some())
    {
        return Err(invalid(text));
    }
    let [hour, minute, second, nanos] = time.map(|v| v.unwrap_or(0));
    if !(0..=24).contains(&hour)
        || !(0..=59).contains(&minute)
        || !(0..=59).contains(&second)
        || hour == 24 && (minute != 0 || second != 0 || nanos != 0)
    {
        return Err(invalid(text));
    }
    if hour == 24 && complete_date {
        epoch_day += 1;
    }
    let seconds = epoch_day * 86_400 + i64::from(hour % 24 * 3600 + minute * 60 + second);
    let nanos = nanos as u32 / 10_u32.pow(9 - precision) * 10_u32.pow(9 - precision);
    let millis = if local {
        local_to_utc_seconds(seconds, zone.unwrap())?.wrapping_mul(1000)
    } else {
        seconds.wrapping_mul(1000)
    };
    Ok((
        millis.wrapping_add(i64::from(nanos / 1_000_000)),
        nanos % 1_000_000,
    ))
}
