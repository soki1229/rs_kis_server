use chrono::{Datelike, NaiveDate};

/// 2026 FOMC meeting dates (Fed publishes the schedule in advance each year).
/// Both days of each two-day meeting are blocked.
/// Source: Federal Reserve 2026 meeting calendar.
#[allow(dead_code)]
const FOMC_2026: &[(u32, u32)] = &[
    (1, 27),
    (1, 28), // January
    (3, 17),
    (3, 18), // March
    (4, 28),
    (4, 29), // April
    (6, 9),
    (6, 10), // June
    (7, 28),
    (7, 29), // July
    (9, 15),
    (9, 16), // September
    (10, 27),
    (10, 28), // October
    (12, 15),
    (12, 16), // December
];

/// Returns true if `date` falls on any FOMC meeting day (both days of two-day meetings).
/// Fail-closed: unknown years return true (block) to prevent trading through FOMC unprotected.
#[allow(dead_code)]
pub fn is_fomc_today(date: NaiveDate) -> bool {
    let year = date.year();
    let (month, day) = (date.month(), date.day());
    match year {
        2026 => FOMC_2026.contains(&(month, day)),
        _ => true, // fail-closed: block on unknown years until calendar is updated
    }
}

/// Returns true if the current year's FOMC calendar is defined.
pub fn has_calendar_for_year(year: i32) -> bool {
    year == 2026
}
