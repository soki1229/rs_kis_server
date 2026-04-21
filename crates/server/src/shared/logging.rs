use tracing_subscriber::fmt::time::OffsetTime;
use tracing_subscriber::EnvFilter;

pub fn init_logging() {
    // KST (UTC+9) offset
    let offset = time::UtcOffset::from_hms(9, 0, 0).expect("KST offset should be valid");

    // Cleaner timestamp: 2026-04-21 20:44:44.215
    let format = time::format_description::parse(
        "[year]-[month]-[day] [hour]:[minute]:[second].[subsecond digits:3]",
    )
    .expect("Custom log format should be valid");

    let timer = OffsetTime::new(offset, format);

    tracing_subscriber::fmt()
        .with_timer(timer)
        .with_target(true)
        .with_thread_ids(false)
        .with_file(false)
        .with_line_number(false)
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();
}
