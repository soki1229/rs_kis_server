use crate::types::MarketRegime;

pub struct SetupScoreInput {
    pub ma5_above_ma20: bool,
    pub volume_ratio: f64,
    pub recent_5min_volume_ratio: f64,
    pub bid_ask_imbalance: f64,
    pub new_high_last_10min: bool,
    pub has_news_catalyst: bool,
    pub daily_change_pct: f64,
    pub mins_until_close: i64,
    pub entry_blackout_close_mins: i64,
    pub regime: MarketRegime,
}

pub fn calculate_setup_score(input: &SetupScoreInput) -> i32 {
    let mut score: i32 = 0;

    if input.ma5_above_ma20 {
        score += 20;
    }
    if input.volume_ratio >= 2.0 {
        score += 20;
    }
    if input.recent_5min_volume_ratio >= 1.5 {
        score += 15;
    }
    if input.bid_ask_imbalance >= 1.3 {
        score += 20;
    }
    if input.new_high_last_10min {
        score += 15;
    }
    if input.has_news_catalyst {
        score += 10;
    }

    if input.daily_change_pct > 7.0 {
        score -= 15;
    }
    if input.mins_until_close < input.entry_blackout_close_mins * 2 {
        score -= 10;
    }
    if input.regime == MarketRegime::Volatile {
        score -= 10;
    }

    score
}
