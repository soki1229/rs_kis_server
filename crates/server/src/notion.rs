use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::future::Future;
use tokio_util::sync::CancellationToken;

const NOTION_VERSION: &str = "2022-06-28";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotionIds {
    pub daily_reports_db: String,
    pub param_changes_db: String,
    pub system_events_db: String,
    pub signal_evals_db: String,
    pub connection_tests_page: String,
}

#[derive(Clone)]
pub struct NotionClient {
    client: Client,
    token: String,
    root_page_id: String,
    ids: Option<NotionIds>,
}

impl NotionClient {
    pub fn new(token: String, root_page_id: String) -> Self {
        Self {
            client: Client::new(),
            token,
            root_page_id,
            ids: None,
        }
    }

    fn headers(&self) -> reqwest::header::HeaderMap {
        let mut h = reqwest::header::HeaderMap::new();
        h.insert(
            "Authorization",
            format!("Bearer {}", self.token).parse().unwrap(),
        );
        h.insert("Notion-Version", NOTION_VERSION.parse().unwrap());
        h.insert("Content-Type", "application/json".parse().unwrap());
        h
    }

    pub async fn bootstrap(&mut self) -> anyhow::Result<NotionIds> {
        let existing = self.list_children(&self.root_page_id.clone()).await?;
        let daily_db = match find_by_title(&existing, "Daily Reports") {
            Some(id) => id,
            None => self.create_daily_reports_db().await?,
        };
        let param_db = match find_by_title(&existing, "Param Changes") {
            Some(id) => id,
            None => self.create_param_changes_db().await?,
        };
        let events_db = match find_by_title(&existing, "System Events") {
            Some(id) => id,
            None => self.create_system_events_db().await?,
        };
        let evals_db = match find_by_title(&existing, "Signal Evals") {
            Some(id) => id,
            None => self.create_signal_evals_db().await?,
        };
        let conn_page = match find_by_title(&existing, "Connection Tests") {
            Some(id) => id,
            None => self.create_child_page("Connection Tests").await?,
        };
        let ids = NotionIds {
            daily_reports_db: daily_db,
            param_changes_db: param_db,
            system_events_db: events_db,
            signal_evals_db: evals_db,
            connection_tests_page: conn_page,
        };
        self.ids = Some(ids.clone());
        Ok(ids)
    }

    #[allow(dead_code)]
    pub fn ids(&self) -> Option<&NotionIds> {
        self.ids.as_ref()
    }

    async fn list_children(&self, page_id: &str) -> anyhow::Result<Vec<(String, String, String)>> {
        let url = format!(
            "https://api.notion.com/v1/blocks/{}/children?page_size=100",
            page_id
        );
        let resp: serde_json::Value = self
            .client
            .get(&url)
            .headers(self.headers())
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        let mut items = Vec::new();
        if let Some(results) = resp["results"].as_array() {
            for block in results {
                let id = block["id"].as_str().unwrap_or_default().to_string();
                let btype = block["type"].as_str().unwrap_or_default().to_string();
                let title = if btype == "child_database" {
                    extract_title_from_block(block, "child_database")
                } else if btype == "child_page" {
                    extract_title_from_block(block, "child_page")
                } else {
                    String::new()
                };
                if !title.is_empty() {
                    items.push((id, btype, title));
                }
            }
        }
        Ok(items)
    }

    async fn create_daily_reports_db(&self) -> anyhow::Result<String> {
        let body = serde_json::json!({
            "parent": { "type": "page_id", "page_id": self.root_page_id },
            "title": [{ "text": { "content": "Daily Reports" } }],
            "is_inline": true,
            "properties": {
                "Date": { "title": {} },
                "Market": { "select": { "options": [{ "name": "KR", "color": "blue" }, { "name": "US", "color": "red" }] }},
                "Mode": { "select": { "options": [{ "name": "VTS", "color": "yellow" }, { "name": "LIVE", "color": "red" }] }},
                "Total Trades": { "number": { "format": "number" } }, "PnL": { "number": { "format": "number" } },
                "Profit Factor": { "number": { "format": "number" } }, "Avg Score": { "number": { "format": "number" } },
                "LLM Enter": { "number": { "format": "number" } }, "LLM Block": { "number": { "format": "number" } },
                "Score 80+": { "number": { "format": "number" } }, "Score 70-79": { "number": { "format": "number" } }, "Score 60-69": { "number": { "format": "number" } },
                "AI Analysis": { "rich_text": {} }, "Suggestions": { "rich_text": {} },
            }
        });
        self.create_database(body).await
    }

    async fn create_param_changes_db(&self) -> anyhow::Result<String> {
        let body = serde_json::json!({
            "parent": { "type": "page_id", "page_id": self.root_page_id },
            "title": [{ "text": { "content": "Param Changes" } }],
            "is_inline": true,
            "properties": {
                "Timestamp": { "title": {} },
                "Market": { "select": { "options": [{ "name": "KR", "color": "blue" }, { "name": "US", "color": "red" }, { "name": "Global", "color": "gray" }] }},
                "Mode": { "select": { "options": [{ "name": "VTS", "color": "yellow" }, { "name": "LIVE", "color": "red" }] }},
                "Parameter": { "rich_text": {} }, "Old Value": { "rich_text": {} }, "New Value": { "rich_text": {} },
                "Reason": { "rich_text": {} }, "Source": { "select": { "options": [{ "name": "stats_rule", "color": "green" }, { "name": "llm", "color": "purple" }, { "name": "manual", "color": "yellow" }] }},
                "Applied": { "checkbox": {} },
            }
        });
        self.create_database(body).await
    }

    async fn create_system_events_db(&self) -> anyhow::Result<String> {
        let body = serde_json::json!({
            "parent": { "type": "page_id", "page_id": self.root_page_id },
            "title": [{ "text": { "content": "System Events" } }],
            "is_inline": true,
            "properties": {
                "Timestamp": { "title": {} },
                "Event": { "select": { "options": [
                    { "name": "startup", "color": "green" }, { "name": "shutdown", "color": "gray" },
                    { "name": "circuit_breaker_open", "color": "red" }, { "name": "circuit_breaker_closed", "color": "green" },
                    { "name": "recovery_pass", "color": "green" }, { "name": "recovery_fail", "color": "red" },
                    { "name": "hard_blocked", "color": "red" }, { "name": "connection_test", "color": "blue" },
                    { "name": "param_auto_adjust", "color": "purple" }
                ]}},
                "Market": { "select": { "options": [{ "name": "KR", "color": "blue" }, { "name": "US", "color": "red" }, { "name": "Global", "color": "gray" }] }},
                "Mode": { "select": { "options": [{ "name": "VTS", "color": "yellow" }, { "name": "LIVE", "color": "red" }] }},
                "Detail": { "rich_text": {} },
            }
        });
        self.create_database(body).await
    }

    async fn create_signal_evals_db(&self) -> anyhow::Result<String> {
        let body = serde_json::json!({
            "parent": { "type": "page_id", "page_id": self.root_page_id },
            "title": [{ "text": { "content": "Signal Evals" } }],
            "is_inline": true,
            "properties": {
                "Timestamp": { "title": {} }, "Symbol": { "rich_text": {} }, "Score": { "number": { "format": "number" } },
                "Market": { "select": { "options": [{ "name": "KR", "color": "blue" }, { "name": "US", "color": "red" }] }},
                "Regime": { "select": { "options": [{ "name": "Trending", "color": "green" }, { "name": "Volatile", "color": "orange" }, { "name": "Quiet", "color": "blue" }] }},
                "Action": { "select": { "options": [{ "name": "enter", "color": "green" }, { "name": "skip", "color": "gray" }, { "name": "block", "color": "red" }] }},
                "Strategy": { "rich_text": {} },
            }
        });
        self.create_database(body).await
    }

    async fn create_child_page(&self, title: &str) -> anyhow::Result<String> {
        let body = serde_json::json!({ "parent": { "type": "page_id", "page_id": self.root_page_id }, "properties": { "title": { "title": [{ "text": { "content": title } }] } } });
        let resp: serde_json::Value = self
            .client
            .post("https://api.notion.com/v1/pages")
            .headers(self.headers())
            .json(&body)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        resp["id"]
            .as_str()
            .map(|s| s.to_string())
            .ok_or_else(|| anyhow::anyhow!("failed to create page"))
    }

    async fn create_database(&self, body: serde_json::Value) -> anyhow::Result<String> {
        let resp: serde_json::Value = self
            .client
            .post("https://api.notion.com/v1/databases")
            .headers(self.headers())
            .json(&body)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        resp["id"]
            .as_str()
            .map(|s| s.to_string())
            .ok_or_else(|| anyhow::anyhow!("failed to create database"))
    }

    pub async fn add_daily_report(&self, report: &DailyReportRow) -> anyhow::Result<()> {
        let db_id = self
            .ids
            .as_ref()
            .map(|i| i.daily_reports_db.as_str())
            .ok_or_else(|| anyhow::anyhow!("notion not bootstrapped"))?;
        let body = serde_json::json!({ "parent": { "database_id": db_id }, "properties": {
            "Date": { "title": [{ "text": { "content": &report.date } }] }, "Market": { "select": { "name": &report.market } }, "Mode": { "select": { "name": &report.mode } },
            "Total Trades": { "number": report.total_trades }, "PnL": { "number": report.pnl }, "Profit Factor": { "number": report.profit_factor },
            "Avg Score": { "number": report.avg_score }, "LLM Enter": { "number": report.llm_enter }, "LLM Block": { "number": report.llm_block },
            "Score 80+": { "number": report.score_80_plus }, "Score 70-79": { "number": report.score_70_79 }, "Score 60-69": { "number": report.score_60_69 },
            "AI Analysis": { "rich_text": [{ "text": { "content": truncate(&report.ai_analysis, 2000) } }] }, "Suggestions": { "rich_text": [{ "text": { "content": truncate(&report.suggestions, 2000) } }] },
        }});
        self.client
            .post("https://api.notion.com/v1/pages")
            .headers(self.headers())
            .json(&body)
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }

    pub async fn add_param_change(&self, row: &ParamChangeRow) -> anyhow::Result<()> {
        let db_id = self
            .ids
            .as_ref()
            .map(|i| i.param_changes_db.as_str())
            .ok_or_else(|| anyhow::anyhow!("notion not bootstrapped"))?;
        let body = serde_json::json!({ "parent": { "database_id": db_id }, "properties": {
            "Timestamp": { "title": [{ "text": { "content": &row.timestamp } }] }, "Market": { "select": { "name": &row.market } }, "Mode": { "select": { "name": &row.mode } },
            "Parameter": { "rich_text": [{ "text": { "content": &row.parameter } }] }, "Old Value": { "rich_text": [{ "text": { "content": &row.old_value } }] },
            "New Value": { "rich_text": [{ "text": { "content": &row.new_value } }] }, "Reason": { "rich_text": [{ "text": { "content": truncate(&row.reason, 2000) } }] },
            "Source": { "select": { "name": &row.source } }, "Applied": { "checkbox": row.applied },
        }});
        self.client
            .post("https://api.notion.com/v1/pages")
            .headers(self.headers())
            .json(&body)
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }

    pub async fn add_system_event(&self, row: &SystemEventRow) -> anyhow::Result<()> {
        let db_id = self
            .ids
            .as_ref()
            .map(|i| i.system_events_db.as_str())
            .ok_or_else(|| anyhow::anyhow!("notion not bootstrapped"))?;
        let body = serde_json::json!({ "parent": { "database_id": db_id }, "properties": {
            "Timestamp": { "title": [{ "text": { "content": &row.timestamp } }] }, "Event": { "select": { "name": &row.event } },
            "Market": { "select": { "name": &row.market } }, "Mode": { "select": { "name": &row.mode } },
            "Detail": { "rich_text": [{ "text": { "content": truncate(&row.detail, 2000) } }] },
        }});
        self.client
            .post("https://api.notion.com/v1/pages")
            .headers(self.headers())
            .json(&body)
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }

    pub async fn add_signal_eval(&self, row: &SignalEvalRow) -> anyhow::Result<()> {
        let db_id = self
            .ids
            .as_ref()
            .map(|i| i.signal_evals_db.as_str())
            .ok_or_else(|| anyhow::anyhow!("notion not bootstrapped"))?;
        let body = serde_json::json!({ "parent": { "database_id": db_id }, "properties": {
            "Timestamp": { "title": [{ "text": { "content": &row.timestamp } }] }, "Symbol": { "rich_text": [{ "text": { "content": &row.symbol } }] },
            "Score": { "number": row.score }, "Market": { "select": { "name": &row.market } }, "Regime": { "select": { "name": &row.regime } },
            "Action": { "select": { "name": &row.action } }, "Strategy": { "rich_text": [{ "text": { "content": &row.strategy } }] },
        }});
        self.client
            .post("https://api.notion.com/v1/pages")
            .headers(self.headers())
            .json(&body)
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }

    #[allow(dead_code)]
    pub async fn append_connection_test(&self, content: &str) -> anyhow::Result<()> {
        let page_id = self
            .ids
            .as_ref()
            .map(|i| i.connection_tests_page.as_str())
            .ok_or_else(|| anyhow::anyhow!("notion not bootstrapped"))?;
        let body = serde_json::json!({ "children": [ { "object": "block", "type": "callout", "callout": { "rich_text": [{ "type": "text", "text": { "content": content } }], "icon": { "emoji": "🔧" }, "color": "green_background" } }, { "object": "block", "type": "divider", "divider": {} } ] });
        self.client
            .patch(format!(
                "https://api.notion.com/v1/blocks/{}/children",
                page_id
            ))
            .headers(self.headers())
            .json(&body)
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }
}

#[derive(Debug, Clone, Default)]
pub struct DailyReportRow {
    pub date: String,
    pub market: String,
    pub mode: String,
    pub total_trades: f64,
    pub pnl: f64,
    pub profit_factor: f64,
    pub avg_score: f64,
    pub llm_enter: f64,
    pub llm_block: f64,
    pub score_80_plus: f64,
    pub score_70_79: f64,
    pub score_60_69: f64,
    pub ai_analysis: String,
    pub suggestions: String,
}
#[derive(Debug, Clone)]
pub struct ParamChangeRow {
    pub timestamp: String,
    pub market: String,
    pub mode: String,
    pub parameter: String,
    pub old_value: String,
    pub new_value: String,
    pub reason: String,
    pub source: String,
    pub applied: bool,
}
#[derive(Debug, Clone)]
pub struct SystemEventRow {
    pub timestamp: String,
    pub event: String,
    pub market: String,
    pub mode: String,
    pub detail: String,
}
#[derive(Debug, Clone)]
pub struct SignalEvalRow {
    pub timestamp: String,
    pub symbol: String,
    pub score: i32,
    pub market: String,
    pub regime: String,
    pub action: String,
    pub strategy: String,
}

pub fn spawn_system_event(
    notion: &Option<std::sync::Arc<tokio::sync::RwLock<NotionClient>>>,
    row: SystemEventRow,
) {
    if let Some(nc) = notion.clone() {
        tokio::spawn(async move {
            let client = nc.read().await;
            let _ = client.add_system_event(&row).await;
        });
    }
}

fn extract_title_from_block(block: &serde_json::Value, block_type: &str) -> String {
    block[block_type]["title"]
        .as_str()
        .map(|s| s.to_string())
        .unwrap_or_default()
}
fn find_by_title(items: &[(String, String, String)], title: &str) -> Option<String> {
    items
        .iter()
        .find(|(_, _, t)| t == title)
        .map(|(id, _, _)| id.clone())
}
fn truncate(s: &str, max_len: usize) -> &str {
    if s.len() <= max_len {
        s
    } else {
        let mut end = max_len;
        while !s.is_char_boundary(end) && end > 0 {
            end -= 1;
        }
        &s[..end]
    }
}

#[allow(dead_code)]
pub async fn send_notion_report_with_retry<Fut>(
    mut f: impl FnMut() -> Fut,
    token: CancellationToken,
) -> anyhow::Result<()>
where
    Fut: Future<Output = anyhow::Result<()>>,
{
    for attempt in 0..3u32 {
        if token.is_cancelled() {
            return Err(anyhow::anyhow!("cancelled"));
        }
        match f().await {
            Ok(()) => return Ok(()),
            Err(e) => {
                if attempt < 2 {
                    tokio::time::sleep(std::time::Duration::from_secs(1 << attempt)).await;
                } else {
                    tracing::warn!("Notion 3회 실패: {}", e);
                }
            }
        }
    }
    Err(anyhow::anyhow!("Notion 3회 모두 실패"))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn find_by_title_returns_matching_id() {
        let items = vec![
            (
                "id1".into(),
                "child_database".into(),
                "Daily Reports".into(),
            ),
            ("id2".into(), "child_page".into(), "Connection Tests".into()),
        ];
        assert_eq!(find_by_title(&items, "Daily Reports"), Some("id1".into()));
    }
}
