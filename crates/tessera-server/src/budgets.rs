// SPDX-License-Identifier: Apache-2.0

//! Enforcing budgets (section 13.4) at the node's doors. A write is
//! counted against the tenant's ingest budget and against the budget
//! of each team its rows belong to; once a budget is reached, the rows
//! beyond it are admitted (alert only, the default), sampled with
//! `sample_rate` set (CO-6, CO-7), or refused. A query is refused
//! before it runs when the tenant's query budget is used up and says
//! to reject, and charged with the bytes it scanned afterwards. The
//! first time a budget is reached in a day is logged and recorded as a
//! `budget` row of `system.usage`, with the action and the limit. The
//! nodes of a cluster share their counts through the root pool
//! ([`share_budgets`]), so a tenant has one budget across them.

use std::collections::BTreeMap;

use arrow::array::{Array, BooleanArray, RecordBatch};
use arrow::compute::filter_record_batch;
use tessera_catalog::TenantSettings;
use tessera_cluster::{Account, Charge, Phase};
use tessera_kernels::sample::sample;
use tessera_types::event::Scalar;
use tessera_types::{Budget, BudgetAction, EventBatch, TenantId};

use crate::metering::{self, Usage};
use crate::{ApiError, AppState};

/// Nanoseconds in a UTC day, the span of a budget.
const NANOS_PER_DAY: i64 = 86_400_000_000_000;

/// `batch` as the tenant's and its teams' ingest budgets admit it: the
/// whole batch, the rows that remain, or nothing to write.
///
/// # Errors
///
/// When every row is refused by a budget whose action is to reject.
pub fn admit_ingest(state: &AppState, batch: EventBatch) -> Result<Option<EventBatch>, ApiError> {
    let Some(settings) = settings(state, batch.tenant()) else {
        return Ok(Some(batch));
    };
    if settings.ingest_budget.is_none() && settings.team_ingest_budgets.is_empty() {
        return Ok(Some(batch));
    }
    let now = state.env.clock.now();
    let rows = batch.rows();
    let bytes = u64::try_from(rows.get_array_memory_size()).unwrap_or(u64::MAX);
    let per_row = bytes / u64::try_from(rows.num_rows()).unwrap_or(1).max(1);
    let tenant = Account {
        tenant: batch.tenant().clone(),
        phase: Phase::Ingest,
        team: None,
    };
    let mut crossings = Vec::new();
    let mut actions: Vec<Option<BudgetAction>> = vec![None; rows.num_rows()];
    let mut refusal = None;
    if let Some(budget) = &settings.ingest_budget {
        let charge = state.budgets.charge(&tenant, budget, bytes, now);
        note(
            &mut crossings,
            &tenant,
            budget,
            &charge,
            batch.dataset().as_str(),
        );
        refusal = refusal.or_else(|| refused(&tenant, budget, &charge));
        actions.fill(charge.action);
    }
    let teams = settings
        .team_attribute
        .as_deref()
        .and_then(|path| metering::column(rows, path));
    if let Some(teams) = &teams {
        for (team, budget) in &settings.team_ingest_budgets {
            let members: Vec<usize> = (0..rows.num_rows())
                .filter(|i| !teams.is_null(*i) && teams.value(*i) == team)
                .collect();
            if members.is_empty() {
                continue;
            }
            let account = Account {
                team: Some(team.clone()),
                ..tenant.clone()
            };
            let share = per_row * u64::try_from(members.len()).unwrap_or(u64::MAX);
            let charge = state.budgets.charge(&account, budget, share, now);
            note(
                &mut crossings,
                &account,
                budget,
                &charge,
                batch.dataset().as_str(),
            );
            refusal = refusal.or_else(|| refused(&account, budget, &charge));
            for i in members {
                actions[i] = stricter(actions[i], charge.action);
            }
        }
    }
    metering::record(state, &crossings);
    apply(&batch, &actions, refusal)
}

/// Refuses a query when the tenant's query budget is used up and its
/// action is to reject.
///
/// # Errors
///
/// As described.
/// Publishes what this node counted today under its own key in the
/// root pool and absorbs what every other node published, one round of
/// the sharing the node repeats on its heartbeat (section 13.4).
///
/// A node's id is new on every start, so the counts a stopped node
/// published stay and keep today's totals for the nodes that remain;
/// counts of an earlier day are removed as they are met, so the prefix
/// holds one object per node that ran today.
///
/// # Errors
///
/// When the root pool cannot be written or listed; a node's counts that
/// cannot be read are skipped.
pub async fn share_budgets(state: &AppState) -> Result<(), String> {
    use futures::TryStreamExt;
    use tessera_cluster::Counts;
    use tessera_object::layout;

    let store = state.pools.root().store();
    let now = state.env.clock.now();
    let own = layout::budget_counts(state.env.node);
    let mine = serde_json::to_vec(&state.budgets.counts(now)).map_err(|e| e.to_string())?;
    store
        .put(&own, mine.into())
        .await
        .map_err(|e| e.to_string())?;
    let listed: Vec<tessera_object::ObjectMeta> = store
        .list(&layout::budgets())
        .try_collect()
        .await
        .map_err(|e| e.to_string())?;
    let today = now.as_nanos().div_euclid(NANOS_PER_DAY);
    let mut others = Vec::new();
    for meta in listed.into_iter().filter(|m| m.key != own) {
        match store.get(&meta.key).await {
            Ok(bytes) => match serde_json::from_slice::<Counts>(&bytes) {
                Ok(counts) if counts.day == today => others.push(counts),
                Ok(_) => {
                    if let Err(error) = store.delete(&meta.key).await {
                        tracing::debug!(key = %meta.key, error = %error, "stale budget counts kept");
                    }
                }
                Err(error) => {
                    tracing::debug!(key = %meta.key, error = %error, "budget counts unreadable");
                }
            },
            Err(error) => tracing::debug!(key = %meta.key, error = %error, "budget counts unread"),
        }
    }
    state.budgets.absorb(&others, now);
    Ok(())
}

pub fn admit_query(
    state: &AppState,
    tenant: &TenantId,
    team: Option<&str>,
) -> Result<(), ApiError> {
    let Some(settings) = settings(state, tenant) else {
        return Ok(());
    };
    let now = state.env.clock.now();
    for (account, budget) in query_accounts(tenant, team, &settings) {
        if budget.action == BudgetAction::Reject && state.budgets.exhausted(&account, &budget, now)
        {
            return Err(ApiError::too_many_requests(format!(
                "query budget of {} bytes per day reached for {}; retry tomorrow",
                budget.bytes_per_day,
                whose(&account)
            )));
        }
    }
    Ok(())
}

/// Counts the bytes a query scanned against the tenant's query budget
/// and the budget of `team`, the team of the role it ran under.
pub fn charge_query(state: &AppState, tenant: &TenantId, team: Option<&str>, bytes: u64) {
    let Some(settings) = settings(state, tenant) else {
        return;
    };
    let now = state.env.clock.now();
    let mut crossings = Vec::new();
    for (account, budget) in query_accounts(tenant, team, &settings) {
        let charge = state.budgets.charge(&account, &budget, bytes, now);
        note(&mut crossings, &account, &budget, &charge, "");
    }
    metering::record(state, &crossings);
}

/// The accounts a query of `team` in `tenant` is charged to: the
/// tenant's when it has a query budget, then the team's when it has
/// one (section 13.4).
fn query_accounts(
    tenant: &TenantId,
    team: Option<&str>,
    settings: &TenantSettings,
) -> Vec<(Account, Budget)> {
    let account = |team: Option<String>| Account {
        tenant: tenant.clone(),
        phase: Phase::Query,
        team,
    };
    let mut accounts = Vec::new();
    if let Some(budget) = settings.query_budget {
        accounts.push((account(None), budget));
    }
    if let Some((team, budget)) = team.and_then(|t| settings.team_query_budgets.get_key_value(t)) {
        accounts.push((account(Some(team.clone())), *budget));
    }
    accounts
}

/// Whose budget an account is: the team's, or the tenant's.
fn whose(account: &Account) -> String {
    match &account.team {
        Some(team) => format!("team {team}"),
        None => "the tenant".to_owned(),
    }
}

fn settings(state: &AppState, tenant: &TenantId) -> Option<TenantSettings> {
    state
        .catalog
        .cached()
        .state()
        .tenant(tenant)
        .map(|t| t.settings.clone())
}

/// The stricter of two actions: reject, then the lower sampling rate,
/// then alert.
fn stricter(a: Option<BudgetAction>, b: Option<BudgetAction>) -> Option<BudgetAction> {
    let rank = |action: &Option<BudgetAction>| match action {
        None => 0,
        Some(BudgetAction::Alert) => 1,
        Some(BudgetAction::Sample { .. }) => 2,
        Some(BudgetAction::Reject) => 3,
    };
    match (a, b) {
        (Some(BudgetAction::Sample { rate: x }), Some(BudgetAction::Sample { rate: y })) => {
            Some(BudgetAction::Sample { rate: x.min(y) })
        }
        _ if rank(&a) >= rank(&b) => a,
        _ => b,
    }
}

/// The error for a request refused by `budget`, if `charge` says so.
fn refused(account: &Account, budget: &Budget, charge: &Charge) -> Option<ApiError> {
    (charge.action == Some(BudgetAction::Reject)).then(|| {
        ApiError::too_many_requests(format!(
            "ingest budget of {} bytes per day reached for {}; retry tomorrow",
            budget.bytes_per_day,
            whose(account)
        ))
    })
}

/// Logs and records the budget being reached, the first time today.
fn note(
    crossings: &mut Vec<Usage>,
    account: &Account,
    budget: &Budget,
    charge: &Charge,
    dataset: &str,
) {
    if !charge.crossed {
        return;
    }
    let phase = match account.phase {
        Phase::Ingest => "ingest",
        Phase::Query => "query",
    };
    let action = match budget.action {
        BudgetAction::Alert => "alert".to_owned(),
        BudgetAction::Sample { rate } => format!("sample {rate}"),
        BudgetAction::Reject => "reject".to_owned(),
    };
    tracing::warn!(
        tenant = %account.tenant,
        team = account.team.as_deref().unwrap_or("-"),
        phase,
        limit = budget.bytes_per_day,
        total = charge.total,
        action,
        "budget reached"
    );
    let mut extra: BTreeMap<&'static str, Scalar> = BTreeMap::new();
    extra.insert("source", Scalar::Text(phase.to_owned()));
    extra.insert("action", Scalar::Text(action));
    extra.insert(
        "limit",
        Scalar::Int(i64::try_from(budget.bytes_per_day).unwrap_or(i64::MAX)),
    );
    crossings.push(Usage {
        phase: "budget",
        tenant: account.tenant.clone(),
        dataset: dataset.to_owned(),
        kind: None,
        service: None,
        team: account.team.clone(),
        bytes: charge.total,
        events: 0,
        extra,
    });
}

/// `batch` with the refused rows removed and the sampled rows sampled.
fn apply(
    batch: &EventBatch,
    actions: &[Option<BudgetAction>],
    refusal: Option<ApiError>,
) -> Result<Option<EventBatch>, ApiError> {
    let keep: BooleanArray = actions
        .iter()
        .map(|a| Some(*a != Some(BudgetAction::Reject)))
        .collect();
    if keep.true_count() == 0 {
        return Err(refusal.unwrap_or_else(|| ApiError::too_many_requests("ingest budget reached")));
    }
    let mut rows: RecordBatch = batch.rows().clone();
    let mut actions: Vec<Option<BudgetAction>> = actions.to_vec();
    if keep.true_count() < actions.len() {
        rows =
            filter_record_batch(&rows, &keep).map_err(|e| ApiError::bad_request(e.to_string()))?;
        actions.retain(|a| *a != Some(BudgetAction::Reject));
    }
    let mut rates: Vec<f64> = actions
        .iter()
        .filter_map(|a| match a {
            Some(BudgetAction::Sample { rate }) => Some(*rate),
            _ => None,
        })
        .collect();
    rates.sort_by(f64::total_cmp);
    rates.dedup();
    for rate in rates {
        let matches: BooleanArray = actions
            .iter()
            .map(|a| Some(*a == Some(BudgetAction::Sample { rate })))
            .collect();
        let sampled = sample(&rows, batch.kind(), &matches, rate)
            .map_err(|e| ApiError::bad_request(e.to_string()))?;
        actions = actions
            .iter()
            .zip(sampled.kept.iter())
            .filter(|(_, kept)| *kept == Some(true))
            .map(|(action, _)| *action)
            .collect();
        rows = sampled.rows;
    }
    if rows.num_rows() == 0 {
        return Ok(None);
    }
    EventBatch::new(
        batch.tenant().clone(),
        batch.dataset().clone(),
        batch.kind(),
        rows,
    )
    .map(Some)
    .map_err(|e| ApiError::bad_request(e.to_string()))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::time::Duration;

    use axum::body::Body;
    use axum::http::{Request, StatusCode, header};
    use http_body_util::BodyExt;
    use tessera_catalog::{TenantName, TenantSettings};
    use tessera_types::{DatasetName, LogRow, Timestamp};
    use tower::ServiceExt;

    use super::*;

    async fn configure(state: &AppState, settings: TenantSettings) {
        state
            .catalog
            .alter_tenant(&TenantName::new("default").unwrap(), settings)
            .await
            .unwrap();
    }

    fn logs(n: u8, team_of: impl Fn(u8) -> &'static str) -> EventBatch {
        let rows: Vec<LogRow> = (0..n)
            .map(|i| LogRow {
                timestamp: Timestamp::from_nanos(1_700_000_000_000_000_000 + i64::from(i)),
                event_id: [i; 16],
                body: Some(format!("line {i}")),
                attributes: BTreeMap::from([(
                    "attr.team".to_owned(),
                    Scalar::Text(team_of(i).to_owned()),
                )]),
                ..LogRow::default()
            })
            .collect();
        EventBatch::from_logs(TenantId::DEFAULT, DatasetName::new("prod").unwrap(), &rows).unwrap()
    }

    async fn sql(state: &AppState, query: &str) -> (StatusCode, serde_json::Value) {
        let body = serde_json::json!({ "dataset": "prod", "query": query });
        let request = Request::builder()
            .method("POST")
            .uri("/api/v1/query")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(body.to_string()))
            .unwrap();
        let response = crate::router(state.clone()).oneshot(request).await.unwrap();
        let status = response.status();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        (status, serde_json::from_slice(&bytes).unwrap())
    }

    /// The rows of `logs`, whole because none of them is sampled.
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    async fn count(state: &AppState) -> u64 {
        let (status, answer) = sql(state, "SELECT count(*) FROM logs").await;
        assert_eq!(status, StatusCode::OK, "{answer}");
        answer["rows"][0][0].as_f64().unwrap().round() as u64
    }

    async fn budget_rows(state: &AppState) -> Vec<serde_json::Value> {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let (_, usage) = sql(
            state,
            "SELECT team, source, action, \"limit\", bytes FROM system.usage \
             WHERE phase = 'budget' ORDER BY team",
        )
        .await;
        usage["rows"].as_array().cloned().unwrap_or_default()
    }

    fn budget(bytes_per_day: u64, action: BudgetAction) -> Budget {
        Budget {
            bytes_per_day,
            action,
        }
    }

    #[tokio::test]
    async fn reaching_a_budget_admits_and_records_it_once_by_default() {
        let state = crate::testing::state().await;
        configure(
            &state,
            TenantSettings {
                ingest_budget: Some(budget(1, BudgetAction::Alert)),
                ..TenantSettings::default()
            },
        )
        .await;
        state.write(logs(3, |_| "a")).await.unwrap();
        state.write(logs(3, |_| "a")).await.unwrap();
        assert_eq!(count(&state).await, 6);
        let rows = budget_rows(&state).await;
        assert_eq!(rows.len(), 1, "{rows:?}");
        assert_eq!(rows[0][1], "ingest");
        assert_eq!(rows[0][2], "alert");
        assert_eq!(rows[0][3], 1.0);
    }

    #[tokio::test]
    async fn a_reject_budget_refuses_writes_once_reached() {
        let state = crate::testing::state().await;
        let first = logs(3, |_| "a");
        let fits = u64::try_from(first.rows().get_array_memory_size()).unwrap();
        configure(
            &state,
            TenantSettings {
                ingest_budget: Some(budget(fits, BudgetAction::Reject)),
                ..TenantSettings::default()
            },
        )
        .await;
        state.write(first).await.unwrap();
        let error = state.write(logs(3, |_| "a")).await.unwrap_err();
        assert_eq!(error.status(), StatusCode::TOO_MANY_REQUESTS);
        assert!(
            error.message().contains("ingest budget"),
            "{}",
            error.message()
        );
        assert_eq!(count(&state).await, 3);
    }

    /// Section 13.4: once the nodes have shared their counts, a budget
    /// reached on one node refuses writes on another.
    #[tokio::test]
    async fn a_budget_reached_on_one_node_holds_on_another() {
        let store = tessera_object::MemoryStore::new();
        let a = AppState::in_memory_on(store.clone()).await;
        let b = AppState::in_memory_on(store).await;
        let first = logs(3, |_| "a");
        let fits = u64::try_from(first.rows().get_array_memory_size()).unwrap();
        configure(
            &a,
            TenantSettings {
                ingest_budget: Some(budget(fits, BudgetAction::Reject)),
                ..TenantSettings::default()
            },
        )
        .await;
        b.catalog.snapshot().await.unwrap();
        a.write(first).await.unwrap();
        assert!(
            b.write(logs(3, |_| "a")).await.is_ok(),
            "b has not heard yet"
        );
        share_budgets(&a).await.unwrap();
        share_budgets(&b).await.unwrap();
        let error = b.write(logs(3, |_| "a")).await.unwrap_err();
        assert_eq!(error.status(), StatusCode::TOO_MANY_REQUESTS);
        share_budgets(&a).await.unwrap();
        let error = a.write(logs(3, |_| "a")).await.unwrap_err();
        assert_eq!(
            error.status(),
            StatusCode::TOO_MANY_REQUESTS,
            "and a counts b's"
        );
    }

    /// The counts a node published for an earlier day go when another
    /// node meets them; today's, from a node that stopped, stay and
    /// count.
    #[tokio::test]
    async fn stale_budget_counts_are_removed_and_todays_kept() {
        use tessera_cluster::{Account, Counts, Phase};
        use tessera_object::{ObjectStore, layout};
        use tessera_types::NodeId;

        let store = tessera_object::MemoryStore::new();
        let state = AppState::in_memory_on(store.clone()).await;
        let now = state.env.clock.now();
        let today = now.as_nanos().div_euclid(NANOS_PER_DAY);
        let account = Account {
            tenant: TenantId::DEFAULT,
            phase: Phase::Ingest,
            team: None,
        };
        let publish = |node: u128, day: i64| {
            let store = store.clone();
            let account = account.clone();
            async move {
                let counts = Counts {
                    day,
                    bytes: vec![(account, 5)],
                };
                store
                    .put(
                        &layout::budget_counts(NodeId::from_u128(node)),
                        serde_json::to_vec(&counts).unwrap().into(),
                    )
                    .await
                    .unwrap();
            }
        };
        publish(1, today - 1).await;
        publish(2, today).await;
        share_budgets(&state).await.unwrap();
        assert!(
            store
                .head(&layout::budget_counts(NodeId::from_u128(1)))
                .await
                .is_err(),
            "yesterday's counts are gone"
        );
        assert!(
            store
                .head(&layout::budget_counts(NodeId::from_u128(2)))
                .await
                .is_ok(),
            "today's stay"
        );
        assert!(
            state.budgets.exhausted(
                &account,
                &Budget {
                    bytes_per_day: 5,
                    action: BudgetAction::Reject,
                },
                now
            ),
            "and count against the budget"
        );
    }

    #[tokio::test]
    async fn a_sample_budget_keeps_a_share_with_the_rate_set() {
        let state = crate::testing::state().await;
        configure(
            &state,
            TenantSettings {
                ingest_budget: Some(budget(1, BudgetAction::Sample { rate: 0.5 })),
                ..TenantSettings::default()
            },
        )
        .await;
        state.write(logs(200, |_| "a")).await.unwrap();
        // Aggregates weigh rows by 1 / sample_rate (QE-13): count(*) stays
        // an estimate of the 200 written, and the sum of the rates is the
        // number of rows actually kept.
        let (status, answer) = sql(
            &state,
            "SELECT count(*), sum(sample_rate), min(sample_rate), max(sample_rate) FROM logs",
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{answer}");
        let row = &answer["rows"][0];
        let estimate = row[0].as_f64().unwrap();
        assert!(estimate > 120.0 && estimate < 280.0, "{answer}");
        let kept = row[1].as_f64().unwrap();
        assert!(kept > 60.0 && kept < 140.0, "{answer}");
        assert_eq!(row[2], 0.5, "{answer}");
        assert_eq!(row[3], 0.5, "{answer}");
    }

    #[tokio::test]
    async fn team_budgets_apply_to_the_teams_rows_alone() {
        let state = crate::testing::state().await;
        configure(
            &state,
            TenantSettings {
                team_attribute: Some("attr.team".into()),
                team_ingest_budgets: BTreeMap::from([(
                    "ads".to_owned(),
                    budget(1, BudgetAction::Reject),
                )]),
                ..TenantSettings::default()
            },
        )
        .await;
        state
            .write(logs(10, |i| if i % 2 == 0 { "ads" } else { "search" }))
            .await
            .unwrap();
        assert_eq!(count(&state).await, 5, "search rows stay");
        let error = state.write(logs(4, |_| "ads")).await.unwrap_err();
        assert_eq!(error.status(), StatusCode::TOO_MANY_REQUESTS);
        assert!(error.message().contains("team ads"), "{}", error.message());
        let rows = budget_rows(&state).await;
        assert_eq!(rows.len(), 1, "{rows:?}");
        assert_eq!(rows[0][0], "ads");
        assert_eq!(rows[0][2], "reject");
    }

    #[tokio::test]
    async fn a_query_budget_refuses_queries_once_used_up() {
        let state = crate::testing::state().await;
        state.write(logs(3, |_| "a")).await.unwrap();
        configure(
            &state,
            TenantSettings {
                query_budget: Some(budget(1, BudgetAction::Reject)),
                ..TenantSettings::default()
            },
        )
        .await;
        assert_eq!(
            count(&state).await,
            3,
            "the query that reaches the budget runs"
        );
        let (status, answer) = sql(&state, "SELECT count(*) FROM logs").await;
        assert_eq!(status, StatusCode::TOO_MANY_REQUESTS, "{answer}");
        assert!(
            answer["error"].as_str().unwrap().contains("query budget"),
            "{answer}"
        );
    }

    #[test]
    fn the_stricter_action_wins_and_sampling_takes_the_lower_rate() {
        let sample = |rate| Some(BudgetAction::Sample { rate });
        assert_eq!(
            stricter(None, Some(BudgetAction::Alert)),
            Some(BudgetAction::Alert)
        );
        assert_eq!(
            stricter(Some(BudgetAction::Alert), Some(BudgetAction::Reject)),
            Some(BudgetAction::Reject)
        );
        assert_eq!(
            stricter(sample(0.5), Some(BudgetAction::Alert)),
            sample(0.5)
        );
        assert_eq!(stricter(sample(0.5), sample(0.1)), sample(0.1));
        assert_eq!(stricter(sample(0.5), None), sample(0.5));
        assert_eq!(
            stricter(Some(BudgetAction::Reject), sample(0.1)),
            Some(BudgetAction::Reject)
        );
    }
}
