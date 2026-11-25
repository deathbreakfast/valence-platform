//! Platform deletion-run helpers (extended beyond uf-valence-core DeletionService).

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::time::Duration;
use uuid::Uuid;

use super::step_worker::run_valence_deletion_step_worker;
use crate::{ValenceDeletionStep, ValenceDeletionStepAction, ValenceDeletionStepStatus};
use valence::__internal::CompiledQuery;
use valence::Actor;
use valence::Valence;
use valence::{DateTimePredicate, QueryCore, SortDirection, StringPredicate};
use valence::{Error, Result};

/// Run statuses [`DeletionService::cancel_run`] may stop.
const CANCELLABLE_STATUSES: [&str; 3] = ["queued", "scanning", "processing"];

/// Longest failure message [`DeletionService::deletion_run_failure`] returns, in characters.
const FAILURE_MESSAGE_MAX_CHARS: usize = 200;

/// Steps read per page when a run's steps are listed.
const STEP_PAGE: u32 = 1000;

/// What [`DeletionService::cancel_run`] stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelOutcome {
    /// Steps that had already finished when the run was cancelled. Those changes stay.
    pub completed_steps: i64,
    /// Steps that had not run yet (neither completed nor failed); they are skipped.
    pub remaining_steps: i64,
}

/// What a group of deletion steps does to its records.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeletionSummaryAction {
    /// The records are deleted (`cascade_delete` steps).
    Delete,
    /// The records stay, and their link to the deleted record is removed (`set_null` and
    /// `remove_edge` steps).
    Unrelate,
}

impl DeletionSummaryAction {
    /// `"delete"` or `"unrelate"`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Delete => "delete",
            Self::Unrelate => "unrelate",
        }
    }

    const fn from_step(action: &ValenceDeletionStepAction) -> Self {
        match action {
            ValenceDeletionStepAction::CascadeDelete => Self::Delete,
            ValenceDeletionStepAction::SetNull | ValenceDeletionStepAction::RemoveEdge => {
                Self::Unrelate
            }
        }
    }
}

/// Step counts for one table and action in a deletion run, from
/// [`DeletionService::deletion_run_summary`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeletionSchemaSummary {
    /// The table whose records the steps touch.
    pub table: String,
    /// What the steps do to those records.
    pub action: DeletionSummaryAction,
    /// Steps in the group.
    pub total: u64,
    /// Steps that finished.
    pub completed: u64,
    /// Steps that failed.
    pub failed: u64,
}

/// Why a deletion run failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeletionFailureKind {
    /// A `Restrict` connection still has records pointing at the record, so nothing ran.
    RestrictViolation,
    /// Storage refused or failed a step.
    BackendError,
    /// The requester's Delete privacy policy denied a step.
    PrivacyDeny,
}

/// The failure behind a failed run, from [`DeletionService::deletion_run_failure`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeletionRunFailure {
    /// The table of the failing step, or the blocking table for a Restrict failure.
    pub table: String,
    /// What kind of failure it was.
    pub failure_kind: DeletionFailureKind,
    /// What went wrong, with record values and ids removed and at most 200 characters.
    pub message: String,
}

/// A `RestrictViolation` read back from the run's stored `error_message`.
struct StoredRestrictViolation {
    table: String,
    field: String,
    count: String,
    connection: String,
}

/// Value of `key: "..."` (or `key: 123`) inside one `RestrictViolation { .. }` Debug block.
fn debug_field(block: &str, key: &str) -> Option<String> {
    let rest = &block[block.find(&format!("{key}: "))? + key.len() + 2..];
    if let Some(quoted) = rest.strip_prefix('"') {
        return quoted.find('"').map(|end| quoted[..end].to_string());
    }
    let end = rest.find([',', ' ', '}']).unwrap_or(rest.len());
    Some(rest[..end].to_string())
}

/// Parse the orchestrator's `format!("{:?}", dag.restrict_violations)` message.
fn parse_restrict_violations(message: &str) -> Vec<StoredRestrictViolation> {
    message
        .split("RestrictViolation {")
        .skip(1)
        .filter_map(|block| {
            Some(StoredRestrictViolation {
                table: debug_field(block, "blocking_table")?,
                field: debug_field(block, "blocking_field")?,
                count: debug_field(block, "blocking_record_count")?,
                connection: debug_field(block, "connection_name")?,
            })
        })
        .collect()
}

/// Cut `message` to at most [`FAILURE_MESSAGE_MAX_CHARS`] characters.
fn cap_chars(message: &str) -> String {
    message.chars().take(FAILURE_MESSAGE_MAX_CHARS).collect()
}

/// Byte index of the next opening quote in `text`. An apostrophe right after a letter or
/// digit (`can't`) is not one.
fn next_open_quote(text: &str) -> Option<(usize, char)> {
    let mut prev: Option<char> = None;
    for (i, c) in text.char_indices() {
        let opens = match c {
            '"' | '`' => true,
            '\'' => !prev.is_some_and(char::is_alphanumeric),
            _ => false,
        };
        if opens {
            return Some((i, c));
        }
        prev = Some(c);
    }
    None
}

/// Remove record values from a stored step error: every quoted literal (`'..'`, `".."`,
/// `` `..` ``) becomes `…`, and so does the step's record id.
fn scrub_failure_message(message: &str, record_id: &str) -> String {
    let mut out = String::with_capacity(message.len());
    let mut rest = message;
    while let Some((open, quote)) = next_open_quote(rest) {
        let after = &rest[open + quote.len_utf8()..];
        let Some(close) = after.find(quote) else {
            break;
        };
        out.push_str(&rest[..open]);
        out.push('…');
        rest = &after[close + quote.len_utf8()..];
    }
    out.push_str(rest);
    if !record_id.is_empty() {
        out = out.replace(record_id, "…");
    }
    cap_chars(&out)
}

fn run_counter(run: &Value, key: &str) -> i64 {
    run.get(key)
        .and_then(|v| v.as_i64().or_else(|| v.as_f64().map(|f| f as i64)))
        .unwrap_or(0)
}

fn run_status(run: &Value) -> &str {
    run.get("status").and_then(Value::as_str).unwrap_or("")
}

/// Plan order of a step: deepest first, then remove_edge, set_null, cascade_delete.
fn step_plan_order(step: &ValenceDeletionStep) -> (std::cmp::Reverse<i64>, u8) {
    let wave = match step.action() {
        ValenceDeletionStepAction::RemoveEdge => 0,
        ValenceDeletionStepAction::SetNull => 1,
        ValenceDeletionStepAction::CascadeDelete => 2,
    };
    (std::cmp::Reverse(*step.depth()), wave)
}

fn bare_step_id(step: &ValenceDeletionStep) -> Result<String> {
    step.id()
        .map(|rid| rid.id().to_string())
        .ok_or_else(|| Error::Internal("deletion step row has no id".to_string()))
}

fn system_valence(v: &Valence) -> Valence {
    v.with_actor(Actor::System {
        operation: "valence_deletion_run".to_string(),
    })
}

/// Parse `valence_deletion_run.requested_by` (stringified actor JSON or object) into [`Actor`].
///
/// # Errors
///
/// Returns [`Error::Internal`] when the field is missing, empty, or not valid actor JSON.
pub fn parse_requested_by_actor(requested_by: &Value) -> Result<Actor> {
    let actor: Actor = match requested_by {
        Value::String(s) => serde_json::from_str(s).map_err(|e| {
            Error::Internal(format!(
                "invalid deletion run requested_by JSON string: {e}"
            ))
        })?,
        other => serde_json::from_value(other.clone()).map_err(|e| {
            Error::Internal(format!("invalid deletion run requested_by value: {e}"))
        })?,
    };
    Ok(actor)
}

/// CRUD helpers for the `valence_deletion_run` platform table.
pub struct DeletionService;
impl DeletionService {
    /// Rebuild a [`Valence`] handle as the deleting actor stored on the run (`requested_by`).
    ///
    /// Chronon/Boson jobs may boot as System; call this before privacy, DAG work, or physical
    /// delete so those paths run as the requester — not System.
    ///
    /// # Errors
    ///
    /// Missing run, missing/`null` `requested_by`, or unparseable actor JSON.
    pub async fn requester_valence_from_run(run_id: &str, boot: &Valence) -> Result<Valence> {
        let run = Self::get_run_json(run_id, boot)
            .await?
            .ok_or_else(|| Error::NotFound(format!("deletion run {run_id} not found")))?;
        let requested_by = run.get("requested_by").ok_or_else(|| {
            Error::Internal(format!(
                "deletion run {run_id} missing requested_by; refusing System fallback"
            ))
        })?;
        if requested_by.is_null() {
            return Err(Error::Internal(format!(
                "deletion run {run_id} has null requested_by; refusing System fallback"
            )));
        }
        let actor = parse_requested_by_actor(requested_by)?;
        Ok(boot.with_actor(actor))
    }

    /// Create a new deletion run row and return its id.
    ///
    /// # Errors
    ///
    /// Backend create failures for `valence_deletion_run`.
    pub async fn create_run(
        root_table: &str,
        root_record_id: &str,
        actor_json: Value,
        v: &Valence,
    ) -> Result<String> {
        let run_id = Uuid::new_v4().to_string();
        let requested_by = actor_json.to_string();
        let sys = system_valence(v);
        let backend = sys.backend_for_table("valence_deletion_run")?;
        let row = json!({
            "id": run_id,
            "root_table": root_table,
            "root_record_id": root_record_id,
            "status": "queued",
            "total_steps": 0,
            "completed_steps": 0,
            "failed_steps": 0,
            "requested_by": requested_by,
            "requested_at": Utc::now(),
        });
        backend
            .create_record("valence_deletion_run", row)
            .await
            .map_err(|e| Error::database(e.to_string()))?;
        Ok(run_id)
    }

    /// Fetch a run's raw JSON by id, or `None` if it does not exist.
    pub async fn get_run_json(run_id: &str, v: &Valence) -> Result<Option<Value>> {
        let sys = system_valence(v);
        QueryCore::get_record_json(
            "valence_deletion_run",
            run_id,
            &sys,
            valence::use_!(r"In **Valence platform iter and deletion**, we **load a deletion run's stored JSON** so callers can read that run's status and counters. Operators and services using this API see the result; it is not an end-user page."),
        )
        .await
        .map_err(|e| Error::database(e.to_string()))
    }

    /// Merge `patch` into the run row.
    pub async fn merge_run(run_id: &str, patch: Value, v: &Valence) -> Result<()> {
        let sys = system_valence(v);
        let backend = sys.backend_for_table("valence_deletion_run")?;
        backend
            .merge_record("valence_deletion_run", run_id, patch)
            .await
            .map_err(|e| Error::database(e.to_string()))
            .map(|_| ())
    }

    /// Runs for a specific root entity (most recent first).
    pub async fn list_runs_for_record(
        root_table: &str,
        root_record_id: &str,
        v: &Valence,
    ) -> Result<Vec<Value>> {
        let sys = system_valence(v);
        QueryCore::new("valence_deletion_run".to_string())
            .where_string(
                "root_table".to_string(),
                StringPredicate::Equals(root_table.to_string()),
            )
            .where_string(
                "root_record_id".to_string(),
                StringPredicate::Equals(root_record_id.to_string()),
            )
            .order_by("requested_at".to_string(), SortDirection::Desc)
            .limit(50)
            .execute(&sys, valence::use_!(r"In **Valence platform iter and deletion**, we **list deletion runs for one record** so an operator checking a specific entity's deletion history can see every run tied to it, most recent first. This backs admin tooling; it does not appear on any end-user page."))
            .await
            .map_err(|e| Error::database(e.to_string()))
    }

    /// Recent runs for a logical schema (matches `root_table`).
    pub async fn list_runs_for_schema(schema_table: &str, v: &Valence) -> Result<Vec<Value>> {
        let sys = system_valence(v);
        QueryCore::new("valence_deletion_run".to_string())
            .where_string(
                "root_table".to_string(),
                StringPredicate::Equals(schema_table.to_string()),
            )
            .order_by("requested_at".to_string(), SortDirection::Desc)
            .limit(50)
            .execute(&sys, valence::use_!(r"In **Valence platform iter and deletion**, we **list recent deletion runs for a schema** so an operator can review how that schema's deletion runs have been progressing. This backs admin tooling; it does not appear on any end-user page."))
            .await
            .map_err(|e| Error::database(e.to_string()))
    }

    /// Count in-flight runs requested by the given actor JSON (`requested_by` field).
    ///
    /// Delegates to Valence's own count, which compiles the query for the engine that
    /// stores the runs.
    pub async fn count_active_runs_for_requester(requested_by: &str, v: &Valence) -> Result<u64> {
        valence::deletion::DeletionService::count_active_runs_for_requester(requested_by, v).await
    }

    /// Paged listing for admin UI (newest first).
    pub async fn list_runs_recent(limit: u32, v: &Valence) -> Result<Vec<Value>> {
        let sys = system_valence(v);
        QueryCore::new("valence_deletion_run".to_string())
            .order_by("requested_at".to_string(), SortDirection::Desc)
            .limit(limit)
            .execute(&sys, valence::use_!(r"In **Valence platform iter and deletion**, we **list the most recent deletion runs** so the admin console can show operators what has run and each run's status. Only operators with console access see this list."))
            .await
            .map_err(|e| Error::database(e.to_string()))
    }

    /// `status = queued` and `requested_at` strictly before `before` (oldest first) — for reconciling
    /// runs where the manual Chronon `run_now` did not start the orchestrator.
    pub async fn list_queued_runs_requested_before(
        before: DateTime<Utc>,
        limit: u32,
        v: &Valence,
    ) -> Result<Vec<Value>> {
        let sys = system_valence(v);
        QueryCore::new("valence_deletion_run".to_string())
            .where_string(
                "status".to_string(),
                StringPredicate::Equals("queued".to_string()),
            )
            .where_datetime(
                "requested_at".to_string(),
                DateTimePredicate::Before(before),
            )
            .order_by("requested_at".to_string(), SortDirection::Asc)
            .limit(limit)
            .execute(&sys, valence::use_!(r"In **Valence platform iter and deletion**, we **find queued runs older than a cutoff** so the reconciler can restart any run whose Chronon run_now never actually started the orchestrator. This is an internal system check, not shown to operators or end users."))
            .await
            .map_err(|e| Error::database(e.to_string()))
    }

    /// Only-one-wins transition `queued` → `scanning` (sets `started_at`) so two Chronon
    /// `run_now` invocations for the same run cannot each insert `valence_deletion_step` rows.
    /// Returns `true` if this invocation claimed the run; `false` if another instance already
    /// advanced the row or the run is not `queued`.
    pub async fn try_claim_queued_to_scanning(run_id: &str, v: &Valence) -> Result<bool> {
        let sys = system_valence(v);
        let backend = sys.backend_for_table("valence_deletion_run")?;
        if backend.engine_id() == valence::KnownEngines::SURREALDB {
            let q = concat!(
                "UPDATE type::record('valence_deletion_run', $rid) SET ",
                "status = 'scanning',",
                " started_at = time::now()",
                " WHERE status = 'queued' RETURN AFTER"
            );
            let compiled =
                CompiledQuery::new(q.to_string(), vec![("rid".to_string(), json!(run_id))]);
            let rows = backend
                .execute_compiled_query(&compiled)
                .await
                .map_err(|e| Error::database(e.to_string()))?;
            return Ok(!rows.is_empty());
        }

        // SQL / mem: read-then-merge (single-writer tests and sqlite hosts).
        let Some(j) = Self::get_run_json(run_id, &sys).await? else {
            return Ok(false);
        };
        if j.get("status").and_then(|s| s.as_str()) != Some("queued") {
            return Ok(false);
        }
        Self::merge_run(
            run_id,
            json!({
                "status": "scanning",
                "started_at": chrono::Utc::now(),
            }),
            &sys,
        )
        .await?;
        Ok(true)
    }

    /// Most recent deletion run id for `root_table` + `root_record_id` (bare id, no `thing:` prefix).
    pub async fn latest_run_id_for_record(
        root_table: &str,
        root_record_id: &str,
        v: &Valence,
    ) -> Result<Option<String>> {
        let runs = Self::list_runs_for_record(root_table, root_record_id, v).await?;
        let Some(row) = runs.first() else {
            return Ok(None);
        };
        let id = row.get("id").and_then(|x| x.as_str()).map(|s| {
            s.strip_prefix("valence_deletion_run:")
                .unwrap_or(s)
                .to_string()
        });
        Ok(id)
    }

    /// Poll until `run_id` reaches a terminal status or `deadline` passes.
    ///
    /// Returns `Ok(())` for `completed` or `cancelled`. Returns `Err` for `failed` or timeout.
    /// If the run row disappears, returns `Ok(())` (treated as settled).
    pub async fn wait_for_run_terminal(
        run_id: &str,
        deadline: std::time::Instant,
        v: &Valence,
    ) -> Result<()> {
        loop {
            if std::time::Instant::now() >= deadline {
                return Err(Error::Internal(format!(
                    "valence deletion run {run_id} did not reach a terminal status before deadline"
                )));
            }
            let st = Self::get_run_json(run_id, v).await?;
            let Some(doc) = st else {
                return Ok(());
            };
            let status = doc.get("status").and_then(|s| s.as_str()).unwrap_or("");
            match status {
                "completed" => return Ok(()),
                "failed" => {
                    return Err(Error::database(format!(
                        "valence deletion run {run_id} failed"
                    )));
                }
                "cancelled" => return Ok(()),
                _ => tokio::time::sleep(Duration::from_millis(350)).await,
            }
        }
    }

    /// Cancel a run that hasn't finished, recording who stopped it, when and why.
    ///
    /// Sets `status = cancelled`, `cancelled_by` (`by` as an actor JSON string, the same shape
    /// as `requested_by`), `cancelled_at` and `cancel_reason`. Steps that already ran stay
    /// done; the orchestrator and step workers skip the rest. This doesn't check
    /// permissions: the caller checks the actor may cancel before calling.
    ///
    /// # Errors
    ///
    /// [`Error::NotFound`] for an unknown run, [`Error::Validation`] when the run has already
    /// finished (only `queued`, `scanning` and `processing` runs can be cancelled), and
    /// backend errors.
    pub async fn cancel_run(
        run_id: &str,
        by: Value,
        reason: Option<String>,
        v: &Valence,
    ) -> Result<CancelOutcome> {
        let sys = system_valence(v);
        let run = Self::get_run_json(run_id, &sys)
            .await?
            .ok_or_else(|| Error::NotFound(format!("deletion run {run_id} not found")))?;
        let status = run_status(&run);
        if !CANCELLABLE_STATUSES.contains(&status) {
            return Err(Error::Validation(format!(
                "deletion run {run_id} is {status}; only queued, scanning or processing runs can be cancelled"
            )));
        }
        let completed_steps = run_counter(&run, "completed_steps");
        let failed_steps = run_counter(&run, "failed_steps");
        let total_steps = run_counter(&run, "total_steps");
        Self::merge_run(
            run_id,
            json!({
                "status": "cancelled",
                "cancelled_by": by.to_string(),
                "cancelled_at": Utc::now(),
                "cancel_reason": reason,
            }),
            &sys,
        )
        .await?;
        Ok(CancelOutcome {
            completed_steps,
            remaining_steps: (total_steps - completed_steps - failed_steps).max(0),
        })
    }

    /// Every `valence_deletion_step` row of `run_id`, optionally only those in `status`.
    async fn list_run_steps(
        run_id: &str,
        status: Option<ValenceDeletionStepStatus>,
        sys: &Valence,
    ) -> Result<Vec<ValenceDeletionStep>> {
        let mut steps = Vec::new();
        let mut offset = 0u32;
        loop {
            let mut query = ValenceDeletionStep::query(
                sys,
                valence::use_!(
                    r"In **Valence platform iter and deletion**, we **list the steps of one deletion run** so operators can see per-table progress, find the failing step, or retry failed steps. This backs admin tooling; it does not appear on any end-user page."
                ),
            );
            query.inner = query.inner.where_string(
                "run_id".to_string(),
                StringPredicate::Equals(run_id.to_string()),
            );
            if let Some(status) = &status {
                query.inner = query.inner.where_string(
                    "status".to_string(),
                    StringPredicate::Equals(status.as_str().to_string()),
                );
            }
            query.inner = query
                .inner
                .order_by("id".to_string(), SortDirection::Asc)
                .limit(STEP_PAGE)
                .offset(offset);
            let page = query.await?;
            let full = page.len() == STEP_PAGE as usize;
            steps.extend(page);
            if !full {
                return Ok(steps);
            }
            offset = offset.saturating_add(STEP_PAGE);
            if offset >= valence::MAX_QUERY_OFFSET {
                return Err(Error::Internal(format!(
                    "deletion run {run_id} has more steps than one listing can page through"
                )));
            }
        }
    }

    /// Step counts of a run grouped by table and action, in plan order (deepest steps first,
    /// then unlinking before deleting, as the orchestrator runs them).
    ///
    /// A run that hasn't been planned yet (`queued`, `scanning`) has no steps and returns an
    /// empty list.
    ///
    /// # Errors
    ///
    /// Backend errors while listing `valence_deletion_step`.
    pub async fn deletion_run_summary(
        run_id: &str,
        v: &Valence,
    ) -> Result<Vec<DeletionSchemaSummary>> {
        let sys = system_valence(v);
        let mut steps = Self::list_run_steps(run_id, None, &sys).await?;
        steps.sort_by_key(step_plan_order);
        let mut summary: Vec<DeletionSchemaSummary> = Vec::new();
        for step in &steps {
            let action = DeletionSummaryAction::from_step(step.action());
            let table = step.record_table();
            let idx = match summary
                .iter()
                .position(|g| g.table == *table && g.action == action)
            {
                Some(idx) => idx,
                None => {
                    summary.push(DeletionSchemaSummary {
                        table: table.clone(),
                        action,
                        total: 0,
                        completed: 0,
                        failed: 0,
                    });
                    summary.len() - 1
                }
            };
            let group = &mut summary[idx];
            group.total += 1;
            match step.status() {
                ValenceDeletionStepStatus::Completed => group.completed += 1,
                ValenceDeletionStepStatus::Failed => group.failed += 1,
                _ => {}
            }
        }
        Ok(summary)
    }

    /// Why a failed run failed: the failing table, a [`DeletionFailureKind`] and a message
    /// with record values and ids removed (at most 200 characters).
    ///
    /// A Restrict violation fails the run before any step exists, so it is read from the
    /// run row; otherwise the first failed step in plan order decides. Returns `None` for a
    /// run that isn't `failed`, or one that failed with no step or Restrict detail stored.
    ///
    /// # Errors
    ///
    /// [`Error::NotFound`] for an unknown run, and backend errors.
    pub async fn deletion_run_failure(
        run_id: &str,
        v: &Valence,
    ) -> Result<Option<DeletionRunFailure>> {
        let sys = system_valence(v);
        let run = Self::get_run_json(run_id, &sys)
            .await?
            .ok_or_else(|| Error::NotFound(format!("deletion run {run_id} not found")))?;
        if run_status(&run) != "failed" {
            return Ok(None);
        }
        let run_error = run
            .get("error_message")
            .and_then(Value::as_str)
            .unwrap_or("");
        let violations = parse_restrict_violations(run_error);
        if let Some(first) = violations.first() {
            let message = violations
                .iter()
                .map(|r| {
                    format!(
                        "{} record(s) in {}.{} still point here through {}",
                        r.count, r.table, r.field, r.connection
                    )
                })
                .collect::<Vec<_>>()
                .join("; ");
            return Ok(Some(DeletionRunFailure {
                table: first.table.clone(),
                failure_kind: DeletionFailureKind::RestrictViolation,
                message: cap_chars(&message),
            }));
        }
        let mut failed =
            Self::list_run_steps(run_id, Some(ValenceDeletionStepStatus::Failed), &sys).await?;
        failed.sort_by_key(step_plan_order);
        let Some(step) = failed.first() else {
            return Ok(None);
        };
        let raw = step.error_message().map_or("", String::as_str);
        let failure_kind = if raw.starts_with("Privacy policy violation:") {
            DeletionFailureKind::PrivacyDeny
        } else {
            DeletionFailureKind::BackendError
        };
        Ok(Some(DeletionRunFailure {
            table: step.record_table().clone(),
            failure_kind,
            message: scrub_failure_message(raw, step.record_id()),
        }))
    }

    /// Run the failed steps of a failed run again, and return how many were re-queued.
    ///
    /// Each failed step goes back to `queued` (its error cleared), `failed_steps` drops by
    /// that count and the run returns to `processing` with `retried_by` (`by` as an actor
    /// JSON string) and `retried_at`. The steps then run in plan order, in this call,
    /// under the run's requester like the first attempt, and the run ends `completed`, or
    /// `failed` again if any step still fails. A cancel while they run stops the rest.
    ///
    /// This doesn't check permissions: the caller checks the actor may retry before calling.
    ///
    /// # Errors
    ///
    /// [`Error::NotFound`] for an unknown run, [`Error::Validation`] when the run isn't
    /// `failed` or has no failed steps (a Restrict failure never created any), and backend
    /// or step-worker errors.
    pub async fn retry_failed_steps(run_id: &str, by: Value, v: &Valence) -> Result<u64> {
        let sys = system_valence(v);
        let run = Self::get_run_json(run_id, &sys)
            .await?
            .ok_or_else(|| Error::NotFound(format!("deletion run {run_id} not found")))?;
        let status = run_status(&run);
        if status != "failed" {
            return Err(Error::Validation(format!(
                "deletion run {run_id} is {status}; only failed runs can be retried"
            )));
        }
        let mut failed =
            Self::list_run_steps(run_id, Some(ValenceDeletionStepStatus::Failed), &sys).await?;
        if failed.is_empty() {
            return Err(Error::Validation(format!(
                "deletion run {run_id} has no failed steps to retry"
            )));
        }
        failed.sort_by_key(step_plan_order);

        let mut step_ids = Vec::with_capacity(failed.len());
        for step in failed {
            step_ids.push(bare_step_id(&step)?);
            step.get_mutable(
                &sys,
                valence::use_!(r"In **Valence platform iter and deletion**, we **load the deletion step as a mutable builder** so a failed step can be queued again for a retry. The same actors who can run **Valence platform iter and deletion** use the updated values."),
            )
            .set_status(ValenceDeletionStepStatus::Queued)?
            .clear_error_message()
            .clear_started_at()
            .clear_completed_at()
            .commit()
            .await?;
        }
        let requeued = step_ids.len() as u64;
        Self::merge_run(
            run_id,
            json!({
                "status": "processing",
                "failed_steps": (run_counter(&run, "failed_steps") - requeued as i64).max(0),
                "retried_by": by.to_string(),
                "retried_at": Utc::now(),
                "completed_at": Value::Null,
            }),
            &sys,
        )
        .await?;

        for step_id in step_ids {
            run_valence_deletion_step_worker(v.clone(), run_id.to_string(), step_id)
                .await
                .map_err(|e| Error::Internal(format!("deletion step retry failed: {e}")))?;
        }

        let run_after = Self::get_run_json(run_id, &sys)
            .await?
            .ok_or_else(|| Error::NotFound(format!("deletion run {run_id} not found")))?;
        if run_status(&run_after) == "cancelled" {
            return Ok(requeued);
        }
        let terminal = if run_counter(&run_after, "failed_steps") > 0 {
            "failed"
        } else {
            "completed"
        };
        Self::merge_run(
            run_id,
            json!({
                "status": terminal,
                "completed_at": Utc::now(),
            }),
            &sys,
        )
        .await?;
        Ok(requeued)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scrub_keeps_apostrophes_in_words_and_drops_quoted_values() {
        assert_eq!(
            scrub_failure_message("can't delete 'Dana Whitfield' (id u-9)", "u-9"),
            "can't delete … (id …)"
        );
    }

    #[test]
    fn scrub_leaves_an_unclosed_quote_alone() {
        assert_eq!(scrub_failure_message("bad \"value", ""), "bad \"value");
    }
}
