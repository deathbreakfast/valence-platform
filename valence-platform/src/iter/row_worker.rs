//! Boson worker: executes **one row** of a [`crate::ValenceIterRun`].
//!
//! Resolves [`valence::find_iter_descriptor`], loads the row via privacy-aware [`valence::QueryCore::get_entity_used`],
//! injects a synthetic `id` into the JSON payload when Surreal omitted it, then runs generated
//! `should_run` / `execute` functions.
//!
//! Updates [`crate::ValenceIterRun`] counters (`processed_rows`, `skipped_rows`, `failed_rows`) and
//! matching [`crate::ValenceIterBatch`] counters; failures create [`crate::ValenceIterRowError`].
//!
//! # Entrypoints
//!
//! - [`run_valence_iter_row_worker`] — full logic; use directly in tests.
//! - `valence_iter_row_worker` — `#[boson::task]` entry (macro may rename the body to `*_impl`).

// `#[boson_macros::task]` expands the task params into a `*Params` struct without doc hooks on the
// struct or its fields; every hand-written item in this module already carries its own doc comment.
#![allow(missing_docs)]

use crate::{
    ValenceIterBatch, ValenceIterBatchStatus, ValenceIterRowError, ValenceIterRowErrorErrorKind,
    ValenceIterRun, ValenceIterRunStatus,
};
use anyhow::{anyhow, Context};
use chrono::Utc;
use std::collections::BTreeMap;

use valence::Model;
use valence::QueryCore;
use valence::Valence;

/// Surreal row payloads often omit the primary key from the field map. Iter glue deserializes into
/// generated models that expect `id` for merges; inject `table:id` when missing.
fn inject_row_id_if_missing(
    table_name: &str,
    row_id: &str,
    mut row: serde_json::Value,
) -> serde_json::Value {
    let synthetic = format!("{table_name}:{row_id}");
    if let serde_json::Value::Object(ref mut map) = row {
        let missing_or_null = matches!(map.get("id"), None | Some(serde_json::Value::Null));
        if missing_or_null {
            map.insert("id".to_string(), serde_json::Value::String(synthetic));
        }
    }
    row
}

async fn load_row_json(
    valence: &Valence,
    table_name: &str,
    row_id: &str,
) -> anyhow::Result<serde_json::Value> {
    match QueryCore::get_entity(
        table_name,
        row_id,
        valence,
        valence::use_!(r"In **Valence platform iter and deletion**, we **load one table row** so the iter row worker can run the registered should_run and execute handlers on that row's fields. The platform uses the payload for that worker only; it is not displayed to end users."),
    )
    .await
    {
        Ok(Some(entity)) => {
            let mut map: BTreeMap<String, serde_json::Value> = entity.data.clone();
            map.insert(
                "id".to_string(),
                serde_json::Value::String(format!("{table_name}:{row_id}")),
            );
            Ok(serde_json::Value::Object(map.into_iter().collect()))
        }
        Ok(None) => Err(anyhow!("row not found")),
        Err(e) => Err(anyhow!("{}", e)),
    }
}

async fn bump_run_field(
    run_id: &str,
    field: &str,
    delta: i64,
    valence: &Valence,
) -> anyhow::Result<()> {
    let run = ValenceIterRun::get(run_id, valence, valence::use_!(r"In **Valence platform iter and deletion**, we **load Valence Iter Run** so the application can decide what to do next in this workflow. The result is used by **Valence platform iter and deletion** logic—not necessarily displayed on a page unless that feature’s UI shows it."))
        .await
        .map_err(|e| anyhow!("{}", e))?
        .context("iter run missing")?;
    let builder = run.get_mutable(
        valence,
        valence::use_!(r"In **Valence platform iter and deletion**, we **load the iter run as a mutable builder** so its row counter can be bumped. The same actors who can run **Valence platform iter and deletion** use the updated values."),
    );
    let builder = match field {
        "processed_rows" => builder.set_processed_rows(*run.processed_rows() + delta),
        "skipped_rows" => builder.set_skipped_rows(*run.skipped_rows() + delta),
        "failed_rows" => builder.set_failed_rows(*run.failed_rows() + delta),
        _ => return Err(anyhow!("unknown run counter field {}", field)),
    }
    .map_err(|e| anyhow!("{}", e))?;
    builder.commit().await.map_err(|e| anyhow!("{}", e))?;
    Ok(())
}

async fn bump_batch_field(
    batch_id: &str,
    field: &str,
    delta: i64,
    valence: &Valence,
) -> anyhow::Result<()> {
    let batch = ValenceIterBatch::get(batch_id, valence, valence::use_!(r"In **Valence platform iter and deletion**, we **load Valence Iter Batch** so the application can decide what to do next in this workflow. The result is used by **Valence platform iter and deletion** logic—not necessarily displayed on a page unless that feature’s UI shows it."))
        .await
        .map_err(|e| anyhow!("{}", e))?
        .context("iter batch missing")?;
    let builder = batch.get_mutable(
        valence,
        valence::use_!(r"In **Valence platform iter and deletion**, we **load the iter batch as a mutable builder** so its row counter can be bumped. The same actors who can run **Valence platform iter and deletion** use the updated values."),
    );
    let builder = match field {
        "processed" => builder.set_processed(*batch.processed() + delta),
        "skipped" => builder.set_skipped(*batch.skipped() + delta),
        "failed" => builder.set_failed(*batch.failed() + delta),
        _ => return Err(anyhow!("unknown batch counter field {}", field)),
    }
    .map_err(|e| anyhow!("{}", e))?;
    builder.commit().await.map_err(|e| anyhow!("{}", e))?;
    Ok(())
}

async fn maybe_complete_batch(batch_id: &str, valence: &Valence) -> anyhow::Result<()> {
    let batch = ValenceIterBatch::get(batch_id, valence, valence::use_!(r"In **Valence platform iter and deletion**, we **load Valence Iter Batch** so the application can decide what to do next in this workflow. The result is used by **Valence platform iter and deletion** logic—not necessarily displayed on a page unless that feature’s UI shows it."))
        .await
        .map_err(|e| anyhow!("{}", e))?
        .context("batch missing")?;
    let done = *batch.processed() + *batch.skipped() + *batch.failed();
    if done >= *batch.row_count() {
        batch
            .get_mutable(
                valence,
                valence::use_!(r"In **Valence platform iter and deletion**, we **load the iter batch as a mutable builder** so it can be marked completed. The same actors who can run **Valence platform iter and deletion** use the updated values."),
            )
            .set_status(ValenceIterBatchStatus::Completed)
            .and_then(|b| b.set_completed_at(Utc::now()))
            .map_err(|e| anyhow!("{}", e))?
            .commit()
            .await
            .map_err(|e| anyhow!("{}", e))?;
    }
    Ok(())
}

/// Longest row error message saved, in characters.
const ROW_ERROR_MESSAGE_MAX_CHARS: usize = 200;

/// Field values shorter than this aren't removed from saved error text; stripping every
/// `1` or `a` would leave the message unreadable.
const MIN_REDACTED_VALUE_CHARS: usize = 2;

/// `<stage>:<kind>` for a row error's `rule`, such as `execute:validation`.
fn error_rule(stage: &str, error: &valence::Error) -> String {
    let kind = match error {
        valence::Error::Database { .. } => "database",
        valence::Error::NotFound(_) => "not_found",
        valence::Error::Validation(_) => "validation",
        valence::Error::Privacy(_) => "privacy",
        valence::Error::Internal(_) => "internal",
        valence::Error::Serialization { .. } => "serialization",
        valence::Error::PendingDeletion(_) => "pending_deletion",
        valence::Error::Identity(_) => "identity",
    };
    format!("{stage}:{kind}")
}

/// Every string and number stored in `value`, as text.
fn collect_stored_values(value: &serde_json::Value, out: &mut Vec<String>) {
    match value {
        serde_json::Value::String(s) => out.push(s.clone()),
        serde_json::Value::Number(n) => out.push(n.to_string()),
        serde_json::Value::Array(items) => {
            for item in items {
                collect_stored_values(item, out);
            }
        }
        serde_json::Value::Object(map) => {
            for item in map.values() {
                collect_stored_values(item, out);
            }
        }
        serde_json::Value::Null | serde_json::Value::Bool(_) => {}
    }
}

/// Whether `word` appears in `text` with no letter, digit or `_` on either side.
fn contains_word(text: &str, word: &str) -> bool {
    let is_word_char = |c: char| c.is_alphanumeric() || c == '_';
    text.match_indices(word).any(|(i, _)| {
        let before = text[..i].chars().next_back();
        let after = text[i + word.len()..].chars().next();
        !before.is_some_and(is_word_char) && !after.is_some_and(is_word_char)
    })
}

/// Make a row error safe to store: every substring equal to one of the row's stored
/// values becomes `…`, and the result is cut to 200 characters. Also returns the first of
/// the row's field names the message mentions, if any.
fn redact_row_error(message: &str, row: Option<&serde_json::Value>) -> (String, Option<String>) {
    let mut redacted = message.to_string();
    let field = row.and_then(|row| {
        let mut values = Vec::new();
        collect_stored_values(row, &mut values);
        values.retain(|v| v.chars().count() >= MIN_REDACTED_VALUE_CHARS);
        // Longest first, so a value that contains a shorter one goes whole.
        values.sort_by_key(|v| std::cmp::Reverse(v.len()));
        values.dedup();
        for value in &values {
            redacted = redacted.replace(value.as_str(), "…");
        }
        row.as_object().and_then(|map| {
            map.keys()
                .filter(|k| k.as_str() != "id")
                .find(|k| contains_word(&redacted, k))
                .cloned()
        })
    });
    let capped = redacted.chars().take(ROW_ERROR_MESSAGE_MAX_CHARS).collect();
    (capped, field)
}

/// One failed row, as [`save_row_failure`] records it.
struct RowFailure<'a> {
    run_id: &'a str,
    batch_id: &'a str,
    row_id: &'a str,
    kind: ValenceIterRowErrorErrorKind,
    rule: String,
    message: &'a str,
    row: Option<&'a serde_json::Value>,
}

/// Save a [`ValenceIterRowError`] with record values removed from its message, then count
/// the row as failed on its run and batch.
async fn save_row_failure(valence: &Valence, failure: RowFailure<'_>) -> anyhow::Result<()> {
    let (message, field) = redact_row_error(failure.message, failure.row);
    let err = ValenceIterRowError::new(
        failure.run_id.to_string(),
        Some(failure.batch_id.to_string()),
        failure.row_id.to_string(),
        message,
        failure.kind,
        field,
        Some(failure.rule),
        Utc::now(),
    )
    .map_err(|e| anyhow!("{}", e))?;
    ValenceIterRowError::create(err, valence, valence::use_!(r"When **Valence platform iter and deletion** needs to persist work, we **save Valence Iter Row Error** so the next step in that feature can continue with the latest values. People and services allowed for **Valence platform iter and deletion** use this data for that workflow—not as a general export of unrelated personal fields."))
        .await
        .map_err(|e| anyhow!("{}", e))?;
    bump_run_field(failure.run_id, "failed_rows", 1, valence).await?;
    bump_batch_field(failure.batch_id, "failed", 1, valence).await?;
    maybe_complete_batch(failure.batch_id, valence).await
}

/// Directly run row-worker logic (same as the Boson task body). Use this in tests; the
/// `#[boson::task]` entrypoint is renamed to `__valence_iter_row_worker_impl` by the macro.
pub async fn run_valence_iter_row_worker(
    valence: Valence,
    run_id: String,
    batch_id: String,
    row_id: String,
    iter_name: String,
    table_name: String,
) -> anyhow::Result<()> {
    let run = match ValenceIterRun::get(&run_id, &valence, valence::use_!(r"In **Valence platform iter and deletion**, we **load Valence Iter Run** so the application can decide what to do next in this workflow. The result is used by **Valence platform iter and deletion** logic—not necessarily displayed on a page unless that feature’s UI shows it."))
        .await
        .map_err(|e| anyhow!("{}", e))?
    {
        Some(r) => r,
        None => return Ok(()),
    };

    if *run.status() == ValenceIterRunStatus::Cancelled {
        return Ok(());
    }

    let desc = inventory::iter::<valence::IterDescriptor>
        .into_iter()
        .find(|d| d.table_name == table_name && d.iter_type_name == iter_name)
        .ok_or_else(|| {
            anyhow!(
                "no IterDescriptor for table={} iter={}",
                table_name,
                iter_name
            )
        })?;

    let row_json = match load_row_json(&valence, &table_name, &row_id).await {
        Ok(j) => inject_row_id_if_missing(&table_name, &row_id, j),
        Err(e) => {
            save_row_failure(
                &valence,
                RowFailure {
                    run_id: &run_id,
                    batch_id: &batch_id,
                    row_id: &row_id,
                    kind: ValenceIterRowErrorErrorKind::ShouldRunError,
                    rule: "load_row".to_string(),
                    message: &e.to_string(),
                    row: None,
                },
            )
            .await?;
            return Ok(());
        }
    };

    let eval = match (desc.should_run)(valence.clone(), row_json.clone()).await {
        Ok(e) => e,
        Err(e) => {
            save_row_failure(
                &valence,
                RowFailure {
                    run_id: &run_id,
                    batch_id: &batch_id,
                    row_id: &row_id,
                    kind: ValenceIterRowErrorErrorKind::ShouldRunError,
                    rule: error_rule("should_run", &e),
                    message: &e.to_string(),
                    row: Some(&row_json),
                },
            )
            .await?;
            return Ok(());
        }
    };

    if !eval.should_run {
        bump_run_field(&run_id, "skipped_rows", 1, &valence).await?;
        bump_batch_field(&batch_id, "skipped", 1, &valence).await?;
        maybe_complete_batch(&batch_id, &valence).await?;
        return Ok(());
    }

    if let Err(e) = (desc.execute)(valence.clone(), row_json.clone()).await {
        save_row_failure(
            &valence,
            RowFailure {
                run_id: &run_id,
                batch_id: &batch_id,
                row_id: &row_id,
                kind: ValenceIterRowErrorErrorKind::ExecuteError,
                rule: error_rule("execute", &e),
                message: &e.to_string(),
                row: Some(&row_json),
            },
        )
        .await?;
        return Ok(());
    }

    bump_run_field(&run_id, "processed_rows", 1, &valence).await?;
    bump_batch_field(&batch_id, "processed", 1, &valence).await?;
    maybe_complete_batch(&batch_id, &valence).await?;
    Ok(())
}

#[boson_macros::task(
    name = "valence_iter_row_worker",
    priority = 50,
    pool = "valence_iter",
    max_in_flight = 1000,
    max_enqueue_per_second = 100,
    max_attempts = 3,
    base_delay_ms = 1000,
    backoff_multiplier = 2.0,
    max_delay_ms = 30_000
)]
/// Boson task entry: forwards to [`run_valence_iter_row_worker`].
pub async fn valence_iter_row_worker(
    ctx: Box<dyn boson_core::ExecutionContext>,
    run_id: String,
    batch_id: String,
    row_id: String,
    iter_name: String,
    table_name: String,
) -> anyhow::Result<()> {
    let valence = boson_valence_identity::valence_from_context(ctx.as_ref())?;
    run_valence_iter_row_worker(valence, run_id, batch_id, row_id, iter_name, table_name).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn worker_error_quoting_the_title_is_saved_without_it() {
        let row = json!({
            "id": "doc:d1",
            "title": "Quarterly payroll for Dana Whitfield",
            "pages": 12,
            "archived": false,
        });
        let error = valence::Error::Validation(
            "title \"Quarterly payroll for Dana Whitfield\" exceeds 20 characters".to_string(),
        );
        let (message, field) = redact_row_error(&error.to_string(), Some(&row));
        assert!(!message.contains("Dana"), "{message}");
        assert_eq!(
            message,
            "Validation error: title \"…\" exceeds 20 characters"
        );
        assert_eq!(field.as_deref(), Some("title"));
        assert_eq!(error_rule("execute", &error), "execute:validation");
    }

    #[test]
    fn numbers_are_removed_and_short_values_kept() {
        let row = json!({"id": "doc:d2", "pages": 1234, "grade": "B"});
        let (message, field) = redact_row_error("pages 1234 over limit, rated B", Some(&row));
        assert_eq!(message, "pages … over limit, rated B");
        assert_eq!(field.as_deref(), Some("pages"));
    }

    #[test]
    fn message_is_capped_at_200_characters() {
        let long = "é".repeat(500);
        let (message, field) = redact_row_error(&long, None);
        assert_eq!(message.chars().count(), ROW_ERROR_MESSAGE_MAX_CHARS);
        assert_eq!(field, None);
    }

    #[test]
    fn field_names_match_whole_words_only() {
        let row = json!({"id": "doc:d3", "page": "p"});
        let (_, field) = redact_row_error("pages are missing", Some(&row));
        assert_eq!(field, None);
    }
}
