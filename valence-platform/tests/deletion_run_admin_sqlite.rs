//! Admin reads and actions on a deletion run (SQLite): cancel records who, when and why;
//! the summary groups steps by table in plan order; a failed run explains its failure;
//! retrying re-runs the failed steps.

#![allow(missing_docs, clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use serde_json::{json, Value};
use valence::deletion::dag::RestrictViolation;
use valence::evaluator::DEFAULT_IN_MEMORY;
use valence::privacy_policies::common::PUBLIC_READ;
use valence::schema::{SchemaMetadata, SchemaMetadataInit};
use valence::schema_api::{
    Schema, SchemaMeta, SchemaPolicies, SchemaPolicyRule, SchemaPolicyRules, SchemaPrivacy,
};
use valence::{
    register_backend_logical_names, router_key, Actor, DatabaseBackend, DatabaseEvaluator,
    DatabaseRouter, Error, Model, RegisterBackendLogicalNamesOptions, SqliteBackend, Valence,
    SQLITE_ENGINE_ID,
};
use valence_platform::deletion::run_service::{
    CancelOutcome, DeletionFailureKind, DeletionSchemaSummary, DeletionService,
    DeletionSummaryAction,
};
use valence_platform::{ValenceDeletionStep, ValenceDeletionStepAction, ValenceDeletionStepStatus};

const ITEM: &str = "ra_item";

fn schema_meta(name: &'static str) -> &'static SchemaMetadata {
    let public = Some(SchemaPolicyRules {
        allow: vec![SchemaPolicyRule {
            name: "PUBLIC".into(),
            description: None,
            evaluator: Some(&PUBLIC_READ),
        }],
        ..SchemaPolicyRules::default()
    });
    let schema: &'static Schema = Box::leak(Box::new(Schema {
        name: name.to_string(),
        version: "0.1.0".into(),
        databases: vec![DEFAULT_IN_MEMORY.name().to_string()],
        database_evaluator: &DEFAULT_IN_MEMORY,
        privacy: SchemaPrivacy {
            read: "t".into(),
            write: "t".into(),
        },
        policies: Some(SchemaPolicies {
            read: public.clone(),
            delete: public,
            ..SchemaPolicies::default()
        }),
        fields: vec![],
        edges: Vec::new(),
        connections: Vec::new(),
        side_effects: Vec::new(),
        iters: Vec::new(),
        composite_key: Vec::new(),
        retired_fields: Vec::new(),
        partition_export: None,
        traits: Vec::new(),
        ttl: None,
        ownership: None,
        meta: SchemaMeta {
            retention: "1d".into(),
            row_count: 0,
            owner: "t".into(),
            description: None,
            repository: "https://github.com/unified-field-dev/valence-platform".to_string(),
        },
    }));
    Box::leak(Box::new(SchemaMetadata::from_schema(schema)))
}

valence::inventory::submit! {
    SchemaMetadataInit(|| schema_meta(ITEM))
}

async fn sqlite_boot() -> Valence {
    if std::env::var_os("VALENCE_OWNERSHIP_UNIFIED_FETCH").is_none() {
        std::env::set_var("VALENCE_OWNERSHIP_UNIFIED_FETCH", "0");
    }
    if std::env::var_os("VALENCE_OWNERSHIP_COLOCATE").is_none() {
        std::env::set_var("VALENCE_OWNERSHIP_COLOCATE", "0");
    }
    let backend: Arc<dyn DatabaseBackend> =
        Arc::new(SqliteBackend::connect_memory().await.expect("sqlite"));
    let mut router = DatabaseRouter::new();
    register_backend_logical_names(
        &mut router,
        backend,
        &["default"],
        RegisterBackendLogicalNamesOptions::default(),
    );
    Valence::builder()
        .database_router(Arc::new(router))
        .default_backend_key(router_key("default", SQLITE_ENGINE_ID))
        .with_actor(Actor::System {
            operation: "chronon_boot".into(),
        })
        .build()
        .expect("valence")
}

fn actor_json(user: &str) -> Value {
    serde_json::to_value(Actor::User {
        user_id: user.into(),
    })
    .unwrap()
}

/// A step row as the orchestrator would write it.
struct StepFixture<'a> {
    table: &'a str,
    record_id: &'a str,
    action: ValenceDeletionStepAction,
    depth: i64,
    status: ValenceDeletionStepStatus,
    error: Option<&'a str>,
}

async fn seed_step(boot: &Valence, run_id: &str, step: StepFixture<'_>) -> String {
    let set_null_field =
        (step.action == ValenceDeletionStepAction::SetNull).then(|| "parent_id".to_string());
    let edge_table =
        (step.action == ValenceDeletionStepAction::RemoveEdge).then(|| "ra_edge".to_string());
    let row = ValenceDeletionStep::new(
        run_id.to_string(),
        step.table.to_string(),
        step.record_id.to_string(),
        step.action,
        set_null_field,
        edge_table,
        step.status,
        step.depth,
        "fixture".to_string(),
        ITEM.to_string(),
        step.error.map(str::to_string),
        None,
        None,
    )
    .unwrap();
    let step_id = uuid::Uuid::new_v4().to_string();
    ValenceDeletionStep::upsert(
        &step_id,
        row,
        boot,
        valence::use_!(r"**Test:** Fixture deletion step for `deletion_run_admin_sqlite` so the suite can check run summaries, failures and retries. CI and developers running the suite only."),
    )
    .await
    .unwrap();
    step_id
}

async fn run_json(boot: &Valence, run_id: &str) -> Value {
    DeletionService::get_run_json(run_id, boot)
        .await
        .unwrap()
        .expect("run row")
}

#[tokio::test]
async fn cancel_records_who_when_and_why_happy() {
    let boot = sqlite_boot().await;
    let run_id = DeletionService::create_run(ITEM, "c1", actor_json("alice"), &boot)
        .await
        .unwrap();
    DeletionService::merge_run(
        &run_id,
        json!({"status": "processing", "total_steps": 4, "completed_steps": 2, "failed_steps": 1}),
        &boot,
    )
    .await
    .unwrap();

    let outcome = DeletionService::cancel_run(
        &run_id,
        actor_json("admin1"),
        Some("Opened by mistake".into()),
        &boot,
    )
    .await
    .expect("cancel a processing run");
    assert_eq!(
        outcome,
        CancelOutcome {
            completed_steps: 2,
            remaining_steps: 1,
        }
    );

    let run = run_json(&boot, &run_id).await;
    assert_eq!(run["status"], "cancelled");
    assert_eq!(run["cancel_reason"], "Opened by mistake");
    assert_eq!(
        run["cancelled_by"],
        Value::String(actor_json("admin1").to_string())
    );
    assert!(
        run.get("cancelled_at").is_some_and(|v| !v.is_null()),
        "{run}"
    );
}

#[tokio::test]
async fn cancel_refuses_a_finished_run_sad() {
    let boot = sqlite_boot().await;
    let run_id = DeletionService::create_run(ITEM, "c2", actor_json("alice"), &boot)
        .await
        .unwrap();
    DeletionService::merge_run(&run_id, json!({"status": "completed"}), &boot)
        .await
        .unwrap();
    let err = DeletionService::cancel_run(&run_id, actor_json("admin1"), None, &boot)
        .await
        .expect_err("a completed run can't be cancelled");
    assert!(matches!(err, Error::Validation(_)), "{err:?}");
    assert_eq!(run_json(&boot, &run_id).await["status"], "completed");
}

#[tokio::test]
async fn summary_groups_steps_by_table_in_plan_order_happy() {
    let boot = sqlite_boot().await;
    let run_id = DeletionService::create_run(ITEM, "s1", actor_json("alice"), &boot)
        .await
        .unwrap();
    use ValenceDeletionStepAction::{CascadeDelete, RemoveEdge, SetNull};
    use ValenceDeletionStepStatus::{Completed, Failed, Queued};
    for (table, record_id, action, depth, status) in [
        (ITEM, "s1", CascadeDelete, 0, Queued),
        ("ra_child", "k1", CascadeDelete, 1, Completed),
        ("ra_child", "k2", CascadeDelete, 1, Failed),
        ("ra_ref", "r1", SetNull, 1, Completed),
        ("ra_tag", "t1", RemoveEdge, 1, Completed),
    ] {
        seed_step(
            &boot,
            &run_id,
            StepFixture {
                table,
                record_id,
                action,
                depth,
                status,
                error: None,
            },
        )
        .await;
    }

    let summary = DeletionService::deletion_run_summary(&run_id, &boot)
        .await
        .unwrap();
    let group = |table: &str, action, total, completed, failed| DeletionSchemaSummary {
        table: table.to_string(),
        action,
        total,
        completed,
        failed,
    };
    assert_eq!(
        summary,
        vec![
            group("ra_tag", DeletionSummaryAction::Unrelate, 1, 1, 0),
            group("ra_ref", DeletionSummaryAction::Unrelate, 1, 1, 0),
            group("ra_child", DeletionSummaryAction::Delete, 2, 1, 1),
            group(ITEM, DeletionSummaryAction::Delete, 1, 0, 0),
        ]
    );
    assert_eq!(DeletionSummaryAction::Unrelate.as_str(), "unrelate");
}

#[tokio::test]
async fn restrict_failure_names_the_blocking_table_sad() {
    let boot = sqlite_boot().await;
    let run_id = DeletionService::create_run(ITEM, "f1", actor_json("alice"), &boot)
        .await
        .unwrap();
    let violations = vec![RestrictViolation {
        blocking_table: "ra_invoice".into(),
        blocking_field: "customer_id".into(),
        blocking_record_count: 3,
        connection_name: "invoices".into(),
    }];
    // The same message the orchestrator stores for a Restrict failure.
    DeletionService::merge_run(
        &run_id,
        json!({"status": "failed", "error_message": format!("{violations:?}")}),
        &boot,
    )
    .await
    .unwrap();

    let failure = DeletionService::deletion_run_failure(&run_id, &boot)
        .await
        .unwrap()
        .expect("failed run has a failure");
    assert_eq!(failure.failure_kind, DeletionFailureKind::RestrictViolation);
    assert_eq!(failure.table, "ra_invoice");
    assert_eq!(
        failure.message,
        "3 record(s) in ra_invoice.customer_id still point here through invoices"
    );
}

#[tokio::test]
async fn step_failure_is_classified_and_scrubbed_sad() {
    let boot = sqlite_boot().await;
    let run_id = DeletionService::create_run(ITEM, "f2", actor_json("alice"), &boot)
        .await
        .unwrap();
    seed_step(
        &boot,
        &run_id,
        StepFixture {
            table: "ra_child",
            record_id: "kid-77",
            action: ValenceDeletionStepAction::CascadeDelete,
            depth: 1,
            status: ValenceDeletionStepStatus::Failed,
            error: Some(
                "Database error: delete of kid-77 failed: UNIQUE constraint on 'dana@example.com'",
            ),
        },
    )
    .await;
    DeletionService::merge_run(
        &run_id,
        json!({"status": "failed", "failed_steps": 1}),
        &boot,
    )
    .await
    .unwrap();

    let failure = DeletionService::deletion_run_failure(&run_id, &boot)
        .await
        .unwrap()
        .expect("failed run has a failure");
    assert_eq!(failure.failure_kind, DeletionFailureKind::BackendError);
    assert_eq!(failure.table, "ra_child");
    assert_eq!(
        failure.message,
        "Database error: delete of … failed: UNIQUE constraint on …"
    );

    let other = DeletionService::create_run(ITEM, "f3", actor_json("alice"), &boot)
        .await
        .unwrap();
    seed_step(
        &boot,
        &other,
        StepFixture {
            table: ITEM,
            record_id: "f3",
            action: ValenceDeletionStepAction::CascadeDelete,
            depth: 0,
            status: ValenceDeletionStepStatus::Failed,
            error: Some("Privacy policy violation: Access denied by block policy"),
        },
    )
    .await;
    DeletionService::merge_run(
        &other,
        json!({"status": "failed", "failed_steps": 1}),
        &boot,
    )
    .await
    .unwrap();
    let failure = DeletionService::deletion_run_failure(&other, &boot)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(failure.failure_kind, DeletionFailureKind::PrivacyDeny);
}

#[tokio::test]
async fn failure_is_none_for_a_run_that_did_not_fail_edge() {
    let boot = sqlite_boot().await;
    let run_id = DeletionService::create_run(ITEM, "f4", actor_json("alice"), &boot)
        .await
        .unwrap();
    assert!(DeletionService::deletion_run_failure(&run_id, &boot)
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn retry_reruns_failed_steps_and_completes_the_run_happy() {
    let boot = sqlite_boot().await;
    boot.backend_for_table(ITEM)
        .unwrap()
        .create_record(ITEM, json!({"id": {"table": ITEM, "id": "r1"}}))
        .await
        .unwrap();
    let run_id = DeletionService::create_run(ITEM, "r1", actor_json("alice"), &boot)
        .await
        .unwrap();
    let step_id = seed_step(
        &boot,
        &run_id,
        StepFixture {
            table: ITEM,
            record_id: "r1",
            action: ValenceDeletionStepAction::CascadeDelete,
            depth: 0,
            status: ValenceDeletionStepStatus::Failed,
            error: Some("Database error: database is locked"),
        },
    )
    .await;
    DeletionService::merge_run(
        &run_id,
        json!({"status": "failed", "total_steps": 1, "failed_steps": 1}),
        &boot,
    )
    .await
    .unwrap();

    let requeued = DeletionService::retry_failed_steps(&run_id, actor_json("admin1"), &boot)
        .await
        .expect("retry a failed run");
    assert_eq!(requeued, 1);

    let run = run_json(&boot, &run_id).await;
    assert_eq!(run["status"], "completed", "{run}");
    assert_eq!(run["failed_steps"], 0);
    assert_eq!(run["completed_steps"], 1);
    assert_eq!(
        run["retried_by"],
        Value::String(actor_json("admin1").to_string())
    );
    let step = ValenceDeletionStep::get(
        &step_id,
        &boot,
        valence::use_!(r"**Test:** Fixture deletion step reload for `deletion_run_admin_sqlite` so the suite can check a retried step finished. CI and developers running the suite only."),
    )
    .await
    .unwrap()
    .expect("step row");
    assert_eq!(*step.status(), ValenceDeletionStepStatus::Completed);
    assert!(step.error_message().is_none());
    assert!(
        boot.backend_for_table(ITEM)
            .unwrap()
            .get_record(ITEM, "r1")
            .await
            .unwrap()
            .is_none(),
        "the retried step deletes the record"
    );
}

#[tokio::test]
async fn retry_refuses_a_run_that_has_not_failed_sad() {
    let boot = sqlite_boot().await;
    let run_id = DeletionService::create_run(ITEM, "r2", actor_json("alice"), &boot)
        .await
        .unwrap();
    let err = DeletionService::retry_failed_steps(&run_id, actor_json("admin1"), &boot)
        .await
        .expect_err("a queued run can't be retried");
    assert!(matches!(err, Error::Validation(_)), "{err:?}");
}
