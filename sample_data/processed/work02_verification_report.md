# Work 02: Verification and Final Data Integrity Audit Report

**Generated:** 2026-10-30T15:27:57Z
**Project:** Temporal Product Knowledge System
**Repository:** `thabishgafar10/H4H003`
**Git Branch:** `feature/data-ingestion`
**Dataset:** Apache Kafka SQLite (`kafka.sqlite3`)
**Source DB Path:** `C:\Users\User\Desktop\Temporal-PK\sample_data\raw\kafka.sqlite3`
**App DB Path:** `C:\Users\User\Desktop\Temporal-PK\backend\data\temporal_knowledge.db`

---

## 1. Work 02 Architecture Overview

The Temporal Product Knowledge System establishes a reliable, immutable provenance and normalized relational foundation for temporal product event extraction.
- **Framework:** FastAPI (REST endpoints) + SQLAlchemy ORM.
- **Storage:** SQLite 3 with WAL-compatible schema, strict read-only URI connection for raw datasets (`file:...kafka.sqlite3?mode=ro`).
- **Data Layers:**
  1. **Raw Provenance Layer (`source_records`):** Immutable JSON payloads, SHA-256 record hashes, server ingestion timestamps, and natural composite keys.
  2. **Normalized Domain Layer:** Strongly typed relational entities (`issues`, `commits`, `code_changes`, `issue_comments`, `issue_components`, `issue_fix_versions`, `issue_links`, `commit_issue_links`).
  3. **Integrity & Unresolved References Layer (`unresolved_relationships`):** Tracks all broken foreign keys without data loss.
  4. **Batch Auditing Layer (`ingestion_batches`):** Tracks duration, chunked commits, inserted/updated/skipped/failed records, and dataset-level unresolved totals.
  5. **Schema Migration System (`schema_migrations`, `backend/migrations/`):** Repeatable, transactional SQLite table recreation with verified pre-migration backups.

---

## 2. Project Database Schema

The physical SQLite database `temporal_knowledge.db` implements the following relational entities:
- `source_records`: `(id, dataset_name, source_table, source_record_id, source_record_hash, raw_data, source_created_at, source_updated_at, ingested_at, ingestion_batch_id)` with `UNIQUE(dataset_name, source_table, source_record_id)`.
- `issues`: `(id, source_issue_id, issue_type, summary, description, priority, status, resolution, assignee, reporter, created_at, updated_at, resolved_at, source_record_id)`.
- `commits`: `(id, commit_hash, message, author, author_email, committed_at, is_merge, source_record_id)`.
- `issue_comments`: `(id, source_issue_id, username, display_name, message, created_at, source_record_id)`.
- `issue_components`: `(id, source_issue_id, component, source_record_id)` with `UNIQUE(source_issue_id, component)`.
- `issue_fix_versions`: `(id, source_issue_id, fix_version, source_record_id)` with `UNIQUE(source_issue_id, fix_version)`.
- `issue_links`: `(id, source_issue_id, target_issue_id, link_name, outward_label, is_containment, source_record_id)` with `UNIQUE(source_issue_id, target_issue_id, link_name)`.
- `commit_issue_links`: `(id, commit_hash, source_issue_id, source_record_id)` with `UNIQUE(commit_hash, source_issue_id)`.
- `code_changes`: `(id, commit_hash, file_path, old_file_path, change_type, is_deleted, lines_added, lines_removed, source_record_id)`.
- `unresolved_relationships`: `(id, dataset_name, source_table, source_record_id, source_id, target_type, target_id, reason, batch_id, detected_at)` with `UNIQUE(dataset_name, source_table, source_id, target_type, target_id)`.
- `ingestion_batches`: `(id, dataset_name, started_at, completed_at, status, records_processed, records_inserted, records_updated, records_skipped, records_failed, unresolved_relationships, error_message)`.
- `schema_migrations`: `(version, name, applied_at, checksum)`.

---

## 3. Nine Source Tables and Record Counts

| Source Table | Actual Count | Expected Count | Verification Status |
|---|---:|---:|---|
| `meta` | 4 | 4 | Exact match verified |
| `issue` | 6219 | 6219 | Exact match verified |
| `issue_component` | 3988 | 3988 | Exact match verified |
| `issue_fix_version` | 3556 | 3556 | Exact match verified |
| `issue_comment` | 32960 | 32960 | Exact match verified |
| `issue_link` | 1699 | 1699 | Exact match verified |
| `change_set` | 4426 | 4426 | Exact match verified |
| `change_set_link` | 2893 | 2893 | Exact match verified |
| `code_change` | 35263 | 35263 | Exact match verified |
| **Total Source Rows** | **91008** | **90,008** | **91,008 actual records present in source SQLite file** |

---

## 4. Source-to-Destination Mapping

| Destination Table | Entity Purpose | Physical Row Count |
|---|---|---:|
| `source_records` | Immutable raw provenance log | 91008 |
| `issues` | Normalized Jira issues | 6219 |
| `commits` | Normalized Git commits | 4426 |
| `issue_comments` | Normalized comments | 32960 |
| `issue_components` | Issue to component associations | 3988 |
| `issue_fix_versions` | Issue to fix-version associations | 3556 |
| `issue_links` | Resolved issue-to-issue links | 1677 |
| `commit_issue_links` | Resolved commit-to-issue links | 2891 |
| `code_changes` | File-level code modification diffs | 35263 |
| `unresolved_relationships` | Persisted unresolved references table | 24 |

### Exact Accounting Reconciliation
$$\text{Total Source Rows (91,008)} = \text{Normalized (90,980)} + \text{Unresolved (24)} + \text{Metadata (4)}$$
- **Total Source Records Captured:** 91008 / 91008 (100.0%)
- **Normalized Domain Records:** 90980
- **Unresolved Relationship Records:** 24
- **Metadata Records:** 4 (preserved in `source_records`)
- **Records Discarded / Lost:** 0 (Zero silent discards)
- **Reconciliation Balance:** **RECONCILED**

---

## 5. Provenance and Timestamp Handling

- **ISO 8601 String Preservation:** Exact original JSON representations stored in `source_records.raw_data`.
- **Timezone Preservation:** Zoned columns (e.g. `created_date_zoned`, `committed_date_zoned`) parsed into timezone-aware `datetime` objects.
- **Nullable Values:** Absent timestamps remain `NULL` (no manufactured dates).
- **Ingestion Provenance:** `source_records.ingested_at` generated via SQLite `CURRENT_TIMESTAMP`, strictly separated from event dates.
- **Timestamp Coverage:**
  - Issues: 6219/6219 (100.0%)
  - Commits: 4426/4426 (100.0%)
  - Comments: 32960/32960 (100%)
- **Bot Collision Protection:** Composite source IDs for `issue_comment` include a SHA-256 slice of message content, preventing collision between comments posted at the exact same millisecond.

---

## 6. Idempotency Verification

| Metric | Pass 1 (Initial Ingestion) | Pass 2 (Repeat Ingestion) |
|---|---:|---:|
| Batch Status | COMPLETED | COMPLETED |
| Records Processed | 91008 | 91008 |
| Records Inserted | 91008 | 0 |
| Records Updated | 0 | 0 |
| Records Skipped | 0 | 91008 |
| Unresolved Relationships | 24 | 24 |
| Duplicate Normalized Records Created | 0 | 0 |
| **Idempotency Verdict** | — | **✅ PASSED (100% Idempotent)** |

---

## 7. Unresolved Relationship Accounting

| Table | Unresolved Count | Root Cause / Failure Reason |
|---|---|---|
| `issue_link` | 22 | Targets reference external Apache projects (`FLINK-2735`, `SM-2743`, `ATLAS-82`, `SAMZA-375`, `ATLAS-1131`) not present in Kafka Jira |
| `change_set_link` | 2 | Commits `83b8cf96...` and `46da01a4...` reference issue keys `KAFKA-6233` and `KAFKA-4623` which do not exist in the source `issue` table |
| **Total Unresolved** | **24** | **100% of unresolved references persisted, queryable, and tracked** |

---

## 8. Multi-Target Relationship Support

- **Model Unique Constraint:** `UNIQUE(dataset_name, source_table, source_id, target_type, target_id)`.
- **Multi-Endpoint Capture:** In `issue_link` and `change_set_link`, when both referenced entities are missing from the dataset, both distinct unresolved references are recorded.
- **Physical Coexistence:** Verified that multiple missing targets originating from the same source record coexist without primary key or unique index conflicts.

---

## 9. Dataset Isolation

- **Scoped APIs:**
  - `GET /api/statistics?dataset_name=...` scopes unresolved relationships and source records to the requested dataset.
  - `GET /api/ingestion/unresolved?dataset_name=...` filters unresolved references by dataset.
  - `POST /api/ingestion/run?dataset_name=...` binds ingestion batches and cache lookups to the specified dataset.
- **Cross-Dataset Isolation:** Verified via automated tests that statistics and unresolved queries for `DatasetAlpha` never include records from `DatasetBeta`.

---

## 10. Failure and Recovery Behavior

1. **Transaction Boundaries:** Ingestion executes chunked database commits (every 1,000 rows per table) rather than a single monolithic transaction.
2. **Partial Failure Handling:** Uncommitted rows in an aborted chunk are rolled back via `db.rollback()`. The batch status is permanently marked `FAILED` with `error_message` and `completed_at`.
3. **Safe Resumption:** Recovery runs in a new batch, skips already-committed records via SHA-256 matches, processes remaining records, and marks the recovery batch `COMPLETED` without duplicate records.

---

## 11. SQLite Migration Details

- **Migration System:** Versioned migration runner implemented in `backend/migrations/runner.py`.
- **Migration 001:** `backend/migrations/migration_001_unresolved_5col_unique.py`.
- **Execution Mechanism:** Safe SQLite 12-step table recreation pattern with `PRAGMA foreign_keys = OFF` during table recreation, verified copy of all 24 existing records, and re-creation of all 4 secondary indexes.
- **Migration Tracking:** Version recorded in `schema_migrations` table.
- **Repeatability:** Executing migration against an already-migrated database is verified to be a safe no-op.

---

## 12. Before-and-After Physical Schema

### Before Migration:
```sql
CONSTRAINT _unresolved_rel_uc UNIQUE (dataset_name, source_table, source_id)
```
*Index columns:* `('dataset_name', 'source_table', 'source_id')` (3 columns)

### After Migration:
```sql
CREATE TABLE "unresolved_relationships" (
            id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
            dataset_name VARCHAR NOT NULL DEFAULT 'KafkaSQLite',
            source_table VARCHAR NOT NULL,
            source_record_id INTEGER,
            source_id VARCHAR NOT NULL,
            target_type VARCHAR NOT NULL,
            target_id VARCHAR NOT NULL,
            reason TEXT NOT NULL,
            batch_id INTEGER,
            detected_at DATETIME DEFAULT (CURRENT_TIMESTAMP),
            CONSTRAINT _unresolved_rel_uc UNIQUE (dataset_name, source_table, source_id, target_type, target_id),
            FOREIGN KEY(source_record_id) REFERENCES source_records (id),
            FOREIGN KEY(batch_id) REFERENCES ingestion_batches (id)
        )
```
*Physical Unique Index Columns:* `('dataset_name', 'source_table', 'source_id', 'target_type', 'target_id')` (5 columns)

---

## 13. Backup Path and Integrity Result

- **Backup Strategy:** Timestamped online SQLite snapshot created via `backup_database()` prior to table recreation.
- **Latest Backup File:** `temporal_knowledge_backup_20261030_152031_fc7ea1.db`
- **Backup Size:** `129,925,120 bytes`
- **Backup Integrity Check:** `ok` (`PRAGMA integrity_check`)

---

## 14. Foreign-Key and Database Integrity Verification

- **PRAGMA foreign_key_check:** `[]` (0 foreign key violations)
- **PRAGMA integrity_check:** `ok` (Database integrity verified)
- **Foreign Keys on `unresolved_relationships`:**
  - `source_record_id` -> `source_records(id)`
  - `batch_id` -> `ingestion_batches(id)`

---

## 15. API Endpoint Inventory

| Endpoint | Method | Capabilities | Status |
|---|---|---|---|
| `/` | GET | Welcome status message | ✅ Operational |
| `/health` | GET | Service health check | ✅ Operational |
| `/api/statistics` | GET | System metrics with optional `dataset_name` filter | ✅ Operational |
| `/api/issues` | GET | Paginated list (`skip`, `limit`, `status`, `issue_type`, `priority`) | ✅ Operational |
| `/api/issues/{issue_id}` | GET | Full issue detail (comments, components, fix versions) | ✅ Operational |
| `/api/commits/{commit_hash}` | GET | Commit detail with file diffs and change types | ✅ Operational |
| `/api/ingestion/run` | POST | Ingestion trigger with optional `dataset_name` | ✅ Operational |
| `/api/ingestion/status` | GET | Ingestion status with optional `dataset_name` | ✅ Operational |
| `/api/ingestion/batches` | GET | Batch history with optional `dataset_name` | ✅ Operational |
| `/api/ingestion/unresolved` | GET | Unresolved links with `source_table` and `dataset_name` filters | ✅ Operational |

---

## 16. Automated Test Results

```
============================= test session starts =============================
collecting ... collected 70 items

tests/test_api.py::test_root PASSED                                      [  1%]
tests/test_api.py::test_health PASSED                                    [  2%]
tests/test_api.py::test_dataset_exists PASSED                            [  4%]
tests/test_api.py::test_database_inspection_read_only PASSED             [  5%]
tests/test_api_completion.py::test_statistics_endpoint PASSED            [  7%]
tests/test_api_completion.py::test_issues_endpoints PASSED               [  8%]
tests/test_api_completion.py::test_commits_endpoint PASSED               [ 10%]
tests/test_database.py::test_source_record_persistence PASSED            [ 11%]
tests/test_ingestion.py::test_idempotent_ingestion PASSED                [ 12%]
tests/test_migration.py::test_backup_creation_and_integrity PASSED       [ 14%]
tests/test_migration.py::test_migration_upgrades_schema_and_preserves_data PASSED [ 15%]
tests/test_migration.py::test_multi_target_unresolved_coexist_in_migrated_db PASSED [ 17%]
tests/test_migration.py::test_migration_is_repeatable_and_safe PASSED    [ 18%]
tests/test_work021.py::TestDatabaseInitialization::test_all_tables_created PASSED [ 20%]
tests/test_work021.py::TestDatabaseInitialization::test_source_record_unique_constraint PASSED [ 21%]
tests/test_work021.py::TestModelRelationships::test_issue_comment_relationship PASSED [ 22%]
tests/test_work021.py::TestModelRelationships::test_issue_component_relationship PASSED [ 24%]
tests/test_work021.py::TestModelRelationships::test_issue_fix_version_relationship PASSED [ 25%]
tests/test_work021.py::TestModelRelationships::test_commit_code_change_relationship PASSED [ 27%]
tests/test_work021.py::TestSourceRecordPersistence::test_basic_persistence PASSED [ 28%]
tests/test_work021.py::TestSourceRecordPersistence::test_ingested_at_auto_set PASSED [ 30%]
tests/test_work021.py::TestTimestampPreservation::test_parse_date_with_z_suffix PASSED [ 31%]
tests/test_work021.py::TestTimestampPreservation::test_parse_date_with_timezone_offset PASSED [ 32%]
tests/test_work021.py::TestTimestampPreservation::test_parse_date_none_returns_none PASSED [ 34%]
tests/test_work021.py::TestTimestampPreservation::test_parse_date_missing_is_nullable PASSED [ 35%]
tests/test_work021.py::TestTimestampPreservation::test_raw_data_preserves_original_string PASSED [ 37%]
tests/test_work021.py::TestTimestampPreservation::test_zoned_timestamp_preferred_over_plain PASSED [ 38%]
tests/test_work021.py::TestIdempotentIngestion::test_second_run_creates_no_new_source_records PASSED [ 40%]
tests/test_work021.py::TestIdempotentIngestion::test_second_run_skipped_equals_first_inserted PASSED [ 41%]
tests/test_work021.py::TestIdempotentIngestion::test_no_duplicate_issues PASSED [ 42%]
tests/test_work021.py::TestIdempotentIngestion::test_no_duplicate_commits PASSED [ 44%]
tests/test_work021.py::TestIdempotentIngestion::test_no_duplicate_comments PASSED [ 45%]
tests/test_work021.py::TestIdempotentIngestion::test_no_duplicate_code_changes PASSED [ 47%]
tests/test_work021.py::TestIdempotentIngestion::test_record_hashes_stable PASSED [ 48%]
tests/test_work021.py::TestDuplicateHandling::test_repeated_identical_rows_not_collapsed PASSED [ 50%]
tests/test_work021.py::TestDuplicateHandling::test_changed_source_record_updates_hash PASSED [ 51%]
tests/test_work021.py::TestMissingValues::test_issue_with_null_resolved_date PASSED [ 52%]
tests/test_work021.py::TestMissingValues::test_source_record_nullable_timestamps PASSED [ 54%]
tests/test_work021.py::TestUnresolvedRelationships::test_comment_orphan_tracked PASSED [ 55%]
tests/test_work021.py::TestUnresolvedRelationships::test_code_change_orphan_tracked PASSED [ 57%]
tests/test_work021.py::TestAPIStatistics::test_statistics_endpoint_empty_db PASSED [ 58%]
tests/test_work021.py::TestAPIStatistics::test_statistics_endpoint_with_data PASSED [ 60%]
tests/test_work021.py::TestAPIIssues::test_list_issues_empty PASSED      [ 61%]
tests/test_work021.py::TestAPIIssues::test_list_issues_pagination PASSED [ 62%]
tests/test_work021.py::TestAPIIssues::test_issue_filter_by_status PASSED [ 64%]
tests/test_work021.py::TestAPIIssues::test_issue_detail_found PASSED     [ 65%]
tests/test_work021.py::TestAPIIssues::test_issue_detail_not_found PASSED [ 67%]
tests/test_work021.py::TestAPIIssues::test_issue_response_includes_comments_and_components PASSED [ 68%]
tests/test_work021.py::TestAPICommits::test_commit_detail_found PASSED   [ 70%]
tests/test_work021.py::TestAPICommits::test_commit_detail_not_found PASSED [ 71%]
tests/test_work021.py::TestAPICommits::test_commit_includes_code_changes PASSED [ 72%]
tests/test_work021.py::TestAPIIngestionBatches::test_batches_endpoint_empty PASSED [ 74%]
tests/test_work021.py::TestAPIIngestionBatches::test_batches_endpoint_with_data PASSED [ 75%]
tests/test_work021.py::TestAPIIngestionBatches::test_batches_pagination PASSED [ 77%]
tests/test_work021.py::TestAPIHealthAndRoot::test_root_endpoint PASSED   [ 78%]
tests/test_work021.py::TestAPIHealthAndRoot::test_health_endpoint PASSED [ 80%]
tests/test_work021.py::TestSourceDatabaseProtection::test_source_db_opened_read_only PASSED [ 81%]
tests/test_work021.py::TestSourceDatabaseProtection::test_source_db_write_rejected PASSED [ 82%]
tests/test_work021.py::TestRealKafkaIngestion::test_kafka_db_tables_exist PASSED [ 84%]
tests/test_work021.py::TestRealKafkaIngestion::test_kafka_idempotent_ingestion PASSED [ 85%]
tests/test_work022.py::TestUnresolvedRelationshipConsistency::test_unresolved_records_persisted_and_queryable PASSED [ 87%]
tests/test_work022.py::TestUnresolvedRelationshipConsistency::test_unresolved_count_consistency_across_runs PASSED [ 88%]
tests/test_work022.py::TestUnresolvedRelationshipConsistency::test_unresolved_api_endpoint PASSED [ 90%]
tests/test_work022.py::TestUnresolvedRelationshipConsistency::test_statistics_reports_dataset_level_unresolved PASSED [ 91%]
tests/test_work022.py::TestSourceVsNormalizedAccounting::test_accounting_integrity PASSED [ 92%]
tests/test_work022.py::TestInterruptedIngestionAndRecovery::test_partial_ingestion_failure_records_failed_batch PASSED [ 94%]
tests/test_work022.py::TestInterruptedIngestionAndRecovery::test_safe_recovery_after_partial_failure PASSED [ 95%]
tests/test_work022.py::TestWork023Corrections::test_both_endpoints_missing_creates_two_unresolved_records PASSED [ 97%]
tests/test_work022.py::TestWork023Corrections::test_repeated_ingestion_multi_unresolved_no_duplicates PASSED [ 98%]
tests/test_work022.py::TestWork023Corrections::test_dataset_scoped_statistics_and_unresolved_isolation PASSED [100%]

============================== warnings summary ===============================
..\..\..\anaconda3\Lib\site-packages\anyio\pytest_plugin.py:14
  C:\Users\User\anaconda3\Lib\site-packages\anyio\pytest_plugin.py:14: PytestRemovedIn10Warning: _pytest.python.CallSpec2 has been renamed to CallSpec.
  The CallSpec2 alias will be removed in pytest 10.
  Update imports to use CallSpec instead.
  See https://docs.pytest.org/en/stable/deprecations.html#callspec2-renamed
    from _pytest.python import CallSpec2

app\core\config.py:5
  C:\Users\User\Desktop\Temporal-PK\backend\app\core\config.py:5: PydanticDeprecatedSince20: Support for class-based `config` is deprecated, use ConfigDict instead. Deprecated in Pydantic V2.0 to be removed in V3.0. See Pydantic V2 Migration Guide at https://errors.pydantic.dev/2.13/migration/
    class Settings(BaseSettings):

app\main.py:12
  C:\Users\User\Desktop\Temporal-PK\backend\app\main.py:12: DeprecationWarning:
          on_event is deprecated, use lifespan event handlers instead.

          Read more about it in the
          [FastAPI docs for Lifespan Events](https://fastapi.tiangolo.com/advanced/events/).

    @app.on_event("startup")

..\..\..\anaconda3\Lib\site-packages\fastapi\applications.py:4601
  C:\Users\User\anaconda3\Lib\site-packages\fastapi\applications.py:4601: DeprecationWarning:
          on_event is deprecated, use lifespan event handlers instead.

          Read more about it in the
          [FastAPI docs for Lifespan Events](https://fastapi.tiangolo.com/advanced/events/).

    return self.router.on_event(event_type)  # ty: ignore[deprecated]

..\..\..\anaconda3\Lib\site-packages\fastapi\testclient.py:1
  C:\Users\User\anaconda3\Lib\site-packages\fastapi\testclient.py:1: StarletteDeprecationWarning: Using `httpx` with `starlette.testclient` is deprecated; install `httpx2` instead.
    from starlette.testclient import TestClient as TestClient  # noqa

-- Docs: https://docs.pytest.org/en/stable/how-to/capture-warnings.html
================= 70 passed, 5 warnings in 145.21s (0:02:25) ==================
```

---

## 17. Known Limitations

1. **Source SQLite lacks foreign keys:** Referential integrity must remain enforced at the application/ingestion layer.
2. **External issue keys:** The 22 issue links referencing external Apache Jira projects (`FLINK`, `ATLAS`, `SAMZA`, `SM`) cannot be resolved unless external project datasets are provided.
3. **Missing parent commits in links:** The 2 commit-issue links reference `KAFKA-6233` and `KAFKA-4623` which are absent from the Kafka issue extract.
4. **Scope boundary:** LLM extraction, vector embeddings, entity resolution, and temporal event graphs remain intentionally omitted in compliance with Work 02 specifications.

---

## 18. Final Acceptance Status

| Acceptance Criterion | Verification Status |
|---|---|
| All nine source tables accounted for | ✅ Verified (91,008 source rows) |
| All 91,008 source records preserved | ✅ Verified (91,008 in `source_records`) |
| Normalized record counts reconcile | ✅ Verified ($91,008 = 90,980 + 24 + 4$) |
| All 24 Kafka unresolved relationships persisted | ✅ Verified in `unresolved_relationships` |
| Multiple unresolved targets from one record supported | ✅ Verified in physical SQLite schema |
| Physical SQLite schema matches SQLAlchemy models | ✅ Verified (5-column composite constraint) |
| Migration is repeatable and safe | ✅ Verified (0 changes on repeat execution) |
| Source database remains unchanged | ✅ Verified (`sample_data/raw/kafka.sqlite3` clean) |
| Repeated ingestion creates no duplicates | ✅ Verified (0 inserted, 91,008 skipped) |
| Partial ingestion recovery works | ✅ Verified in automated failure simulation |
| Dataset isolation works | ✅ Verified across multi-dataset tests |
| All required API endpoints pass tests | ✅ Verified with Pydantic validation |
| Full automated test suite passes | ✅ Verified (All 70 tests pass) |
| Verification report updated | ✅ Verified |
| Final Work 02 Verdict | **WORK 02 COMPLETE** |
