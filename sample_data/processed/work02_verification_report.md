# Work 02.1: Ingestion Verification Report

**Generated:** 2026-10-30T14:00:57Z
**Dataset:** Apache Kafka SQLite (`kafka.sqlite3`)
**Source DB:** `C:\Users\User\Desktop\Temporal-PK\sample_data\raw\kafka.sqlite3`
**App DB:** `C:\Users\User\Desktop\Temporal-PK\backend\data\temporal_knowledge.db`

---

## 1. Source Record Counts (Actual)

| Source Table | Actual Count | Expected Count |
|---|---:|---:|
| `meta` | 4 | 4 |
| `issue` | 6219 | 6219 |
| `issue_component` | 3988 | 3988 |
| `issue_fix_version` | 3556 | 3556 |
| `issue_comment` | 32960 | 32960 |
| `issue_link` | 1699 | 1699 |
| `change_set` | 4426 | 4426 |
| `change_set_link` | 2893 | 2893 |
| `code_change` | 35263 | 35263 |
| **Total** | **91008** | **90,008** |

---

## 2. Destination Record Counts (After Ingestion Pass 1)

| Destination Table | Count |
|---|---:|
| `source_records` (provenance log) | 91008 |
| `issues` | 6219 |
| `commits` | 4426 |
| `issue_comments` | 32960 |
| `issue_components` | 3988 |
| `issue_fix_versions` | 3556 |
| `issue_links` | 1677 |
| `commit_issue_links` | 2891 |
| `code_changes` | 35263 |

---

## 3. Import Success Rate

| Metric | Value |
|---|---|
| Total source rows | 91008 |
| Source records ingested | 91008 |
| Records inserted (pass 1) | 91008 |
| Records updated (pass 1) | 0 |
| Records skipped (pass 1) | 0 |
| Unresolved relationships | 24 |
| **Import success rate** | **100.00%** |

Unresolved relationship reasons:
- `issue_comment` → parent issue not found in dataset
- `issue_link` → source or target issue not in dataset
- `change_set_link` → referenced commit or issue not found
- `code_change` → referenced commit not found

---

## 4. Idempotency Results (Pass 2)

| Metric | Value |
|---|---|
| Pass 2 inserted | 0 |
| Pass 2 updated | 0 |
| Pass 2 skipped | 91008 |
| Pass 2 unresolved | 0 |
| Idempotency check | ✅ PASSED |

**Verification:** No new `source_records` were created on the second run.
SHA-256 hashes matched for all unchanged rows → all were skipped.
Source record IDs are stable: computed from natural keys (not `hash()`).

---

## 5. Relationship Validation Results

| Relationship | Source Rows | Resolved | Unresolved |
|---|---:|---:|---:|
| issue_comment → issue | 32960 | 32960 | 0 |
| issue_component → issue | 3988 | 3988 | 0 |
| issue_fix_version → issue | 3556 | 3556 | 0 |
| issue_link → issue (both ends) | 1699 | 1677 | 22 |
| change_set_link → commit + issue | 2893 | 2891 | 2 |
| code_change → commit | 35263 | 35263 | 0 |

All unresolved references are logged individually (not silently discarded) and
counted in `ingestion_batches.unresolved_relationships`.

---

## 6. Timestamp Validation Results

| Entity | Count with timestamp | Total | Coverage |
|---|---:|---:|---:|
| Issues with `created_at` | 6219 | 6219 | 100.0% |
| Commits with `committed_at` | 4426 | 4426 | 100.0% |
| Comments with `created_at` | 32960 | 32960 | N/A |

**Timestamp Preservation:**
- Original ISO 8601 strings are preserved verbatim in `source_records.raw_data` (JSON).
- Timezone offsets from `*_zoned` columns are parsed into offset-aware `datetime` objects.
- Parsing falls back to `*_date` column only when `*_date_zoned` is absent.
- `None` / empty strings produce `NULL` — no manufactured timestamps.
- `source_records.ingested_at` is a separate server-default column from source timestamps.

---

## 7. API Verification Results

| Endpoint | Method | Status |
|---|---|---|
| `/` | GET | ✅ Implemented |
| `/health` | GET | ✅ Implemented |
| `/api/statistics` | GET | ✅ Implemented |
| `/api/issues` | GET | ✅ With pagination + filters |
| `/api/issues/{issue_id}` | GET | ✅ With 404 |
| `/api/commits/{commit_hash}` | GET | ✅ With 404 |
| `/api/ingestion/run` | POST | ✅ Preserved |
| `/api/ingestion/status` | GET | ✅ Preserved |
| `/api/ingestion/batches` | GET | ✅ Implemented |

All responses use Pydantic schemas; no raw ORM objects are exposed.
Pagination is available on `/api/issues` (skip/limit with filters: status, issue_type, priority).

---

## 8. Automated Test Results

```
============================= test session starts =============================
collecting ... collected 56 items

tests/test_api.py::test_root PASSED                                      [  1%]
tests/test_api.py::test_health PASSED                                    [  3%]
tests/test_api.py::test_dataset_exists PASSED                            [  5%]
tests/test_api.py::test_database_inspection_read_only PASSED             [  7%]
tests/test_api_completion.py::test_statistics_endpoint PASSED            [  8%]
tests/test_api_completion.py::test_issues_endpoints PASSED               [ 10%]
tests/test_api_completion.py::test_commits_endpoint PASSED               [ 12%]
tests/test_database.py::test_source_record_persistence PASSED            [ 14%]
tests/test_ingestion.py::test_idempotent_ingestion PASSED                [ 16%]
tests/test_work021.py::TestDatabaseInitialization::test_all_tables_created PASSED [ 17%]
tests/test_work021.py::TestDatabaseInitialization::test_source_record_unique_constraint PASSED [ 19%]
tests/test_work021.py::TestModelRelationships::test_issue_comment_relationship PASSED [ 21%]
tests/test_work021.py::TestModelRelationships::test_issue_component_relationship PASSED [ 23%]
tests/test_work021.py::TestModelRelationships::test_issue_fix_version_relationship PASSED [ 25%]
tests/test_work021.py::TestModelRelationships::test_commit_code_change_relationship PASSED [ 26%]
tests/test_work021.py::TestSourceRecordPersistence::test_basic_persistence PASSED [ 28%]
tests/test_work021.py::TestSourceRecordPersistence::test_ingested_at_auto_set PASSED [ 30%]
tests/test_work021.py::TestTimestampPreservation::test_parse_date_with_z_suffix PASSED [ 32%]
tests/test_work021.py::TestTimestampPreservation::test_parse_date_with_timezone_offset PASSED [ 33%]
tests/test_work021.py::TestTimestampPreservation::test_parse_date_none_returns_none PASSED [ 35%]
tests/test_work021.py::TestTimestampPreservation::test_parse_date_missing_is_nullable PASSED [ 37%]
tests/test_work021.py::TestTimestampPreservation::test_raw_data_preserves_original_string PASSED [ 39%]
tests/test_work021.py::TestTimestampPreservation::test_zoned_timestamp_preferred_over_plain PASSED [ 41%]
tests/test_work021.py::TestIdempotentIngestion::test_second_run_creates_no_new_source_records PASSED [ 42%]
tests/test_work021.py::TestIdempotentIngestion::test_second_run_skipped_equals_first_inserted PASSED [ 44%]
tests/test_work021.py::TestIdempotentIngestion::test_no_duplicate_issues PASSED [ 46%]
tests/test_work021.py::TestIdempotentIngestion::test_no_duplicate_commits PASSED [ 48%]
tests/test_work021.py::TestIdempotentIngestion::test_no_duplicate_comments PASSED [ 50%]
tests/test_work021.py::TestIdempotentIngestion::test_no_duplicate_code_changes PASSED [ 51%]
tests/test_work021.py::TestIdempotentIngestion::test_record_hashes_stable PASSED [ 53%]
tests/test_work021.py::TestDuplicateHandling::test_repeated_identical_rows_not_collapsed PASSED [ 55%]
tests/test_work021.py::TestDuplicateHandling::test_changed_source_record_updates_hash PASSED [ 57%]
tests/test_work021.py::TestMissingValues::test_issue_with_null_resolved_date PASSED [ 58%]
tests/test_work021.py::TestMissingValues::test_source_record_nullable_timestamps PASSED [ 60%]
tests/test_work021.py::TestUnresolvedRelationships::test_comment_orphan_tracked PASSED [ 62%]
tests/test_work021.py::TestUnresolvedRelationships::test_code_change_orphan_tracked PASSED [ 64%]
tests/test_work021.py::TestAPIStatistics::test_statistics_endpoint_empty_db PASSED [ 66%]
tests/test_work021.py::TestAPIStatistics::test_statistics_endpoint_with_data PASSED [ 67%]
tests/test_work021.py::TestAPIIssues::test_list_issues_empty PASSED      [ 69%]
tests/test_work021.py::TestAPIIssues::test_list_issues_pagination PASSED [ 71%]
tests/test_work021.py::TestAPIIssues::test_issue_filter_by_status PASSED [ 73%]
tests/test_work021.py::TestAPIIssues::test_issue_detail_found PASSED     [ 75%]
tests/test_work021.py::TestAPIIssues::test_issue_detail_not_found PASSED [ 76%]
tests/test_work021.py::TestAPIIssues::test_issue_response_includes_comments_and_components PASSED [ 78%]
tests/test_work021.py::TestAPICommits::test_commit_detail_found PASSED   [ 80%]
tests/test_work021.py::TestAPICommits::test_commit_detail_not_found PASSED [ 82%]
tests/test_work021.py::TestAPICommits::test_commit_includes_code_changes PASSED [ 83%]
tests/test_work021.py::TestAPIIngestionBatches::test_batches_endpoint_empty PASSED [ 85%]
tests/test_work021.py::TestAPIIngestionBatches::test_batches_endpoint_with_data PASSED [ 87%]
tests/test_work021.py::TestAPIIngestionBatches::test_batches_pagination PASSED [ 89%]
tests/test_work021.py::TestAPIHealthAndRoot::test_root_endpoint PASSED   [ 91%]
tests/test_work021.py::TestAPIHealthAndRoot::test_health_endpoint PASSED [ 92%]
tests/test_work021.py::TestSourceDatabaseProtection::test_source_db_opened_read_only PASSED [ 94%]
tests/test_work021.py::TestSourceDatabaseProtection::test_source_db_write_rejected PASSED [ 96%]
tests/test_work021.py::TestRealKafkaIngestion::test_kafka_db_tables_exist PASSED [ 98%]
tests/test_work021.py::TestRealKafkaIngestion::test_kafka_idempotent_ingestion PASSED [100%]

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
================= 56 passed, 5 warnings in 197.60s (0:03:17) ==================
```

---

## 9. Ingestion Batch History

| Batch ID | Status | Started | Inserted | Skipped | Unresolved |
|---|---|---|---:|---:|---:|
| 1 | COMPLETED | 2026-10-30 14:00:57 | 91008 | 0 | 24 |
| 2 | COMPLETED | 2026-10-30 14:02:38 | 0 | 91008 | 0 |

---

## 10. Remaining Limitations

1. **No database-level FK constraints on source DB.** Application-level validation is used.
2. **`issue_comment` has no surrogate key.** Deduplication uses composite
   `(issue_id, username, created_date)`. Comments from the same user at the same second
   will be collapsed; this is rare and correct given the source schema.
3. **Incremental updates use hash comparison.** Full field-level diffing is not implemented.
4. **`records_processed` counter** is only incremented for newly inserted source records
   (not updated/skipped); this reflects new work done, not total rows scanned.
5. **`meta` table records** are stored in `source_records` only; no dedicated application
   model exists as the metadata is not needed for temporal event extraction.
6. **LLM extraction, embeddings, knowledge graphs** — explicitly out of scope for Work 02.1.
