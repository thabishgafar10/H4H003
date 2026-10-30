# Dataset Inspection Report: Apache Kafka SQLite Dataset

## 1. Database Overview
- **Database Path:** `C:\Users\User\Desktop\Temporal-PK\sample_data\raw\kafka.sqlite3`
- **Total Tables:** 9

## 2. Table Summary & Schema Details

### Table: `meta`
**Description:** Stores metadata about the extracted database/ingestion process.

- **Total Records:** 4
- **Declared Primary Key(s):** None declared
- **Declared Foreign Keys:** None declared (relationships inferred by matching column names)

**Columns:**

| Column Name | Data Type | Primary Key | Nullable |
| --- | --- | --- | --- |
| `key` | TEXT | No | No |
| `value` | TEXT | No | No |

**Sample Record:**
```json
{
  "key": "traceng_db_version",
  "value": "1.0"
}
```

---

### Table: `issue`
**Description:** Stores main Jira/issue tracking records including tickets, summaries, status, assignees, and dates.

- **Total Records:** 6219
- **Declared Primary Key(s):** None declared
- **Declared Foreign Keys:** None declared (relationships inferred by matching column names)

**Columns:**

| Column Name | Data Type | Primary Key | Nullable |
| --- | --- | --- | --- |
| `issue_id` | TEXT | No | No |
| `type` | TEXT | No | Yes |
| `created_date` | TEXT | No | Yes |
| `created_date_zoned` | TEXT | No | Yes |
| `updated_date` | TEXT | No | Yes |
| `updated_date_zoned` | TEXT | No | Yes |
| `resolved_date` | TEXT | No | Yes |
| `resolved_date_zoned` | TEXT | No | Yes |
| `summary` | TEXT | No | Yes |
| `description` | TEXT | No | Yes |
| `priority` | TEXT | No | Yes |
| `status` | TEXT | No | Yes |
| `resolution` | TEXT | No | Yes |
| `assignee` | TEXT | No | Yes |
| `assignee_username` | TEXT | No | Yes |
| `reporter` | TEXT | No | Yes |
| `reporter_username` | TEXT | No | Yes |

- **Important Date/Timestamp Columns:** `created_date`, `created_date_zoned`, `updated_date`, `updated_date_zoned`, `resolved_date`, `resolved_date_zoned`

**Sample Record:**
```json
{
  "issue_id": "KAFKA-5707",
  "type": "Bug",
  "created_date": "2017-08-07T01:39:37.367Z",
  "created_date_zoned": "2017-08-07T01:39:37.367Z",
  "updated_date": "2017-08-07T08:39:06.009Z",
  "updated_date_zoned": "2017-08-07T08:39:06.009Z",
  "resolved_date": "2017-08-07T08:39:05.957Z",
  "resolved_date_zoned": "2017-08-07T08:39:05.957Z",
  "summary": "Remove useless `--force` option for both TopicCommand and ConfigCommand",
  "description": "`TopicCommand` and `ConfigCommand` do expose an option named `--force` which suppresses console prompts, but both classes do not actually use it. Shou...",
  "priority": "Minor",
  "status": "Resolved",
  "resolution": "Not A Bug",
  "assignee": "huxihx",
  "assignee_username": "huxi_2b",
  "reporter": "huxihx",
  "reporter_username": "huxi_2b"
}
```

---

### Table: `issue_component`
**Description:** Maps issues to product components (e.g., admin, core, producer).

- **Total Records:** 3988
- **Declared Primary Key(s):** None declared
- **Declared Foreign Keys:** None declared (relationships inferred by matching column names)

**Columns:**

| Column Name | Data Type | Primary Key | Nullable |
| --- | --- | --- | --- |
| `issue_id` | TEXT | No | No |
| `component` | TEXT | No | No |

**Sample Record:**
```json
{
  "issue_id": "KAFKA-5707",
  "component": "admin"
}
```

---

### Table: `issue_fix_version`
**Description:** Maps issues to intended/actual fix release versions.

- **Total Records:** 3556
- **Declared Primary Key(s):** None declared
- **Declared Foreign Keys:** None declared (relationships inferred by matching column names)

**Columns:**

| Column Name | Data Type | Primary Key | Nullable |
| --- | --- | --- | --- |
| `issue_id` | TEXT | No | No |
| `fix_version` | TEXT | No | No |

**Sample Record:**
```json
{
  "issue_id": "KAFKA-5771",
  "fix_version": "0.11.0.1"
}
```

---

### Table: `issue_comment`
**Description:** Stores discussion comments and automated system posts on issues.

- **Total Records:** 32960
- **Declared Primary Key(s):** None declared
- **Declared Foreign Keys:** None declared (relationships inferred by matching column names)

**Columns:**

| Column Name | Data Type | Primary Key | Nullable |
| --- | --- | --- | --- |
| `issue_id` | TEXT | No | No |
| `username` | TEXT | No | Yes |
| `display_name` | TEXT | No | Yes |
| `created_date` | TEXT | No | Yes |
| `created_date_zoned` | TEXT | No | Yes |
| `message` | TEXT | No | Yes |

- **Important Date/Timestamp Columns:** `created_date`, `created_date_zoned`

**Sample Record:**
```json
{
  "issue_id": "KAFKA-5707",
  "username": "githubbot",
  "display_name": "ASF GitHub Bot",
  "created_date": "2017-08-07T01:42:11.723Z",
  "created_date_zoned": "2017-08-07T01:42:11.723Z",
  "message": "GitHub user huxihx opened a pull request:\n\n    https://github.com/apache/kafka/pull/3632\n\n    KAFKA-5707: TopicCommand and ConfigCommand should remove..."
}
```

---

### Table: `issue_link`
**Description:** Captures relationships/links between issues (e.g., Duplicates, Depends, Blocks).

- **Total Records:** 1699
- **Declared Primary Key(s):** None declared
- **Declared Foreign Keys:** None declared (relationships inferred by matching column names)

**Columns:**

| Column Name | Data Type | Primary Key | Nullable |
| --- | --- | --- | --- |
| `source_issue_id` | TEXT | No | No |
| `target_issue_id` | TEXT | No | No |
| `name` | TEXT | No | Yes |
| `outward_label` | TEXT | No | Yes |
| `is_containment` | INTEGER | No | Yes |

**Sample Record:**
```json
{
  "source_issue_id": "KAFKA-5786",
  "target_issue_id": "KAFKA-5541",
  "name": "Duplicate",
  "outward_label": "duplicates",
  "is_containment": 0
}
```

---

### Table: `change_set`
**Description:** Stores Git commit metadata including commit hash, author, commit date, and commit message.

- **Total Records:** 4426
- **Declared Primary Key(s):** None declared
- **Declared Foreign Keys:** None declared (relationships inferred by matching column names)

**Columns:**

| Column Name | Data Type | Primary Key | Nullable |
| --- | --- | --- | --- |
| `commit_hash` | TEXT | No | No |
| `committed_date` | TEXT | No | Yes |
| `committed_date_zoned` | TEXT | No | Yes |
| `message` | TEXT | No | Yes |
| `author` | TEXT | No | Yes |
| `author_email` | TEXT | No | Yes |
| `is_merge` | INTEGER | No | Yes |

- **Important Date/Timestamp Columns:** `committed_date`, `committed_date_zoned`

**Sample Record:**
```json
{
  "commit_hash": "83b8cf96f92242d61db9e87aafce1e8dc457c1a1",
  "committed_date": "2017-11-19T18:54:50Z",
  "committed_date_zoned": "2017-11-19T10:54:50-08:00",
  "message": "KAFKA-6233: Removed unnecessary null check in StringSerializer\n\nRemoved unnecessary null check\nif (encodingValue != null && encodingValue instanceof S...",
  "author": "Sagar Chavan",
  "author_email": "sagar.chavan3172@gmail.com",
  "is_merge": 0
}
```

---

### Table: `change_set_link`
**Description:** Links Git commits to Jira issues based on commit message references or issue references.

- **Total Records:** 2893
- **Declared Primary Key(s):** None declared
- **Declared Foreign Keys:** None declared (relationships inferred by matching column names)

**Columns:**

| Column Name | Data Type | Primary Key | Nullable |
| --- | --- | --- | --- |
| `commit_hash` | TEXT | No | No |
| `issue_id` | TEXT | No | No |

**Sample Record:**
```json
{
  "commit_hash": "83b8cf96f92242d61db9e87aafce1e8dc457c1a1",
  "issue_id": "KAFKA-6233"
}
```

---

### Table: `code_change`
**Description:** Captures file-level changes made in a commit, including lines added/removed and change types.

- **Total Records:** 35263
- **Declared Primary Key(s):** None declared
- **Declared Foreign Keys:** None declared (relationships inferred by matching column names)

**Columns:**

| Column Name | Data Type | Primary Key | Nullable |
| --- | --- | --- | --- |
| `commit_hash` | TEXT | No | No |
| `file_path` | TEXT | No | Yes |
| `old_file_path` | TEXT | No | Yes |
| `change_type` | TEXT | No | Yes |
| `is_deleted` | INTEGER | No | Yes |
| `sum_added_lines` | INTEGER | No | Yes |
| `sum_removed_lines` | INTEGER | No | Yes |

**Sample Record:**
```json
{
  "commit_hash": "83b8cf96f92242d61db9e87aafce1e8dc457c1a1",
  "file_path": "clients/src/main/java/org/apache/kafka/common/serialization/StringSerializer.java",
  "old_file_path": "clients/src/main/java/org/apache/kafka/common/serialization/StringSerializer.java",
  "change_type": "MODIFY",
  "is_deleted": 0,
  "sum_added_lines": 1,
  "sum_removed_lines": 1
}
```

---

## 3. Relationships & Foreign Key Inferences
- `issue_component.issue_id` -> `issue.issue_id`
- `issue_fix_version.issue_id` -> `issue.issue_id`
- `issue_comment.issue_id` -> `issue.issue_id`
- `issue_link.source_issue_id` / `target_issue_id` -> `issue.issue_id`
- `change_set_link.commit_hash` -> `change_set.commit_hash`
- `change_set_link.issue_id` -> `issue.issue_id`
- `code_change.commit_hash` -> `change_set.commit_hash`

## 4. Key Observations Relevant to Temporal Product Knowledge Reconstruction
1. **Complete Traceability Chain:** The dataset connects Jira issues (`issue`) to Git commits (`change_set`) via `change_set_link`, and commits to modified code files (`code_change`). This enables precise mapping of feature changes and bug fixes to specific code files.
2. **Rich Discussion Context:** The `issue_comment` table (32,960 records) contains extensive technical discussions, code review links, design proposals, and decision history.
3. **Version & Component Evolution:** `issue_fix_version` and `issue_component` allow tracking feature evolution and bug fix frequency across specific product modules and release versions.
4. **Explicit Temporal Anchors:** Dates are stored consistently in ISO 8601 strings (e.g. `created_date`, `updated_date`, `resolved_date`, `committed_date`), facilitating exact chronological timeline construction.

## 5. Potential Data Quality Limitations
1. **Missing Formal Foreign Keys:** Most tables do not have SQL-level declared foreign keys, relying instead on application-level integrity.
2. **Unlinked Commits / Issues:** Some commits in `change_set` may not map to an issue in `change_set_link` if commit messages lacked issue key references.
3. **Automated Comments:** `issue_comment` contains bot posts (e.g., `githubbot`) alongside human developer comments which will need filtering for sentiment or design decision extractions.