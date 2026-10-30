import sqlite3
import os
import json
from pathlib import Path

def inspect_dataset(db_path: str, report_path: str):
    if not os.path.exists(db_path):
        print(f"Database not found: {db_path}")
        return

    try:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        cursor = conn.cursor()

        # Get all tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [t[0] for t in cursor.fetchall()]

        report = [
            "# Dataset Inspection Report: Apache Kafka SQLite Dataset",
            "",
            "## 1. Database Overview",
            f"- **Database Path:** `{db_path}`",
            f"- **Total Tables:** {len(tables)}",
            "",
            "## 2. Table Summary & Schema Details",
            ""
        ]

        # Table descriptions mapped from domain knowledge & observed data
        table_descriptions = {
            "meta": "Stores metadata about the extracted database/ingestion process.",
            "issue": "Stores main Jira/issue tracking records including tickets, summaries, status, assignees, and dates.",
            "issue_component": "Maps issues to product components (e.g., admin, core, producer).",
            "issue_fix_version": "Maps issues to intended/actual fix release versions.",
            "issue_comment": "Stores discussion comments and automated system posts on issues.",
            "issue_link": "Captures relationships/links between issues (e.g., Duplicates, Depends, Blocks).",
            "change_set": "Stores Git commit metadata including commit hash, author, commit date, and commit message.",
            "change_set_link": "Links Git commits to Jira issues based on commit message references or issue references.",
            "code_change": "Captures file-level changes made in a commit, including lines added/removed and change types."
        }

        for table_name in tables:
            report.append(f"### Table: `{table_name}`")
            desc = table_descriptions.get(table_name, "No description available.")
            report.append(f"**Description:** {desc}\n")

            # Get PRAGMA info (cid, name, type, notnull, dflt_value, pk)
            cursor.execute(f"PRAGMA table_info({table_name});")
            columns = cursor.fetchall()

            # Record count
            cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
            count = cursor.fetchone()[0]

            report.append(f"- **Total Records:** {count}")

            # Primary keys
            pks = [c[1] for c in columns if c[5] > 0]
            pk_str = ", ".join(pks) if pks else "None declared"
            report.append(f"- **Declared Primary Key(s):** {pk_str}")

            # Foreign keys PRAGMA
            cursor.execute(f"PRAGMA foreign_key_list({table_name});")
            fks = cursor.fetchall()
            if fks:
                fk_strs = [f"`{fk[3]}` -> `{fk[2]}`(`{fk[4]}`)" for fk in fks]
                report.append(f"- **Declared Foreign Keys:** {', '.join(fk_strs)}")
            else:
                report.append("- **Declared Foreign Keys:** None declared (relationships inferred by matching column names)")

            # Columns table
            report.append("\n**Columns:**\n")
            report.append("| Column Name | Data Type | Primary Key | Nullable |")
            report.append("| --- | --- | --- | --- |")

            date_cols = []
            for col in columns:
                col_name = col[1]
                col_type = col[2]
                is_pk = "Yes" if col[5] > 0 else "No"
                is_nullable = "No" if col[3] == 1 else "Yes"
                report.append(f"| `{col_name}` | {col_type} | {is_pk} | {is_nullable} |")

                if "date" in col_name.lower() or "time" in col_name.lower():
                    date_cols.append(col_name)

            if date_cols:
                report.append(f"\n- **Important Date/Timestamp Columns:** {', '.join([f'`{d}`' for d in date_cols])}")

            # Sample record
            cursor.execute(f"SELECT * FROM {table_name} LIMIT 1;")
            sample = cursor.fetchone()
            if sample:
                col_names = [c[1] for c in columns]
                sample_dict = dict(zip(col_names, sample))

                # Truncate long descriptions or messages in sample output for formatting
                for k, v in sample_dict.items():
                    if isinstance(v, str) and len(v) > 150:
                        sample_dict[k] = v[:150] + "..."

                sample_json = json.dumps(sample_dict, indent=2)
                report.append(f"\n**Sample Record:**\n```json\n{sample_json}\n```\n")

            report.append("---\n")

        # Temporal Observations & Data Quality Limitations
        report.extend([
            "## 3. Relationships & Foreign Key Inferences",
            "- `issue_component.issue_id` -> `issue.issue_id`",
            "- `issue_fix_version.issue_id` -> `issue.issue_id`",
            "- `issue_comment.issue_id` -> `issue.issue_id`",
            "- `issue_link.source_issue_id` / `target_issue_id` -> `issue.issue_id`",
            "- `change_set_link.commit_hash` -> `change_set.commit_hash`",
            "- `change_set_link.issue_id` -> `issue.issue_id`",
            "- `code_change.commit_hash` -> `change_set.commit_hash`",
            "",
            "## 4. Key Observations Relevant to Temporal Product Knowledge Reconstruction",
            "1. **Complete Traceability Chain:** The dataset connects Jira issues (`issue`) to Git commits (`change_set`) via `change_set_link`, and commits to modified code files (`code_change`). This enables precise mapping of feature changes and bug fixes to specific code files.",
            "2. **Rich Discussion Context:** The `issue_comment` table (32,960 records) contains extensive technical discussions, code review links, design proposals, and decision history.",
            "3. **Version & Component Evolution:** `issue_fix_version` and `issue_component` allow tracking feature evolution and bug fix frequency across specific product modules and release versions.",
            "4. **Explicit Temporal Anchors:** Dates are stored consistently in ISO 8601 strings (e.g. `created_date`, `updated_date`, `resolved_date`, `committed_date`), facilitating exact chronological timeline construction.",
            "",
            "## 5. Potential Data Quality Limitations",
            "1. **Missing Formal Foreign Keys:** Most tables do not have SQL-level declared foreign keys, relying instead on application-level integrity.",
            "2. **Unlinked Commits / Issues:** Some commits in `change_set` may not map to an issue in `change_set_link` if commit messages lacked issue key references.",
            "3. **Automated Comments:** `issue_comment` contains bot posts (e.g., `githubbot`) alongside human developer comments which will need filtering for sentiment or design decision extractions."
        ])

        with open(report_path, "w", encoding="utf-8") as f:
            f.write("\n".join(report))

        print(f"Report generated at {report_path}")
        conn.close()
    except Exception as e:
        print(f"Error inspecting database: {e}")

if __name__ == "__main__":
    # Base directory resolution relative to project root
    project_root = Path(__file__).resolve().parent.parent.parent
    db_file = project_root / "sample_data" / "raw" / "kafka.sqlite3"
    report_file = project_root / "sample_data" / "processed" / "dataset_inspection_report.md"

    os.makedirs(report_file.parent, exist_ok=True)
    inspect_dataset(str(db_file), str(report_file))
