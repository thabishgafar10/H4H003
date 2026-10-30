# WORK 09: FRONTEND ARCHITECTURE & COMPONENT AUDIT

**Project:** Temporal Product Knowledge System  
**Branch:** `feature/data-ingestion`  
**Date:** 2026-10-31  
**Author:** Senior Full-Stack Engineer, Frontend Architect, QA Lead  

---

## 1. Executive Architecture Overview

The Temporal Product Knowledge System frontend is implemented as a high-density, responsive Single Page Application (SPA) served directly from the FastAPI backend at `/dashboard` and `/app` via `backend/app/static/index.html`.

### Technology Stack
- **Framework:** Vanilla Modern JavaScript (ES6+), HTML5, DOM API.
- **Styling:** Tailwind CSS (via official script CDN) configured with dark mode, custom slate backgrounds (`#0b0f19`, `#0f172a`, `#1e293b`), and custom brand color palettes.
- **Iconography:** FontAwesome 6 (CDN).
- **Markdown Rendering:** marked.js 15+ (CDN) with GFM table, blockquote, and code syntax support.
- **Graph Visualization:** Pure Vector SVG engine (`<svg id="graph-svg">`) with dynamic geometry, color coding, and node inspection.
- **State Management:** In-memory reactive state (`currentProjectId`, `currentProjectSlug`, `projectsList`, `allFeaturesCache`, `currentEventsPage`).
- **Backend Communication:** Asynchronous Fetch API querying FastAPI REST endpoints under `/api`.

---

## 2. Infrastructure & Environment Audit

### 2.1 Stale Server Discovery & Reproduction
- **Reproduced Failure:** All requests to `/api/features`, `/api/graph`, `/api/qa`, `/api/projects`, and `/api/sources` returned `404 Not Found`.
- **Root Cause:** A stale background python process (PID 25896) launched earlier in development was still bound to port 8000, running an outdated `version: 0.2.0` FastAPI instance from Work 02 that lacked routers for Work 04-08.
- **Remediation:** Process 25896 was terminated and restarted with current codebase `version: 0.7.0` mounting all routers.

### 2.2 Global Multi-Project Context Deficiency
- **Reproduced Failure:** Switching projects in the header dropdown (`#global-project-select`) failed to update the Features list, Knowledge Graph controls, QA context, Events Explorer, or Extraction runs.
- **Root Cause:** API fetch calls hardcoded `dataset_name=KafkaSQLite` and omitted `project_id=${currentProjectId}`. When switching to a non-Kafka project (e.g. `fintech-core` or `healthtech`), the UI continued displaying Kafka data or broke with 404s.
- **Remediation:** Centralize project state; pass `project_id` and project-scoped URLs (`/api/projects/{slug}/...` or `project_id=${currentProjectId}`) across all 8 navigation tabs.

---

## 3. Feature-by-Feature Component & API Dependency Inventory

### 3.1 Global & Header
| Property | Detail |
| :--- | :--- |
| **Component** | Header (`#global-project-select`, `#modal-new-project`) |
| **API Endpoints** | `GET /api/projects`, `POST /api/projects` |
| **HTTP Method** | GET, POST |
| **Parameters** | POST body: `{ name, slug, domain, description }` |
| **Expected Response** | List of `ProjectResponse` / Single `ProjectResponse` |
| **Status** | **PARTIAL** |
| **Reproduced Defect** | Project selection does not persist across browser reload; switching projects leaves child tabs in stale states. |
| **Root Cause** | Missing `localStorage` persistence and incomplete event dispatching in `onProjectChange()`. |
| **Required Fix** | Store `currentProjectId` in `localStorage`; trigger refresh of all 8 tabs on project change. |

---

### 3.2 Overview Tab (`#view-overview`)
| Property | Detail |
| :--- | :--- |
| **Component** | Overview Statistics & Taxonomy Grid |
| **API Endpoints** | `GET /api/extraction/statistics`, `GET /api/graph/stats` |
| **HTTP Method** | GET |
| **Parameters** | `dataset_name=KafkaSQLite` (hardcoded) |
| **Expected Response** | `{ total_temporal_events, total_evidence_links, events_by_type: {...} }`, `{ canonical_features, total_graph_elements }` |
| **Status** | **PARTIAL** |
| **Reproduced Defect** | Statistics only show Kafka benchmark dataset regardless of selected project. |
| **Root Cause** | Queries lack `project_id` parameter. |
| **Required Fix** | Update queries to `GET /api/statistics?project_id=${currentProjectId}` and `GET /api/graph/stats?project_id=${currentProjectId}`. |

---

### 3.3 Features Explorer Tab (`#view-features`)
| Property | Detail |
| :--- | :--- |
| **Component** | Features Grid, Search, Filter, Detail Modal |
| **API Endpoints** | `GET /api/features`, `GET /api/features/{id}`, `GET /api/features/{id}/history`, `POST /api/features/resolve`, `POST /api/features/seed` |
| **HTTP Method** | GET, POST |
| **Parameters** | `project_id`, `dataset_name`, `category`, `status`, `q`, `feature_id` |
| **Expected Response** | List of `FeatureResponse`, `FeatureResponse`, List of `TemporalEventResponse`, `FeatureResolveResponse` |
| **Status** | **DEFECTIVE / NONFUNCTIONAL** |
| **Reproduced Defect** | 1. No feature detail view exists; clicking a feature card only opens the graph.<br>2. Cannot inspect linked issues, commits, or chronological history.<br>3. Search only filters client-side memory instead of resolving terms/KIPs via backend.<br>4. Seeding button alerts `undefined` because it reads `data.seeded_features` instead of `data.features_seeded`.<br>5. Ignores active `project_id`. |
| **Root Cause** | Frontend UI was only a stub rendering cards without modal/drawer integration or correct response parsing. |
| **Required Fix** | 1. Add interactive Feature Detail Modal showing canonical properties, aliases, linked issues/commits, and full temporal event history.<br>2. Add backend Feature Resolution search bar (`POST /api/features/resolve`).<br>3. Fix seeding response key references.<br>4. Scope requests with `project_id=${currentProjectId}`. |

---

### 3.4 Knowledge Graph Tab (`#view-graph`)
| Property | Detail |
| :--- | :--- |
| **Component** | SVG Graph Canvas, Node Inspector, Depth/Node Limit Selectors |
| **API Endpoints** | `GET /api/graph/features/{id}`, `GET /api/graph/nodes/{type}/{id}`, `POST /api/graph/query` |
| **HTTP Method** | GET, POST |
| **Parameters** | `feature_id`, `depth`, `max_nodes`, `project_id`, `dataset_name`, `start_time`, `end_time` |
| **Expected Response** | `GraphResponse: { nodes: [...], edges: [...], node_count, edge_count }` |
| **Status** | **DEFECTIVE / NONFUNCTIONAL** |
| **Reproduced Defect** | 1. Only supports Feature seed nodes; cannot query with Issue Key or Commit Hash.<br>2. Clicking a node does not allow expanding it (no "Expand Node" action).<br>3. Node styling omits `ALIAS` node types, rendering them as amber events.<br>4. Graph visualization uses crude radial layout where 50+ nodes overlap with invisible edge labels.<br>5. Ignores active project selection (hardcoded `KafkaSQLite`). |
| **Root Cause** | Naive SVG layout with no layout physics/force or hierarchical clustering; missing node expansion API integration; unscoped project parameters. |
| **Required Fix** | 1. Implement interactive seed selector (`FEATURE`, `ISSUE`, `COMMIT`).<br>2. Implement Node Expansion: clicking "Expand Node" calls `GET /api/graph/nodes/{type}/{id}` and merges subgraph dynamically.<br>3. Improve SVG rendering with collision avoidance, node dragging, distinct type colors (including ALIAS), and directional arrowheads.<br>4. Bind to `project_id=${currentProjectId}`. |

---

### 3.5 Historical QA Tab (`#view-qa`)
| Property | Detail |
| :--- | :--- |
| **Component** | QA Input, Benchmark Pills, Answer Markdown, Timeline, Citations Grid |
| **API Endpoints** | `GET /api/qa/sample-questions`, `POST /api/qa/ask` |
| **HTTP Method** | GET, POST |
| **Parameters** | POST body: `{ query, project_id, dataset_name, include_graph }` |
| **Expected Response** | `QAResponse: { query, answer, canonical_feature, confidence, synthesis_mode, temporal_timeline, citations }` |
| **Status** | **PARTIAL** |
| **Reproduced Defect** | QA questions always query `KafkaSQLite` even when another project is active. |
| **Root Cause** | Payload hardcodes `dataset_name: 'KafkaSQLite'` and omits `project_id`. |
| **Required Fix** | Pass `project_id: currentProjectId` in POST body; show project context tag; handle empty evidence gracefully. |

---

### 3.6 Event Explorer Tab (`#view-explorer`)
| Property | Detail |
| :--- | :--- |
| **Component** | Event Table, Filters, Pagination, Event Detail Modal |
| **API Endpoints** | `GET /api/events`, `GET /api/events/{id}` |
| **HTTP Method** | GET |
| **Parameters** | `skip`, `limit`, `event_type`, `issue_id`, `actor`, `project_id`, `dataset_name` |
| **Expected Response** | List of `TemporalEventResponse` |
| **Status** | **PARTIAL** |
| **Reproduced Defect** | 1. Hardcodes `KafkaSQLite`.<br>2. Table lacks "View Details" modal to inspect verbatim evidence excerpts, SHA-256 hashes, and raw properties. |
| **Root Cause** | Query parameter missing `project_id`; missing detail modal DOM elements. |
| **Required Fix** | Pass `project_id`; implement Event Detail Modal displaying full evidence provenance. |

---

### 3.7 Issue Timeline Tab (`#view-timeline`)
| Property | Detail |
| :--- | :--- |
| **Component** | Issue Key Search, Chronological Vertical Timeline |
| **API Endpoints** | `GET /api/events/issues/{id}/timeline` |
| **HTTP Method** | GET |
| **Parameters** | `issue_id`, `project_id` |
| **Expected Response** | `IssueTimelineResponse: { issue_id, summary, status, total_events, timeline: [...] }` |
| **Status** | **PARTIAL** |
| **Reproduced Defect** | Does not pass project ID or handle foreign issue isolation errors. |
| **Root Cause** | API call does not supply `project_id`. |
| **Required Fix** | Append `?project_id=${currentProjectId}`; display meaningful 404 error when issue is not found in the project. |

---

### 3.8 Extraction Engine Tab (`#view-extraction`)
| Property | Detail |
| :--- | :--- |
| **Component** | Extraction Trigger Form, Run History Table, Candidate Inspector |
| **API Endpoints** | `POST /api/extraction/run`, `GET /api/extraction/runs`, `GET /api/extraction/candidates` |
| **HTTP Method** | POST, GET |
| **Parameters** | POST body: `{ dataset_name, extraction_mode, chunk_size, max_issues, project_id }`, GET `project_id` |
| **Expected Response** | `ExtractionRunResponse`, List of `ExtractionRunResponse`, List of `EventCandidateResponse` |
| **Status** | **PARTIAL** |
| **Reproduced Defect** | 1. Hardcodes `KafkaSQLite`.<br>2. Candidate records are not accessible in the UI. |
| **Root Cause** | Form does not pass `project_id`; missing Candidate review tab/table. |
| **Required Fix** | Pass `project_id`; add Candidates viewer table to review accepted/rejected extraction candidates. |

---

### 3.9 Connect Sources & Ingestion Tab (`#view-sources`)
| Property | Detail |
| :--- | :--- |
| **Component** | Jira Card, GitHub Card, Document Upload Card, Ingestion Monitor Table, Documents Viewer |
| **API Endpoints** | `POST /api/projects/{id}/sources/jira`, `POST .../sources/jira/sync`, `POST .../sources/github`, `POST .../sources/github/sync`, `POST .../documents/upload`, `POST .../documents/url`, `GET /api/projects/{id}/runs`, `GET /api/projects/{id}/documents` |
| **HTTP Method** | GET, POST |
| **Parameters** | `project_id`, multipart/form-data, JSON credentials |
| **Expected Response** | Ingestion run schemas, document schemas |
| **Status** | **PARTIAL** |
| **Reproduced Defect** | Users can upload documents, but there is no list or table showing ingested documents for the project. |
| **Root Cause** | Missing Document inventory table in the UI. |
| **Required Fix** | Add Document Management table displaying filename, page count, SHA-256 hash, and ingestion status. |

---

## 4. Remediation Plan

1. **Phase 2A (Features):**
   - Add Feature Detail Modal with canonical metadata, aliases, linked issues/commits, and chronological event history.
   - Add Feature Resolution search tool.
   - Fix seeding alert keys (`features_seeded`, `links_established`).
   - Scope all feature calls to `currentProjectId`.
2. **Phase 2B (Knowledge Graph):**
   - Support seed selection across `FEATURE`, `ISSUE`, and `COMMIT`.
   - Add interactive "Expand Node" action on node click.
   - Add node limit (25, 50, 100) and depth controls.
   - Color code `ALIAS` nodes distinctly (violet/indigo).
   - Improve layout algorithm with force simulation or clustered levels.
   - Scope graph queries to `currentProjectId`.
3. **Phase 2C (QA, Explorer, Timeline, Extraction):**
   - Connect QA, Event Explorer, Timeline, and Extraction to active `currentProjectId`.
   - Add Event Detail Modal with full verbatim evidence excerpts and SHA-256 hashes.
   - Add Extraction Candidate inspection table.
4. **Phase 3 (Sources & Ingestion):**
   - Add Document Inventory table displaying uploaded documents.
   - Add Ingestion run status auto-refresh and error detail modals.
5. **Phase 4 & 5 (Verification & Testing):**
   - Write API contract validation document.
   - Build automated Playwright/Puppeteer browser test suite executing all tabs, interactions, and screenshots.
   - Run backend regression suite (156 tests).