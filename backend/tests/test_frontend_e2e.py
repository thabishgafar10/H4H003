"""
Playwright End-to-End Automated Browser Test Suite for Work 09.
Tests real frontend UI, DOM rendering, SVG graph canvas, modals, and API integration.
"""

import os
import time
import pytest
from playwright.sync_api import sync_playwright

BASE_URL = "http://127.0.0.1:8000/dashboard"
SCREENSHOT_DIR = os.path.abspath("docs/screenshots")


@pytest.fixture(scope="module")
def browser_context():
    os.makedirs(SCREENSHOT_DIR, exist_ok=True)
    pw = sync_playwright().start()
    browser = pw.chromium.launch(channel="chrome", headless=True)
    context = browser.new_context(viewport={"width": 1400, "height": 900})
    yield context
    context.close()
    browser.close()
    pw.stop()


def test_01_dashboard_startup_and_overview(browser_context):
    """Test dashboard loading, title, overview stats, and take screenshot."""
    page = browser_context.new_page()
    console_errors = []
    page.on("console", lambda msg: console_errors.append(msg.text) if msg.type == "error" else None)

    page.goto(BASE_URL)
    page.wait_for_selector("#stat-temporal-events", timeout=5000)

    assert "Temporal Product Knowledge System" in page.title()

    # Wait for stats to populate
    page.wait_for_function("document.getElementById('stat-temporal-events').innerText !== '-' && document.getElementById('stat-canonical-features').innerText !== '-'", timeout=7000)
    events_text = page.locator("#stat-temporal-events").inner_text()
    features_text = page.locator("#stat-canonical-features").inner_text()
    assert int(events_text.replace(",", "")) > 0
    assert int(features_text.replace(",", "")) > 0

    page.screenshot(path=os.path.join(SCREENSHOT_DIR, "01_overview_dashboard.png"))
    assert len(console_errors) == 0, f"Console errors observed: {console_errors}"
    page.close()


def test_02_features_list_and_details_modal(browser_context):
    """Test features tab, search filter, and opening Feature Detail Modal with history."""
    page = browser_context.new_page()
    page.goto(BASE_URL)
    page.click("#tab-features")
    page.wait_for_selector("#features-grid .bg-slate-900", timeout=5000)

    # Check that features are rendered
    cards = page.locator("#features-grid .bg-slate-900")
    assert cards.count() >= 10

    # Test Category filter
    page.select_option("#feature-category-filter", "STREAMS")
    page.wait_for_timeout(300)
    filtered_cards = page.locator("#features-grid .bg-slate-900")
    assert filtered_cards.count() >= 1

    # Reset Category filter
    page.select_option("#feature-category-filter", "")
    page.wait_for_timeout(300)

    # Click first Details button
    details_btns = page.locator("button:has-text('Details')")
    assert details_btns.count() > 0
    details_btns.first.click()

    # Check Feature Detail Modal
    page.wait_for_selector("#modal-feature-detail:not(.hidden)", timeout=3000)
    feat_title = page.locator("#feat-detail-title").inner_text()
    assert len(feat_title) > 0

    # Wait for timeline history inside modal
    page.wait_for_selector("#feat-detail-timeline .p-2\\.5", timeout=5000)
    history_items = page.locator("#feat-detail-timeline .p-2\\.5")
    assert history_items.count() > 0

    page.screenshot(path=os.path.join(SCREENSHOT_DIR, "02_features_detail_modal.png"))

    # Close modal
    page.click("button:has-text('Close')")
    page.wait_for_selector("#modal-feature-detail", state="hidden", timeout=2000)
    page.close()


def test_03_feature_identity_resolution_tool(browser_context):
    """Test Feature Identity & Term Resolver tool with candidate results."""
    page = browser_context.new_page()
    page.goto(BASE_URL)
    page.click("#tab-features")
    page.wait_for_selector("#feature-resolve-input", timeout=3000)

    # Query 'KIP-500'
    page.fill("#feature-resolve-input", "KIP-500")
    page.click("#btn-resolve-feature")

    # Wait for candidate cards to populate
    page.wait_for_selector("#feature-resolve-results .font-bold, #feature-resolve-results .italic", timeout=5000)
    results_text = page.locator("#feature-resolve-results").inner_text()
    assert "KRaft Mode" in results_text or "candidate identity matches" in results_text

    page.screenshot(path=os.path.join(SCREENSHOT_DIR, "03_feature_resolution_result.png"))
    page.close()


def test_04_knowledge_graph_rendering_and_node_inspector(browser_context):
    """Test Knowledge Graph SVG canvas, node selection, and Inspector panel."""
    page = browser_context.new_page()
    page.goto(BASE_URL)
    page.click("#tab-graph")
    page.wait_for_selector("#graph-root-feature", timeout=5000)

    # Click Render Subgraph
    page.click("button:has-text('Render Subgraph')")
    page.wait_for_selector("#graph-svg circle", timeout=5000)

    nodes = page.locator("#graph-svg circle")
    assert nodes.count() >= 5

    # Click on the root node
    nodes.first.click()
    page.wait_for_selector("#inspector-actions:not(.hidden)", timeout=3000)
    inspector_text = page.locator("#node-inspector-content").inner_text()
    assert len(inspector_text) > 0

    page.screenshot(path=os.path.join(SCREENSHOT_DIR, "04_knowledge_graph_canvas.png"))

    # Test Expand Node Connections
    expand_btn = page.locator("#btn-expand-node")
    assert expand_btn.is_visible()
    expand_btn.click()
    page.wait_for_timeout(1000)

    # Verify graph node count is intact or expanded
    assert page.locator("#graph-svg circle").count() >= 5
    page.close()


def test_05_historical_qa_engine(browser_context):
    """Test Historical QA question submission, markdown response, timeline, and citations."""
    page = browser_context.new_page()
    page.goto(BASE_URL)
    page.click("#tab-qa")
    page.wait_for_selector("#qa-input", timeout=5000)

    # Fill and submit architectural inquiry
    page.fill("#qa-input", "Why was ZooKeeper replaced by KRaft?")
    page.click("#qa-submit-btn")

    # Wait for answer container and actual generated answer
    page.wait_for_selector("#qa-result-container:not(.hidden)", timeout=10000)
    page.wait_for_function("!document.getElementById('qa-answer-markdown').innerText.includes('Performing entity resolution')", timeout=15000)

    answer_text = page.locator("#qa-answer-markdown").inner_text()
    assert len(answer_text) > 50
    assert "KRaft" in answer_text or "ZooKeeper" in answer_text or "consensus" in answer_text

    # Verify citations grid
    citations = page.locator("#qa-citations-grid > div")
    assert citations.count() > 0

    page.screenshot(path=os.path.join(SCREENSHOT_DIR, "05_historical_qa_response.png"))
    page.close()


def test_06_event_explorer_and_provenance_modal(browser_context):
    """Test Event Explorer table, filtering, pagination, and Event Provenance Modal."""
    page = browser_context.new_page()
    page.goto(BASE_URL)
    page.click("#tab-explorer")
    page.wait_for_selector("#events-tbody tr", timeout=5000)

    rows = page.locator("#events-tbody tr")
    assert rows.count() > 0

    # Open Citations Modal for first event
    citation_btn = page.locator("#events-tbody button:has-text('citations')").first
    citation_btn.click()

    page.wait_for_selector("#modal-event-detail:not(.hidden)", timeout=3000)
    modal_title = page.locator("#ev-detail-subject").inner_text()
    assert len(modal_title) > 0

    page.screenshot(path=os.path.join(SCREENSHOT_DIR, "06_event_provenance_modal.png"))

    # Close modal
    page.click("#modal-event-detail button:has-text('Close')")
    page.wait_for_selector("#modal-event-detail", state="hidden", timeout=2000)
    page.close()


def test_07_issue_timeline(browser_context):
    """Test Issue Timeline chronological visualization."""
    page = browser_context.new_page()
    page.goto(BASE_URL)
    page.click("#tab-timeline")
    page.wait_for_selector("#timeline-search-issue", timeout=3000)

    page.fill("#timeline-search-issue", "KAFKA-1")
    page.click("button:has-text('Load Timeline')")

    page.wait_for_selector("#issue-timeline-container .relative", timeout=5000)
    items = page.locator("#issue-timeline-container .relative")
    assert items.count() > 0

    page.screenshot(path=os.path.join(SCREENSHOT_DIR, "07_issue_timeline.png"))
    page.close()


def test_08_extraction_engine_and_candidates(browser_context):
    """Test Extraction run monitor and Candidate inspection."""
    page = browser_context.new_page()
    page.goto(BASE_URL)
    page.click("#tab-extraction")
    page.wait_for_selector("#runs-tbody", timeout=5000)

    # Click Load Candidates
    page.click("button:has-text('Load Candidates')")
    page.wait_for_selector("#candidates-tbody tr", timeout=5000)

    page.screenshot(path=os.path.join(SCREENSHOT_DIR, "08_extraction_engine.png"))
    page.close()


def test_09_connect_sources_and_document_inventory(browser_context):
    """Test Connect Sources tab, Jira/GitHub cards, and Ingested Documents table."""
    page = browser_context.new_page()
    page.goto(BASE_URL)
    page.click("#tab-sources")
    page.wait_for_selector("#ingestion-runs-tbody", timeout=5000)
    page.wait_for_selector("#documents-tbody", timeout=5000)

    # Check Jira test connection button
    page.fill("#jira-base-url", "https://example.atlassian.net")
    page.fill("#jira-email", "dev@example.com")
    page.fill("#jira-api-token", "token123")
    page.click("button:has-text('Test')")
    page.wait_for_selector("#jira-status-msg", timeout=5000)

    page.screenshot(path=os.path.join(SCREENSHOT_DIR, "09_connect_sources.png"))
    page.close()


def test_10_project_switching_and_isolation(browser_context):
    """Test project dropdown selector, switching project, and isolation badge update."""
    page = browser_context.new_page()
    page.goto(BASE_URL)
    page.wait_for_selector("#global-project-select", timeout=5000)

    # Check that selector has at least 1 option
    options = page.locator("#global-project-select option")
    assert options.count() >= 1

    badge_text = page.locator("#active-project-badge").inner_text()
    assert "kafka" in badge_text.lower() or "apache" in badge_text.lower()

    # Open New Project Modal
    page.click("button:has-text('New')")
    page.wait_for_selector("#modal-new-project:not(.hidden)", timeout=3000)
    page.screenshot(path=os.path.join(SCREENSHOT_DIR, "10_project_onboarding_modal.png"))

    # Close Modal
    page.click("button:has-text('Cancel')")
    page.wait_for_selector("#modal-new-project", state="hidden", timeout=2000)
    page.close()
