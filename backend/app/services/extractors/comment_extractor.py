"""
Deterministic extractor for IssueComment records.
Extracts: DECISION_APPROVED, CONCERN_RAISED, DECISION_REVERSED, FEATURE_PROPOSED.
Includes strict negation handling, conditional guards, bot filtering, and quote stripping.
"""

import re
from typing import List, Optional, Set, Tuple
from app.models.issue_comment import IssueComment
from app.services.extractors.base import (
    CandidatePayload,
    EvidencePayload,
    EVENT_DECISION_APPROVED,
    EVENT_CONCERN_RAISED,
    EVENT_DECISION_REVERSED,
    EVENT_FEATURE_PROPOSED,
    compute_candidate_fingerprint,
)

BOT_USERNAMES: Set[str] = {
    "githubbot",
    "asfbot",
    "jira-bot",
    "jira",
    "apache-ci",
    "hudson",
    "jenkins",
    "buildbot",
    "sonarcloud",
    "codecov",
}

# Negation patterns that disqualify approval or indicate negative stance
RE_NEGATION = re.compile(
    r"\b(not\s+(?:approved|ready|accepted|good|lgtm|convinced)|cannot\s+approve|never\s+approve|"
    r"do\s+not\s+merge|won't\s+fix|will\s+not\s+approve|disapprove|veto|reject)\b",
    re.IGNORECASE,
)

# Conditional patterns where approval/action is hypothetical rather than actual
RE_CONDITIONAL = re.compile(
    r"\b(if\s+(?:we|you|this\s+is|it\s+is)\s+approved|would\s+approve\s+if|pending\s+approval|"
    r"once\s+approved|subject\s+to\s+approval|conditional\s+on\s+approval|only\s+if)\b",
    re.IGNORECASE,
)

# Decision approved patterns
RE_APPROVAL_STRONG = re.compile(
    r"(\+1\s*(?:\(binding\))?|\bLGTM\b|\blooks\s+good\s+to\s+me\b|\bvote\s+passed\b|\bthe\s+vote\s+passes\b)",
    re.IGNORECASE,
)
RE_APPROVAL_MODERATE = re.compile(
    r"\b(approved\s+(?:the\s+patch|the\s+pr|by)|approving\s+this|I\s+approve\b|\+1\b)",
    re.IGNORECASE,
)

# Concern raised patterns
RE_CONCERN_STRONG = re.compile(
    r"(-1\s*(?:\(binding\))?|\bblocker\b|\bstrongly\s+object\b|\bbreaks?\s+(?:backward[s]?\s+)?compatibility\b)",
    re.IGNORECASE,
)
RE_CONCERN_MODERATE = re.compile(
    r"\b(raise(?:s|d)?\s+(?:a\s+)?concern|disagree\s+with|serious\s+flaw|security\s+vulnerability|not\s+ready\s+for\s+merge)\b",
    re.IGNORECASE,
)

# Decision reversed patterns
RE_REVERSED = re.compile(
    r"\b(revert(?:ing|ed)?\s+(?:the\s+decision|the\s+vote|the\s+commit|the\s+patch)|withdrawing\s+(?:the\s+)?(?:proposal|kip|vote)|"
    r"reopen(?:ing|ed)?\s+(?:the\s+)?issue|cancel(?:l)?(?:ing|ed)?\s+(?:the\s+)?vote)\b",
    re.IGNORECASE,
)

# Feature proposed patterns
RE_PROPOSED = re.compile(
    r"\b(I\s+propose\b|we\s+propose\b|proposing\s+to\b|new\s+KIP\b|KIP-\d+\s+proposal\b|RFC:\s+)",
    re.IGNORECASE,
)


def strip_quotes_and_email_trails(text: str) -> str:
    """Removes quoted text, JIRA quote macros, and email reply trails."""
    # Remove {quote}...{quote} blocks
    cleaned = re.sub(r"\{quote\}[\s\S]*?\{quote\}", " ", text)
    
    # Process line-by-line
    kept_lines = []
    for line in cleaned.splitlines():
        line_stripped = line.strip()
        # Omit Markdown blockquotes, email quote markers, or separator lines
        if line_stripped.startswith(">") or line_stripped.startswith("|"):
            continue
        if re.match(r"^On\s+.+wrote:$", line_stripped, re.IGNORECASE):
            continue
        if line_stripped.startswith("-----Original Message-----"):
            break
        kept_lines.append(line)
    return "\n".join(kept_lines)


def find_verbatim_sentence(full_text: str, match_text: str) -> str:
    """Finds the verbatim sentence or surrounding clause in full_text containing match_text."""
    pos = full_text.find(match_text)
    if pos == -1:
        return match_text
    # Expand to sentence boundaries (e.g. . ! ? or line breaks)
    start = max(0, full_text.rfind("\n", 0, pos) + 1)
    period_start = full_text.rfind(". ", 0, pos)
    if period_start != -1 and period_start + 2 > start:
        start = period_start + 2

    end = len(full_text)
    newline_end = full_text.find("\n", pos)
    if newline_end != -1:
        end = min(end, newline_end)
    period_end = full_text.find(". ", pos)
    if period_end != -1 and period_end < end:
        end = min(end, period_end + 1)

    excerpt = full_text[start:end].strip()
    return excerpt if excerpt else match_text


class CommentExtractor:
    VERSION = "1.0.0"

    def __init__(self, dataset_name: str = "KafkaSQLite"):
        self.dataset_name = dataset_name

    def extract(self, comment: IssueComment) -> List[CandidatePayload]:
        candidates: List[CandidatePayload] = []
        raw_msg = comment.message or ""
        if not raw_msg.strip():
            return candidates

        # 1. Bot check
        username = (comment.username or "").lower().strip()
        display_name = (comment.display_name or "").lower().strip()
        if any(bot in username or bot in display_name for bot in BOT_USERNAMES):
            return candidates

        # 2. Strip quotes for pattern detection (preserves verbatim matching via raw_msg)
        detectable_text = strip_quotes_and_email_trails(raw_msg)
        if not detectable_text.strip():
            return candidates

        issue_id = comment.source_issue_id
        src_rec_id = comment.source_record_id
        comment_id_str = str(comment.id)
        actor = comment.display_name or comment.username

        # 3. Check DECISION_REVERSED
        rev_match = RE_REVERSED.search(detectable_text)
        if rev_match:
            excerpt = find_verbatim_sentence(raw_msg, rev_match.group(0))
            candidates.append(
                CandidatePayload(
                    dataset_name=self.dataset_name,
                    event_type=EVENT_DECISION_REVERSED,
                    event_time=comment.created_at,
                    subject=f"Decision reversed on [{issue_id}] by {actor or 'unknown'}",
                    description=excerpt,
                    actor=actor,
                    extractor_type="DETERMINISTIC",
                    extractor_version=self.VERSION,
                    confidence_band="HIGH",
                    validation_status="ACCEPTED",
                    candidate_fingerprint=compute_candidate_fingerprint(
                        self.dataset_name, "issue_comments", comment_id_str, EVENT_DECISION_REVERSED
                    ),
                    issue_id=issue_id,
                    evidence_items=[
                        EvidencePayload(
                            source_record_id=src_rec_id,
                            evidence_excerpt=excerpt,
                            evidence_role="PRIMARY",
                        )
                    ],
                )
            )

        # 4. Check CONCERN_RAISED
        concern_strong = RE_CONCERN_STRONG.search(detectable_text)
        concern_mod = RE_CONCERN_MODERATE.search(detectable_text)
        negation_match = RE_NEGATION.search(detectable_text)

        concern_match = concern_strong or concern_mod or (negation_match if ("approve" in (negation_match.group(0) if negation_match else "").lower() or "merge" in (negation_match.group(0) if negation_match else "").lower()) else None)
        if concern_match:
            excerpt = find_verbatim_sentence(raw_msg, concern_match.group(0))
            confidence = "HIGH" if (concern_strong or "-1" in concern_match.group(0)) else "MEDIUM"
            candidates.append(
                CandidatePayload(
                    dataset_name=self.dataset_name,
                    event_type=EVENT_CONCERN_RAISED,
                    event_time=comment.created_at,
                    subject=f"Concern raised on [{issue_id}] by {actor or 'unknown'}",
                    description=excerpt,
                    actor=actor,
                    extractor_type="DETERMINISTIC",
                    extractor_version=self.VERSION,
                    confidence_band=confidence,
                    validation_status="ACCEPTED",
                    candidate_fingerprint=compute_candidate_fingerprint(
                        self.dataset_name, "issue_comments", comment_id_str, EVENT_CONCERN_RAISED
                    ),
                    issue_id=issue_id,
                    evidence_items=[
                        EvidencePayload(
                            source_record_id=src_rec_id,
                            evidence_excerpt=excerpt,
                            evidence_role="PRIMARY",
                        )
                    ],
                )
            )

        # 5. Check DECISION_APPROVED (ONLY if NO negation and NO disqualifying concern)
        app_strong = RE_APPROVAL_STRONG.search(detectable_text)
        app_mod = RE_APPROVAL_MODERATE.search(detectable_text)
        app_match = app_strong or app_mod

        if app_match:
            excerpt = find_verbatim_sentence(raw_msg, app_match.group(0))
            # Guard against negation
            has_negation = bool(RE_NEGATION.search(detectable_text))
            # Guard against conditional
            has_conditional = bool(RE_CONDITIONAL.search(detectable_text))

            if has_negation:
                # Do not accept approval when negated
                pass
            elif has_conditional:
                # Conditional approval -> mark as NEEDS_REVIEW
                candidates.append(
                    CandidatePayload(
                        dataset_name=self.dataset_name,
                        event_type=EVENT_DECISION_APPROVED,
                        event_time=comment.created_at,
                        subject=f"Conditional approval flagged on [{issue_id}] by {actor or 'unknown'}",
                        description=excerpt,
                        actor=actor,
                        extractor_type="DETERMINISTIC",
                        extractor_version=self.VERSION,
                        confidence_band="LOW",
                        validation_status="NEEDS_REVIEW",
                        rejection_reason="Conditional statement detected; requires manual review",
                        candidate_fingerprint=compute_candidate_fingerprint(
                            self.dataset_name, "issue_comments", comment_id_str, EVENT_DECISION_APPROVED
                        ),
                        issue_id=issue_id,
                        evidence_items=[
                            EvidencePayload(
                                source_record_id=src_rec_id,
                                evidence_excerpt=excerpt,
                                evidence_role="SUPPORTING",
                            )
                        ],
                    )
                )
            else:
                confidence = "HIGH" if (app_strong or "(binding)" in app_match.group(0)) else "MEDIUM"
                candidates.append(
                    CandidatePayload(
                        dataset_name=self.dataset_name,
                        event_type=EVENT_DECISION_APPROVED,
                        event_time=comment.created_at,
                        subject=f"Decision approved on [{issue_id}] by {actor or 'unknown'}",
                        description=excerpt,
                        actor=actor,
                        extractor_type="DETERMINISTIC",
                        extractor_version=self.VERSION,
                        confidence_band=confidence,
                        validation_status="ACCEPTED",
                        candidate_fingerprint=compute_candidate_fingerprint(
                            self.dataset_name, "issue_comments", comment_id_str, EVENT_DECISION_APPROVED
                        ),
                        issue_id=issue_id,
                        evidence_items=[
                            EvidencePayload(
                                source_record_id=src_rec_id,
                                evidence_excerpt=excerpt,
                                evidence_role="PRIMARY",
                            )
                        ],
                    )
                )

        # 6. Check FEATURE_PROPOSED in comment
        prop_match = RE_PROPOSED.search(detectable_text)
        if prop_match:
            excerpt = find_verbatim_sentence(raw_msg, prop_match.group(0))
            candidates.append(
                CandidatePayload(
                    dataset_name=self.dataset_name,
                    event_type=EVENT_FEATURE_PROPOSED,
                    event_time=comment.created_at,
                    subject=f"Feature proposed in comment on [{issue_id}] by {actor or 'unknown'}",
                    description=excerpt,
                    actor=actor,
                    extractor_type="DETERMINISTIC",
                    extractor_version=self.VERSION,
                    confidence_band="MEDIUM",
                    validation_status="ACCEPTED",
                    candidate_fingerprint=compute_candidate_fingerprint(
                        self.dataset_name, "issue_comments", comment_id_str, EVENT_FEATURE_PROPOSED
                    ),
                    issue_id=issue_id,
                    evidence_items=[
                        EvidencePayload(
                            source_record_id=src_rec_id,
                            evidence_excerpt=excerpt,
                            evidence_role="PRIMARY",
                        )
                    ],
                )
            )

        return candidates
