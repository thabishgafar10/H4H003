"""
LLM-assisted extractor adapter.
Provides provider-neutral interface (OpenAI, Anthropic, local/Ollama) for nuanced extraction.
Operates strictly as a candidate proposer; candidates must still pass EvidenceValidator.
Gracefully disabled when LLM_API_KEY is not configured.
"""

import os
import json
import logging
from typing import List, Optional
import httpx
from pydantic import BaseModel, Field

from app.models.issue_comment import IssueComment
from app.services.extractors.base import (
    CandidatePayload,
    EvidencePayload,
    ALL_EVENT_TYPES,
    compute_candidate_fingerprint,
)

logger = logging.getLogger(__name__)


class LLMExtractedEvent(BaseModel):
    event_type: str
    subject: str
    description: Optional[str] = None
    confidence_band: str = "MEDIUM"  # HIGH, MEDIUM, LOW
    evidence_excerpt: str
    reasoning: Optional[str] = None


class LLMExtractor:
    VERSION = "1.0.0"

    def __init__(
        self,
        api_key: Optional[str] = None,
        api_base: Optional[str] = None,
        model: Optional[str] = None,
        dataset_name: str = "KafkaSQLite",
    ):
        self.api_key = api_key or os.getenv("LLM_API_KEY")
        self.api_base = api_base or os.getenv("LLM_API_BASE", "https://api.openai.com/v1")
        self.model = model or os.getenv("LLM_MODEL", "gpt-4o-mini")
        self.dataset_name = dataset_name

    def is_available(self) -> bool:
        """Returns True only if an API key is configured."""
        return bool(self.api_key and self.api_key.strip())

    def extract_from_comment(self, comment: IssueComment) -> List[CandidatePayload]:
        """
        Extract nuanced candidates from comment text.
        If LLM is unconfigured, returns an empty list cleanly without error.
        """
        if not self.is_available():
            return []

        raw_msg = comment.message or ""
        if not raw_msg.strip():
            return []

        try:
            parsed_events = self._call_llm_provider(raw_msg, comment.source_issue_id)
            candidates: List[CandidatePayload] = []

            for idx, ev in enumerate(parsed_events):
                if ev.event_type not in ALL_EVENT_TYPES:
                    continue

                # Ensure excerpt is within the message
                excerpt = ev.evidence_excerpt.strip()
                if excerpt not in raw_msg:
                    continue

                fingerprint = compute_candidate_fingerprint(
                    self.dataset_name,
                    "issue_comments",
                    str(comment.id),
                    ev.event_type,
                    disambiguator=f"llm_{idx}",
                )

                candidates.append(
                    CandidatePayload(
                        dataset_name=self.dataset_name,
                        event_type=ev.event_type,
                        event_time=comment.created_at,
                        subject=ev.subject,
                        description=ev.description or ev.reasoning,
                        actor=comment.display_name or comment.username,
                        extractor_type="LLM",
                        extractor_version=self.VERSION,
                        confidence_band=ev.confidence_band,
                        validation_status="ACCEPTED" if ev.confidence_band in {"HIGH", "MEDIUM"} else "NEEDS_REVIEW",
                        candidate_fingerprint=fingerprint,
                        issue_id=comment.source_issue_id,
                        evidence_items=[
                            EvidencePayload(
                                source_record_id=comment.source_record_id,
                                evidence_excerpt=excerpt,
                                evidence_role="PRIMARY",
                            )
                        ],
                    )
                )

            return candidates
        except Exception as e:
            logger.warning(f"LLM extraction encountered non-fatal error: {e}")
            return []

    def _call_llm_provider(self, text: str, issue_id: str) -> List[LLMExtractedEvent]:
        """
        Calls OpenAI/Ollama/compatible chat completions endpoint with structured JSON schema.
        Mockable in tests.
        """
        if not self.is_available():
            return []

        url = f"{self.api_base.rstrip('/')}/chat/completions"
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        prompt = (
            "You are an expert software product historian analyzing development records. "
            "Extract product evolution events from the comment below using this taxonomy:\n"
            "- FEATURE_PROPOSED: new feature or KIP proposed\n"
            "- DECISION_APPROVED: explicit approval (+1, LGTM, vote passed)\n"
            "- CONCERN_RAISED: blocker, objection, or compatibility concern raised\n"
            "- DECISION_REVERSED: prior decision or vote reversed/withdrawn\n\n"
            "CRITICAL: The evidence_excerpt field MUST be an EXACT VERBATIM substring of the comment.\n"
            "Return JSON in the format: {\"events\": [{\"event_type\": \"...\", \"subject\": \"...\", \"confidence_band\": \"HIGH|MEDIUM|LOW\", \"evidence_excerpt\": \"...\", \"reasoning\": \"...\"}]}"
        )
        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": prompt},
                {"role": "user", "content": f"Issue: {issue_id}\n\nComment:\n{text}"},
            ],
            "temperature": 0.1,
            "response_format": {"type": "json_object"},
        }

        try:
            with httpx.Client(timeout=15.0) as client:
                res = client.post(url, headers=headers, json=payload)
                if res.status_code != 200:
                    logger.warning(f"LLM provider returned status {res.status_code}: {res.text[:200]}")
                    return []
                data = res.json()
                content = data["choices"][0]["message"]["content"]
                parsed = json.loads(content)
                events_list = parsed.get("events", [])
                return [LLMExtractedEvent(**item) for item in events_list if isinstance(item, dict)]
        except Exception as e:
            logger.warning(f"Failed to query LLM provider: {e}")
            return []
