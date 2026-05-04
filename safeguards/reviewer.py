"""
UdyamGraph — Human Reviewer Workflow
=======================================

Manages the human-in-the-loop review process for ambiguous entity
resolution decisions (confidence 0.65–0.92).

Key design principles:
1. **Active Learning**: Prioritize the most informative cases for review
   (cases near decision boundary teach the model the most)
2. **Decision Capture**: Every human decision is stored with metadata
   for model retraining (reviewer ID, rationale, time spent)
3. **Feedback Loop**: Periodic retraining incorporates reviewer decisions
   as ground truth labels, improving the model over time
4. **Audit Trail**: Complete record of who reviewed what, when, and why

Review Process:
    ML classifier → 0.65 ≤ p < 0.92 → Review Queue
                                          ↓
                                    Reviewer Dashboard
                                    (Evidence + SHAP explanation)
                                          ↓
                                    ┌─────────────┐
                                    │ APPROVE (→ merge)     │
                                    │ REJECT  (→ no merge)  │
                                    │ DEFER   (→ re-queue)  │
                                    │ ESCALATE(→ senior)    │
                                    └─────────────┘
                                          ↓
                                    Feedback → Model Retraining
"""

import logging
import json
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, asdict, field
from datetime import datetime, timezone
from enum import Enum
from collections import defaultdict

import numpy as np

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import config

logger = logging.getLogger("udyamgraph.safeguards.reviewer")


# ─── Review Decision Types ──────────────────────────────────────────────────

class ReviewDecision(str, Enum):
    APPROVE = "approve"     # Records are the same entity → merge
    REJECT = "reject"       # Records are different entities → do not merge
    DEFER = "defer"         # Need more information → re-queue
    ESCALATE = "escalate"   # Complex case → senior reviewer


class ReviewPriority(str, Enum):
    CRITICAL = "critical"   # High impact, near boundary
    HIGH = "high"           # Important but less ambiguous
    NORMAL = "normal"       # Standard review
    LOW = "low"             # Can wait


@dataclass
class ReviewCase:
    """A single entity resolution case pending human review."""
    case_id: str
    record_a_id: str
    record_b_id: str
    department_a: str
    department_b: str
    business_name_a: str
    business_name_b: str
    confidence: float
    priority: ReviewPriority
    explanation_summary: str
    top_contributors: List[Dict[str, Any]]
    feature_values: Dict[str, float]
    shap_values: Dict[str, float]
    created_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    assigned_to: Optional[str] = None
    status: str = "pending"     # pending / in_review / completed

    def to_dict(self) -> Dict[str, Any]:
        result = asdict(self)
        result["priority"] = self.priority.value
        return result


@dataclass
class ReviewOutcome:
    """The result of a human reviewer's decision."""
    case_id: str
    reviewer_id: str
    decision: ReviewDecision
    rationale: str                  # Why the reviewer made this decision
    confidence_override: Optional[float] = None  # Reviewer's confidence
    time_spent_seconds: float = 0.0
    additional_notes: Optional[str] = None
    decided_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    # Training label derived from decision
    @property
    def training_label(self) -> Optional[int]:
        """Convert decision to training label: 1=match, 0=non-match."""
        if self.decision == ReviewDecision.APPROVE:
            return 1
        elif self.decision == ReviewDecision.REJECT:
            return 0
        return None  # DEFER/ESCALATE don't produce labels

    def to_dict(self) -> Dict[str, Any]:
        result = asdict(self)
        result["decision"] = self.decision.value
        result["training_label"] = self.training_label
        return result


# ─── Review Queue Manager ───────────────────────────────────────────────────

class ReviewQueueManager:
    """
    Manages the review queue with active learning prioritization.
    
    Active Learning Strategy:
    Cases closest to the decision boundary (p ≈ 0.78, midpoint of
    review range) are most informative for the model. These are
    assigned CRITICAL priority.
    
    Priority Assignment:
    • CRITICAL: 0.75 ≤ p < 0.85 (near boundary, most informative)
    • HIGH:     0.85 ≤ p < 0.92 (leaning match, confirm or deny)
    • NORMAL:   0.70 ≤ p < 0.75 (moderate uncertainty)
    • LOW:      0.65 ≤ p < 0.70 (leaning reject, lower priority)
    """

    def __init__(self):
        self.queue: List[ReviewCase] = []
        self.outcomes: List[ReviewOutcome] = []
        self.reviewer_stats: Dict[str, Dict[str, int]] = defaultdict(
            lambda: {"reviewed": 0, "approved": 0, "rejected": 0}
        )

    def assign_priority(self, confidence: float) -> ReviewPriority:
        """
        Assign review priority based on active learning informativeness.
        Cases near the decision boundary are most valuable for retraining.
        """
        if 0.75 <= confidence < 0.85:
            return ReviewPriority.CRITICAL  # Most informative
        elif 0.85 <= confidence < 0.92:
            return ReviewPriority.HIGH
        elif 0.70 <= confidence < 0.75:
            return ReviewPriority.NORMAL
        else:
            return ReviewPriority.LOW

    def enqueue(
        self,
        record_a_id: str,
        record_b_id: str,
        department_a: str,
        department_b: str,
        business_name_a: str,
        business_name_b: str,
        confidence: float,
        explanation_summary: str,
        top_contributors: List[Dict[str, Any]],
        feature_values: Dict[str, float],
        shap_values: Dict[str, float],
    ) -> ReviewCase:
        """Add a case to the review queue."""
        case_id = f"REV-{len(self.queue):06d}"
        priority = self.assign_priority(confidence)

        case = ReviewCase(
            case_id=case_id,
            record_a_id=record_a_id,
            record_b_id=record_b_id,
            department_a=department_a,
            department_b=department_b,
            business_name_a=business_name_a,
            business_name_b=business_name_b,
            confidence=confidence,
            priority=priority,
            explanation_summary=explanation_summary,
            top_contributors=top_contributors,
            feature_values=feature_values,
            shap_values=shap_values,
        )

        self.queue.append(case)
        logger.info(
            f"Queued review case {case_id}: {business_name_a} ↔ "
            f"{business_name_b} (priority: {priority.value}, "
            f"confidence: {confidence:.1%})"
        )

        return case

    def get_next_cases(
        self,
        reviewer_id: str,
        count: int = 10,
    ) -> List[ReviewCase]:
        """
        Get next cases for a reviewer, prioritized by active learning.
        CRITICAL cases first, then HIGH, NORMAL, LOW.
        """
        pending = [c for c in self.queue if c.status == "pending"]

        # Sort by priority (CRITICAL first), then by confidence
        # (most ambiguous first within same priority)
        priority_order = {
            ReviewPriority.CRITICAL: 0,
            ReviewPriority.HIGH: 1,
            ReviewPriority.NORMAL: 2,
            ReviewPriority.LOW: 3,
        }
        pending.sort(key=lambda c: (
            priority_order[c.priority],
            abs(c.confidence - 0.785),  # Distance from boundary midpoint
        ))

        batch = pending[:count]
        for case in batch:
            case.status = "in_review"
            case.assigned_to = reviewer_id

        return batch

    def submit_decision(self, outcome: ReviewOutcome) -> None:
        """Record a reviewer's decision and update statistics."""
        self.outcomes.append(outcome)

        # Update case status
        for case in self.queue:
            if case.case_id == outcome.case_id:
                case.status = "completed"
                break

        # Update reviewer stats
        stats = self.reviewer_stats[outcome.reviewer_id]
        stats["reviewed"] += 1
        if outcome.decision == ReviewDecision.APPROVE:
            stats["approved"] += 1
        elif outcome.decision == ReviewDecision.REJECT:
            stats["rejected"] += 1

        logger.info(
            f"Review {outcome.case_id}: {outcome.decision.value} "
            f"by {outcome.reviewer_id} "
            f"(rationale: {outcome.rationale[:50]}...)"
        )

    def get_training_data(self) -> List[Dict[str, Any]]:
        """
        Extract training data from completed reviews for model retraining.
        
        Only APPROVE and REJECT decisions produce training labels.
        DEFER and ESCALATE are excluded (no ground truth signal).
        
        Returns:
            List of dicts with 'features', 'label', and metadata
        """
        training_data = []

        for outcome in self.outcomes:
            if outcome.training_label is None:
                continue  # Skip DEFER/ESCALATE

            # Find the original case
            case = next(
                (c for c in self.queue if c.case_id == outcome.case_id),
                None
            )
            if not case:
                continue

            training_data.append({
                "case_id": outcome.case_id,
                "features": case.feature_values,
                "label": outcome.training_label,
                "reviewer_id": outcome.reviewer_id,
                "rationale": outcome.rationale,
                "original_confidence": case.confidence,
                "decided_at": outcome.decided_at,
            })

        return training_data

    def get_queue_statistics(self) -> Dict[str, Any]:
        """Get current queue statistics."""
        pending = [c for c in self.queue if c.status == "pending"]
        in_review = [c for c in self.queue if c.status == "in_review"]
        completed = [c for c in self.queue if c.status == "completed"]

        priority_counts = defaultdict(int)
        for c in pending:
            priority_counts[c.priority.value] += 1

        approval_rate = 0.0
        if self.outcomes:
            approvals = sum(
                1 for o in self.outcomes
                if o.decision == ReviewDecision.APPROVE
            )
            decisions = sum(
                1 for o in self.outcomes
                if o.decision in (ReviewDecision.APPROVE, ReviewDecision.REJECT)
            )
            approval_rate = approvals / max(decisions, 1)

        return {
            "total_cases": len(self.queue),
            "pending": len(pending),
            "in_review": len(in_review),
            "completed": len(completed),
            "priority_breakdown": dict(priority_counts),
            "total_outcomes": len(self.outcomes),
            "approval_rate": round(approval_rate, 3),
            "unique_reviewers": len(self.reviewer_stats),
            "reviewer_stats": dict(self.reviewer_stats),
        }


# ─── Demo ────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 70)
    print("UdyamGraph — Human Reviewer Workflow")
    print("=" * 70)

    queue = ReviewQueueManager()

    # Simulate enqueueing ambiguous cases
    cases_data = [
        {
            "record_a_id": "29AABCU1234R1Z5",
            "record_b_id": "SE-BLR-2021-78901",
            "department_a": "commercial_taxes",
            "department_b": "shops_establishments",
            "business_name_a": "KRISHNA TRADING COMPANY",
            "business_name_b": "KRISHNA TRADERS",
            "confidence": 0.79,
        },
        {
            "record_a_id": "KA-MYS-FAC-2019-00123",
            "record_b_id": "29BDHPK5678L1Z3",
            "department_a": "factories_board",
            "department_b": "commercial_taxes",
            "business_name_a": "DECCAN TEXTILES MILL",
            "business_name_b": "DECCAN TEXTILE INDUSTRIES",
            "confidence": 0.87,
        },
        {
            "record_a_id": "SE-HUB-2022-00789",
            "record_b_id": "29CDRFH9012G1Z8",
            "department_a": "shops_establishments",
            "department_b": "commercial_taxes",
            "business_name_a": "DHARWAD PHARMA",
            "business_name_b": "DHARWAD PHARMACEUTICALS PVT LTD",
            "confidence": 0.72,
        },
    ]

    print("\n📥 Enqueueing cases for review:")
    for case in cases_data:
        c = queue.enqueue(
            **case,
            explanation_summary=f"Ambiguous match at {case['confidence']:.1%}",
            top_contributors=[{"feature": "name_similarity", "value": 0.85}],
            feature_values={"name_jaro_winkler": 0.85},
            shap_values={"name_jaro_winkler": 0.12},
        )
        print(
            f"   {c.case_id}: {c.business_name_a} ↔ {c.business_name_b} "
            f"[{c.priority.value}]"
        )

    # Simulate reviewer workflow
    print("\n👤 Reviewer 'analyst_001' starts reviewing:")
    batch = queue.get_next_cases("analyst_001", count=3)

    for case in batch:
        print(f"\n   Reviewing {case.case_id}:")
        print(f"   {case.business_name_a} ({case.department_a})")
        print(f"   ↔ {case.business_name_b} ({case.department_b})")
        print(f"   Confidence: {case.confidence:.1%}")
        print(f"   Priority: {case.priority.value}")

    # Simulate decisions
    decisions = [
        ReviewOutcome(
            case_id="REV-000000",
            reviewer_id="analyst_001",
            decision=ReviewDecision.APPROVE,
            rationale="Same business, different registrations. Trade name matches.",
            time_spent_seconds=45.0,
        ),
        ReviewOutcome(
            case_id="REV-000001",
            reviewer_id="analyst_001",
            decision=ReviewDecision.APPROVE,
            rationale="Factory and GST belong to same entity. Name variation is normal.",
            time_spent_seconds=30.0,
        ),
        ReviewOutcome(
            case_id="REV-000002",
            reviewer_id="analyst_001",
            decision=ReviewDecision.REJECT,
            rationale="Different entities. 'Dharwad Pharma' is a proprietorship, "
                     "'Dharwad Pharmaceuticals' is a Pvt Ltd.",
            time_spent_seconds=60.0,
        ),
    ]

    print("\n\n📝 Submitting reviewer decisions:")
    for d in decisions:
        queue.submit_decision(d)
        icon = {"approve": "✅", "reject": "❌", "defer": "🔄", "escalate": "⬆️"}
        print(
            f"   {icon[d.decision.value]} {d.case_id}: "
            f"{d.decision.value} — {d.rationale[:60]}..."
        )

    # Show training data
    print("\n\n🎓 Training Data Extracted for Model Retraining:")
    training_data = queue.get_training_data()
    for td in training_data:
        print(
            f"   {td['case_id']}: label={td['label']} "
            f"(original confidence: {td['original_confidence']:.1%}, "
            f"reviewer: {td['reviewer_id']})"
        )

    # Queue statistics
    print("\n📊 Queue Statistics:")
    stats = queue.get_queue_statistics()
    print(f"   Total cases: {stats['total_cases']}")
    print(f"   Completed: {stats['completed']}")
    print(f"   Approval rate: {stats['approval_rate']:.1%}")
    print(f"   Unique reviewers: {stats['unique_reviewers']}")
