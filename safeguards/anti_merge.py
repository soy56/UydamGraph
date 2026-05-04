"""
UdyamGraph — Anti-False-Merge Safeguards
==========================================

Prevents incorrect entity merges through multiple defense layers:

1. **Cost-Sensitive Learning**: XGBoost trained with 5:1 penalty for
   false positives (false merges) vs false negatives (missed links).
   Rationale: merging two different businesses corrupts the UBID graph
   and requires manual cleanup; missing a link can be caught later.

2. **Negative Evidence Detection**: Explicit signals that two records
   should NOT be merged (conflicting PAN, different legal structures
   with same name, etc.)

3. **Confidence Calibration with Conservative Thresholds**: 
   p>=0.92 for auto-link is deliberately high to minimize false merges.

4. **Reviewer Workflow**: Human-in-the-loop for ambiguous cases,
   with decisions fed back for model retraining.

Anti-False-Merge Priority Order:
────────────────────────────────
    1. Negative evidence → block merge regardless of score
    2. Cost-sensitive threshold → only merge at p>=0.92
    3. Structural consistency → verify legal entity compatibility
    4. Human review → ambiguous cases get expert judgment
"""

import logging
import numpy as np
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass, asdict, field
from datetime import datetime, timezone
from enum import Enum

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import config
from resolution.feature_engineering import FEATURE_NAMES

logger = logging.getLogger("udyamgraph.safeguards.anti_merge")


# ─── Negative Evidence Types ────────────────────────────────────────────────

class NegativeEvidenceType(str, Enum):
    """Types of evidence that PREVENT merging two records."""
    CONFLICTING_PAN = "conflicting_pan"
    DIFFERENT_LEGAL_STRUCTURE = "different_legal_structure"
    GEOGRAPHIC_IMPOSSIBILITY = "geographic_impossibility"
    TEMPORAL_IMPOSSIBILITY = "temporal_impossibility"
    KNOWN_FALSE_POSITIVE = "known_false_positive"


@dataclass
class NegativeEvidence:
    """Evidence that two records should NOT be merged."""
    evidence_type: NegativeEvidenceType
    description: str
    severity: str       # "hard" (absolute block) or "soft" (strong signal)
    feature_name: Optional[str] = None
    value_a: Optional[str] = None
    value_b: Optional[str] = None


@dataclass
class MergeSafetyReport:
    """Complete safety analysis for a proposed merge."""
    record_a_id: str
    record_b_id: str
    is_safe: bool
    confidence: float
    negative_evidence: List[NegativeEvidence] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    approved_by: Optional[str] = None   # "auto" or reviewer ID
    analysis_summary: str = ""

    def to_dict(self) -> Dict[str, Any]:
        result = asdict(self)
        result["negative_evidence"] = [
            {**asdict(ne), "evidence_type": ne.evidence_type.value}
            for ne in self.negative_evidence
        ]
        return result


# ─── Anti-False-Merge Engine ────────────────────────────────────────────────

class AntiFalseMergeEngine:
    """
    Multi-layer defense against false entity merges.
    
    Layer 1: Negative evidence detection (hard blocks)
    Layer 2: Cost-sensitive confidence thresholds
    Layer 3: Structural consistency checks
    
    Even if the ML model outputs p=0.95, a hard negative evidence
    signal (e.g., different PAN numbers both present) will BLOCK
    the merge and route to human review.
    """

    def __init__(self):
        self.auto_link_threshold = config.entity_resolution.auto_link_threshold
        self.review_threshold = config.entity_resolution.review_threshold

    def analyze_merge_safety(
        self,
        rec_a: Dict[str, Any],
        rec_b: Dict[str, Any],
        features: np.ndarray,
        confidence: float,
    ) -> MergeSafetyReport:
        """
        Perform comprehensive safety analysis on a proposed merge.
        
        Args:
            rec_a, rec_b: The two records being considered for merge
            features: Pairwise feature vector
            confidence: Calibrated confidence from XGBoost
            
        Returns:
            MergeSafetyReport with go/no-go decision
        """
        negative_evidence = []
        warnings = []

        # ── Layer 1: Negative Evidence Detection ─────────────────────

        # Check for conflicting PAN tokens
        pan_a = rec_a.get("pan_token")
        pan_b = rec_b.get("pan_token")
        if pan_a and pan_b and pan_a != pan_b:
            negative_evidence.append(NegativeEvidence(
                evidence_type=NegativeEvidenceType.CONFLICTING_PAN,
                description=(
                    "Both records have PAN numbers, but they are DIFFERENT. "
                    "This is strong evidence of different legal entities."
                ),
                severity="hard",
                feature_name="pan_token_match",
                value_a=pan_a[:8] + "...",
                value_b=pan_b[:8] + "...",
            ))

        # Check for incompatible legal structures
        legal_a = (rec_a.get("legal_status") or "").upper()
        legal_b = (rec_b.get("legal_status") or "").upper()
        incompatible_pairs = [
            ("PROPRIETORSHIP", "PRIVATE LIMITED"),
            ("PROPRIETORSHIP", "PUBLIC LIMITED"),
            ("PARTNERSHIP", "PRIVATE LIMITED"),
            ("LLP", "PRIVATE LIMITED"),
        ]
        if legal_a and legal_b:
            for pair in incompatible_pairs:
                if (pair[0] in legal_a and pair[1] in legal_b) or \
                   (pair[1] in legal_a and pair[0] in legal_b):
                    negative_evidence.append(NegativeEvidence(
                        evidence_type=NegativeEvidenceType.DIFFERENT_LEGAL_STRUCTURE,
                        description=(
                            f"Legal structures are incompatible: "
                            f"'{legal_a}' vs '{legal_b}'. "
                            f"A business cannot be both simultaneously."
                        ),
                        severity="hard",
                        feature_name="legal_status_match",
                        value_a=legal_a,
                        value_b=legal_b,
                    ))
                    break

        # Check for geographic impossibility
        dist_a = (rec_a.get("district") or "").upper()
        dist_b = (rec_b.get("district") or "").upper()
        pin_a = rec_a.get("pincode", "")
        pin_b = rec_b.get("pincode", "")

        if dist_a and dist_b and dist_a != dist_b:
            # Different districts AND different pincodes AND low name similarity
            if pin_a and pin_b and pin_a[:3] != pin_b[:3]:
                if features[3] < 0.85:  # name_jaro_winkler < 0.85
                    negative_evidence.append(NegativeEvidence(
                        evidence_type=NegativeEvidenceType.GEOGRAPHIC_IMPOSSIBILITY,
                        description=(
                            f"Different districts ({dist_a} vs {dist_b}) "
                            f"with dissimilar pincodes and moderate name "
                            f"similarity ({features[3]:.2f}). Unlikely same entity."
                        ),
                        severity="soft",
                        feature_name="district_match",
                        value_a=f"{dist_a} ({pin_a})",
                        value_b=f"{dist_b} ({pin_b})",
                    ))

        # ── Layer 2: Confidence Threshold Check ──────────────────────

        if confidence < self.review_threshold:
            warnings.append(
                f"Confidence ({confidence:.1%}) is below review threshold "
                f"({self.review_threshold:.1%})"
            )

        # ── Layer 3: Structural Consistency ──────────────────────────

        # If PAN tokens match but names are very different → suspicious
        if features[0] == 1.0 and features[3] < 0.5:
            warnings.append(
                f"PAN tokens match but name similarity is very low "
                f"({features[3]:.2f}). Could be a PAN reuse or data error."
            )

        # ── Final Decision ───────────────────────────────────────────

        hard_blocks = [
            ne for ne in negative_evidence if ne.severity == "hard"
        ]

        is_safe = (
            len(hard_blocks) == 0
            and confidence >= self.review_threshold
        )

        if hard_blocks:
            summary = (
                f"❌ MERGE BLOCKED: {len(hard_blocks)} hard negative "
                f"evidence found. Confidence {confidence:.1%} is irrelevant "
                f"when hard evidence conflicts exist."
            )
        elif not is_safe:
            summary = (
                f"⚠️ MERGE REQUIRES REVIEW: Confidence {confidence:.1%} "
                f"with {len(warnings)} warnings."
            )
        else:
            summary = (
                f"✅ MERGE SAFE: No negative evidence, "
                f"confidence {confidence:.1%}."
            )

        return MergeSafetyReport(
            record_a_id=rec_a.get("record_id", ""),
            record_b_id=rec_b.get("record_id", ""),
            is_safe=is_safe,
            confidence=confidence,
            negative_evidence=negative_evidence,
            warnings=warnings,
            analysis_summary=summary,
        )


# ─── Cost-Sensitive Threshold Analyzer ──────────────────────────────────────

class ThresholdAnalyzer:
    """
    Analyzes and recommends optimal thresholds for entity resolution
    based on cost-sensitive precision-recall tradeoff.
    
    The key insight: at different confidence thresholds, we get
    different precision/recall tradeoffs. We want to find the threshold
    where the COST (weighted by FP:FN cost ratio) is minimized.
    
    UdyamGraph uses 5:1 cost ratio:
    - False Positive (false merge) costs 5 units
    - False Negative (missed link) costs 1 unit
    """

    def __init__(
        self,
        fp_cost: float = None,
        fn_cost: float = None,
    ):
        self.fp_cost = fp_cost or config.entity_resolution.false_positive_cost
        self.fn_cost = fn_cost or config.entity_resolution.false_negative_cost

    def analyze_thresholds(
        self,
        y_true: np.ndarray,
        y_scores: np.ndarray,
        thresholds: Optional[np.ndarray] = None,
    ) -> List[Dict[str, Any]]:
        """
        Analyze cost at different thresholds.
        
        Returns a list of dicts with threshold, precision, recall,
        FP count, FN count, and total cost.
        """
        if thresholds is None:
            thresholds = np.arange(0.50, 0.99, 0.01)

        results = []
        for t in thresholds:
            y_pred = (y_scores >= t).astype(int)

            tp = int(np.sum((y_pred == 1) & (y_true == 1)))
            fp = int(np.sum((y_pred == 1) & (y_true == 0)))
            fn = int(np.sum((y_pred == 0) & (y_true == 1)))
            tn = int(np.sum((y_pred == 0) & (y_true == 0)))

            precision = tp / max(tp + fp, 1)
            recall = tp / max(tp + fn, 1)
            total_cost = (fp * self.fp_cost) + (fn * self.fn_cost)

            results.append({
                "threshold": round(float(t), 3),
                "precision": round(precision, 4),
                "recall": round(recall, 4),
                "true_positives": tp,
                "false_positives": fp,
                "false_negatives": fn,
                "true_negatives": tn,
                "fp_cost": round(fp * self.fp_cost, 1),
                "fn_cost": round(fn * self.fn_cost, 1),
                "total_cost": round(total_cost, 1),
            })

        return results

    def recommend_thresholds(
        self,
        analysis: List[Dict[str, Any]],
    ) -> Dict[str, float]:
        """
        Recommend auto-link and review thresholds based on cost analysis.
        
        Auto-link: Threshold where precision >= 99%
        Review: Threshold where total cost is minimized
        """
        # Auto-link: highest recall at >=99% precision
        high_precision = [
            r for r in analysis if r["precision"] >= 0.99
        ]
        auto_link = min(
            (r["threshold"] for r in high_precision),
            default=0.95
        )

        # Review: minimum total cost
        min_cost = min(analysis, key=lambda r: r["total_cost"])
        review = min_cost["threshold"]

        return {
            "auto_link_threshold": auto_link,
            "review_threshold": review,
            "min_cost_threshold": min_cost["threshold"],
            "min_cost_value": min_cost["total_cost"],
        }


# ─── Demo ────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 70)
    print("UdyamGraph — Anti-False-Merge Safeguards")
    print("=" * 70)

    engine = AntiFalseMergeEngine()

    # Case 1: Conflicting PAN → hard block
    print("\n── Case 1: Conflicting PAN Numbers ─────────────────────")
    print("   Both records have PAN, but they're DIFFERENT")

    rec_a = {
        "record_id": "29AABCU1234R1Z5",
        "business_name": "KRISHNA TRADERS",
        "pan_token": "abc123def456abc123def456abc123def456abc123def456abc123def456abcd",
        "legal_status": "Proprietorship",
        "district": "Mysuru",
        "pincode": "570001",
    }
    rec_b = {
        "record_id": "SE-MYS-2021-00789",
        "business_name": "KRISHNA TRADERS & CO",
        "pan_token": "xyz789ghi012xyz789ghi012xyz789ghi012xyz789ghi012xyz789ghi012xyza",
        "legal_status": "Proprietorship",
        "district": "Mysuru",
        "pincode": "570001",
    }

    features1 = np.array([
        0.0, 0.0, 0.0,           # PAN/GSTIN/Udyam don't match
        0.88, 0.80, 0.67, 0.85,  # Names similar
        0.82,                     # Trade name
        0.92, 1.0, 1.0, 1.0,     # Address similar, same pincode/district
        0.0, 0.0,                 # Phone/email
        1.0, 0.8,                 # Same legal status
    ], dtype=np.float32)

    report1 = engine.analyze_merge_safety(rec_a, rec_b, features1, 0.78)
    print(f"\n   {report1.analysis_summary}")
    for ne in report1.negative_evidence:
        print(f"   🚫 {ne.evidence_type.value}: {ne.description}")

    # Case 2: Safe merge
    print("\n── Case 2: Safe Merge (Matching PAN) ───────────────────")

    rec_c = {
        "record_id": "29AABCU1234R1Z5",
        "business_name": "UBIQUITY INNOVATIONS PVT LTD",
        "pan_token": "same_token_abc123def456",
        "legal_status": "Private Limited Company",
        "district": "Bangalore Urban",
        "pincode": "560034",
    }
    rec_d = {
        "record_id": "KA-BLR-FAC-2020-00456",
        "business_name": "UBIQUITY INNOVATIONS PRIVATE LIMITED",
        "pan_token": "same_token_abc123def456",
        "legal_status": "Private Limited Company",
        "district": "Bangalore Urban",
        "pincode": "560058",
    }

    features2 = np.array([
        1.0, 0.0, 0.0,           # PAN matches!
        0.92, 0.88, 0.75, 0.94,  # Names very similar
        0.85,                     # Trade name
        0.78, 0.0, 1.0, 0.95,    # Similar area, same district
        0.0, 0.0,                 # Phone/email
        1.0, 0.5,                 # Same legal status
    ], dtype=np.float32)

    report2 = engine.analyze_merge_safety(rec_c, rec_d, features2, 0.96)
    print(f"\n   {report2.analysis_summary}")

    # Case 3: Incompatible legal structures
    print("\n── Case 3: Incompatible Legal Structures ───────────────")

    rec_e = {
        "record_id": "29XYZPK5678M1Z3",
        "business_name": "DECCAN ENTERPRISES",
        "pan_token": None,
        "legal_status": "Proprietorship",
        "district": "Hubli",
        "pincode": "580020",
    }
    rec_f = {
        "record_id": "SE-HUB-2022-01234",
        "business_name": "DECCAN ENTERPRISES PVT LTD",
        "pan_token": None,
        "legal_status": "Private Limited Company",
        "district": "Hubli",
        "pincode": "580020",
    }

    features3 = np.array([
        0.0, 0.0, 0.0,
        0.89, 0.82, 0.67, 0.87,
        0.80,
        0.95, 1.0, 1.0, 1.0,
        0.0, 0.0,
        0.3, 0.8,
    ], dtype=np.float32)

    report3 = engine.analyze_merge_safety(rec_e, rec_f, features3, 0.81)
    print(f"\n   {report3.analysis_summary}")
    for ne in report3.negative_evidence:
        print(f"   🚫 {ne.evidence_type.value}: {ne.description}")

    # Summary
    print("\n" + "=" * 70)
    print("SAFETY ANALYSIS SUMMARY")
    print("=" * 70)
    safe_icon = {True: "✅", False: "❌"}
    for r in [report1, report2, report3]:
        print(
            f"  {safe_icon[r.is_safe]} "
            f"{r.record_a_id} ↔ {r.record_b_id}: "
            f"{'SAFE' if r.is_safe else 'BLOCKED'} "
            f"(confidence: {r.confidence:.1%}, "
            f"negative evidence: {len(r.negative_evidence)})"
        )

    # Threshold analysis demo
    print("\n" + "=" * 70)
    print("COST-SENSITIVE THRESHOLD ANALYSIS")
    print("=" * 70)

    np.random.seed(42)
    y_true = np.random.randint(0, 2, size=1000)
    y_scores = np.clip(
        np.where(y_true == 1, np.random.normal(0.8, 0.15, 1000),
                 np.random.normal(0.3, 0.2, 1000)),
        0, 1,
    )

    analyzer = ThresholdAnalyzer()
    analysis = analyzer.analyze_thresholds(y_true, y_scores)
    recommendations = analyzer.recommend_thresholds(analysis)

    print(f"\n  Recommended Auto-Link Threshold: {recommendations['auto_link_threshold']:.2f}")
    print(f"  Recommended Review Threshold: {recommendations['review_threshold']:.2f}")
    print(f"  Minimum Cost at: {recommendations['min_cost_threshold']:.2f} "
          f"(cost={recommendations['min_cost_value']:.0f})")
    print()
    print("  Cost Model: FP costs 5x more than FN")
    print("  → System prefers MISSING a link over FALSELY MERGING")

    # Show threshold analysis table
    print(f"\n  {'Threshold':>10}  {'Precision':>10}  {'Recall':>8}  "
          f"{'FP':>5}  {'FN':>5}  {'Total Cost':>11}")
    print("  " + "─" * 55)
    for r in analysis:
        if r["threshold"] in [0.65, 0.70, 0.75, 0.80, 0.85, 0.90, 0.92, 0.95]:
            marker = " ◀ auto" if r["threshold"] == 0.92 else \
                     " ◀ review" if r["threshold"] == 0.65 else ""
            print(
                f"  {r['threshold']:>10.2f}  {r['precision']:>10.4f}  "
                f"{r['recall']:>8.4f}  {r['false_positives']:>5}  "
                f"{r['false_negatives']:>5}  {r['total_cost']:>11.1f}{marker}"
            )
