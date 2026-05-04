"""
UdyamGraph — SHAP Explanation Generator
==========================================

Generates human-readable, auditable explanations for every entity
resolution decision using SHAP (SHapley Additive exPlanations).

Each linkage decision is accompanied by:
1. Per-feature SHAP values showing contribution to match/non-match
2. A natural language summary of the top contributing features
3. A serializable evidence trail stored in Neo4j for audit

This satisfies regulatory requirements for explainable AI and enables
human reviewers to understand and validate automated decisions.
"""

import logging
import json
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, asdict, field
from datetime import datetime, timezone

import numpy as np
import shap

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from resolution.feature_engineering import FEATURE_NAMES

logger = logging.getLogger("udyamgraph.resolution.explainer")


# ─── Human-Readable Feature Labels ──────────────────────────────────────────

FEATURE_LABELS = {
    "pan_token_match": "PAN Number Match",
    "gstin_token_match": "GSTIN Match",
    "udyam_token_match": "Udyam Registration Match",
    "name_jaro_winkler": "Business Name Similarity (Jaro-Winkler)",
    "name_levenshtein_norm": "Business Name Similarity (Edit Distance)",
    "name_token_overlap": "Business Name Word Overlap",
    "name_sbert_cosine": "Business Name Semantic Similarity (AI)",
    "trade_name_jaro_winkler": "Trade Name Similarity",
    "address_sbert_cosine": "Address Semantic Similarity (AI)",
    "pincode_match": "PIN Code Match",
    "district_match": "District Match",
    "city_jaro_winkler": "City Name Similarity",
    "phone_match": "Phone Number Match",
    "email_match": "Email Address Match",
    "legal_status_match": "Legal Structure Match (Pvt Ltd/LLP/etc.)",
    "business_type_match": "Business Type Match (Mfg/Trading/Services)",
}


@dataclass
class FeatureContribution:
    """A single feature's contribution to the linkage decision."""
    feature_name: str
    feature_label: str
    feature_value: float
    shap_value: float
    direction: str         # "supports_match" or "supports_non_match"
    strength: str          # "strong", "moderate", "weak"
    explanation: str       # Human-readable sentence

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class LinkageExplanation:
    """
    Complete explanation for a single entity resolution decision.
    Stored as evidence in the Neo4j graph for audit trails.
    """
    pair_id: str
    record_a_id: str
    record_b_id: str
    decision: str                                 # auto_link / review / reject
    confidence: float
    base_value: float                             # SHAP base (average prediction)
    contributions: List[FeatureContribution]       # All feature contributions
    summary: str                                  # Natural language summary
    top_positive_factors: List[str]               # Top factors favoring match
    top_negative_factors: List[str]               # Top factors against match
    generated_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    def to_dict(self) -> Dict[str, Any]:
        result = asdict(self)
        result["contributions"] = [c.to_dict() for c in self.contributions]
        return result

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2)


# ─── Explanation Generator ───────────────────────────────────────────────────

class ExplanationGenerator:
    """
    Generates comprehensive, human-readable explanations for
    entity resolution decisions using SHAP feature attributions.
    
    Example output:
        "✅ AUTO-LINKED with 96.3% confidence.
         These records likely represent the same business because:
         • PAN Number Match: Both records share the same PAN (impact: +0.34)
         • Business Name Semantic Similarity: Names are 94% similar (impact: +0.18)
         • District Match: Both in Bangalore Urban (impact: +0.08)
         Minor factors against match:
         • Address Similarity: Addresses differ somewhat (impact: -0.04)"
    """

    def __init__(self, explainer: shap.TreeExplainer):
        self.explainer = explainer

    def explain(
        self,
        features: np.ndarray,
        record_a_id: str,
        record_b_id: str,
        decision: str,
        confidence: float,
    ) -> LinkageExplanation:
        """
        Generate a full explanation for a linkage decision.
        
        Args:
            features: Feature vector (NUM_FEATURES,)
            record_a_id: ID of first record
            record_b_id: ID of second record
            decision: "auto_link", "review", or "reject"
            confidence: Calibrated probability
            
        Returns:
            LinkageExplanation with human-readable attribution
        """
        X = features.reshape(1, -1)

        # Get SHAP values
        shap_values = self.explainer.shap_values(X)
        if isinstance(shap_values, list):
            shap_vals = shap_values[1][0]  # Positive class
        else:
            shap_vals = shap_values[0]

        base_value = float(self.explainer.expected_value)
        if isinstance(self.explainer.expected_value, np.ndarray):
            base_value = float(self.explainer.expected_value[1])

        # Build contributions
        contributions = []
        for i, fname in enumerate(FEATURE_NAMES):
            sv = float(shap_vals[i])
            fv = float(features[i])

            direction = "supports_match" if sv > 0 else "supports_non_match"

            abs_sv = abs(sv)
            if abs_sv > 0.15:
                strength = "strong"
            elif abs_sv > 0.05:
                strength = "moderate"
            else:
                strength = "weak"

            explanation = self._feature_explanation(fname, fv, sv)

            contributions.append(FeatureContribution(
                feature_name=fname,
                feature_label=FEATURE_LABELS.get(fname, fname),
                feature_value=fv,
                shap_value=sv,
                direction=direction,
                strength=strength,
                explanation=explanation,
            ))

        # Sort by absolute SHAP value
        contributions.sort(key=lambda c: abs(c.shap_value), reverse=True)

        # Top positive and negative factors
        top_positive = [
            c.explanation for c in contributions
            if c.direction == "supports_match" and c.strength != "weak"
        ][:3]

        top_negative = [
            c.explanation for c in contributions
            if c.direction == "supports_non_match" and c.strength != "weak"
        ][:3]

        # Generate natural language summary
        summary = self._generate_summary(
            decision, confidence, top_positive, top_negative
        )

        pair_id = f"{record_a_id}___{record_b_id}"

        return LinkageExplanation(
            pair_id=pair_id,
            record_a_id=record_a_id,
            record_b_id=record_b_id,
            decision=decision,
            confidence=confidence,
            base_value=base_value,
            contributions=contributions,
            summary=summary,
            top_positive_factors=top_positive,
            top_negative_factors=top_negative,
        )

    def _feature_explanation(
        self, feature_name: str, value: float, shap_value: float
    ) -> str:
        """Generate human-readable explanation for a single feature."""
        label = FEATURE_LABELS.get(feature_name, feature_name)
        impact = f"impact: {shap_value:+.3f}"

        if feature_name == "pan_token_match":
            if value > 0.5:
                return f"Both records share the same PAN number ({impact})"
            else:
                return f"PAN numbers do not match or are unavailable ({impact})"

        elif feature_name == "gstin_token_match":
            if value > 0.5:
                return f"Both records have the same GSTIN ({impact})"
            else:
                return f"GSTINs differ or are unavailable ({impact})"

        elif "jaro_winkler" in feature_name or "cosine" in feature_name:
            pct = f"{value:.0%}"
            if value > 0.85:
                return f"{label}: Very high similarity ({pct}) ({impact})"
            elif value > 0.65:
                return f"{label}: Moderate similarity ({pct}) ({impact})"
            else:
                return f"{label}: Low similarity ({pct}) ({impact})"

        elif feature_name in ("pincode_match", "district_match",
                              "phone_match", "email_match",
                              "legal_status_match", "business_type_match"):
            if value > 0.5:
                return f"{label}: Match confirmed ({impact})"
            else:
                return f"{label}: No match ({impact})"

        else:
            return f"{label}: value={value:.3f} ({impact})"

    def _generate_summary(
        self,
        decision: str,
        confidence: float,
        positive_factors: List[str],
        negative_factors: List[str],
    ) -> str:
        """Generate a complete natural language summary."""
        lines = []

        # Decision header
        if decision == "auto_link":
            lines.append(
                f"✅ AUTO-LINKED with {confidence:.1%} confidence."
            )
            lines.append(
                "These records likely represent the same business because:"
            )
        elif decision == "review":
            lines.append(
                f"🔍 SENT FOR HUMAN REVIEW with {confidence:.1%} confidence."
            )
            lines.append(
                "Evidence is mixed — human judgment needed. Key factors:"
            )
        else:
            lines.append(
                f"❌ REJECTED as non-match with {confidence:.1%} confidence."
            )
            lines.append(
                "These records likely represent different businesses because:"
            )

        # Positive factors
        if positive_factors:
            for factor in positive_factors:
                lines.append(f"  • {factor}")

        # Negative factors
        if negative_factors:
            if decision == "auto_link":
                lines.append("Minor factors against match:")
            else:
                lines.append("Factors against match:")
            for factor in negative_factors:
                lines.append(f"  • {factor}")

        return "\n".join(lines)


# ─── CLI Demo ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 70)
    print("UdyamGraph — SHAP Explanation Generator")
    print("=" * 70)
    print()
    print("This module generates human-readable explanations for every")
    print("entity resolution decision, showing which features contributed")
    print("most to the match/non-match conclusion.")
    print()
    print("Example explanation structure:")
    print()
    print("  ✅ AUTO-LINKED with 96.3% confidence.")
    print("  These records likely represent the same business because:")
    print("    • Both records share the same PAN number (impact: +0.34)")
    print("    • Business Name Semantic Similarity: Very high (94%) (impact: +0.18)")
    print("    • District Match: Match confirmed (impact: +0.08)")
    print("  Minor factors against match:")
    print("    • Address Similarity: Moderate similarity (78%) (impact: -0.04)")
    print()
    print("Each explanation is stored as a LinkEvidence node in Neo4j")
    print("for complete audit trail and regulatory compliance.")
