"""
UdyamGraph — XGBoost Entity Resolution Classifier
====================================================

Pairwise classifier that determines whether two business records
refer to the same real-world entity. Uses XGBoost with:

1. **Cost-sensitive learning**: 5:1 penalty for false positives
   (false merges are far more damaging than missed links)

2. **Confidence calibration**: Platt scaling for well-calibrated
   probabilities enabling three-tier routing:
   • p ≥ 0.92 → Auto-link (high confidence)
   • 0.65 ≤ p < 0.92 → Human reviewer queue
   • p < 0.65 → Reject

3. **Explainability**: Every decision includes SHAP feature
   attributions showing WHY the model linked/rejected a pair.
"""

import logging
import json
import numpy as np
from typing import Tuple, Dict, Any, List, Optional
from dataclasses import dataclass, asdict
from enum import Enum

import xgboost as xgb
from sklearn.calibration import CalibratedClassifierCV
from sklearn.model_selection import cross_val_predict
import shap

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import config
from resolution.feature_engineering import FEATURE_NAMES, NUM_FEATURES

logger = logging.getLogger("udyamgraph.resolution.classifier")


# ─── Decision Types ──────────────────────────────────────────────────────────

class LinkDecision(str, Enum):
    AUTO_LINK = "auto_link"       # p >= 0.92 — merge automatically
    REVIEW = "review"             # 0.65 <= p < 0.92 — human review
    REJECT = "reject"             # p < 0.65 — do not merge


@dataclass
class LinkageResult:
    """Result of entity resolution for a single candidate pair."""
    record_a_id: str
    record_b_id: str
    decision: LinkDecision
    confidence: float              # Calibrated probability [0, 1]
    raw_score: float               # Uncalibrated XGBoost score
    feature_values: Dict[str, float]   # Named feature values
    shap_values: Dict[str, float]      # SHAP attribution per feature
    explanation: str               # Human-readable explanation
    top_contributors: List[Dict[str, Any]]  # Top 5 features by importance

    def to_dict(self) -> Dict[str, Any]:
        result = asdict(self)
        result["decision"] = self.decision.value
        return result


# ─── Entity Resolution Classifier ───────────────────────────────────────────

class EntityResolutionClassifier:
    """
    XGBoost pairwise classifier with confidence calibration and
    SHAP-based explainability for every linkage decision.
    
    Architecture:
    ┌──────────────┐   ┌──────────────┐   ┌──────────────┐   ┌──────────────┐
    │  Feature      │──▶│  XGBoost     │──▶│  Platt       │──▶│  3-Tier      │
    │  Vector       │   │  Classifier  │   │  Calibration │   │  Router      │
    │  (16 dims)    │   │  (raw score) │   │  (p ∈ [0,1]) │   │  Decision    │
    └──────────────┘   └──────────────┘   └──────────────┘   └──────────────┘
                              │
                              ▼
                       ┌──────────────┐
                       │  SHAP        │
                       │  Explainer   │
                       │  (per-pair)  │
                       └──────────────┘
    """

    def __init__(
        self,
        model_path: Optional[str] = None,
        auto_link_threshold: float = None,
        review_threshold: float = None,
    ):
        self.auto_link_threshold = (
            auto_link_threshold or config.entity_resolution.auto_link_threshold
        )
        self.review_threshold = (
            review_threshold or config.entity_resolution.review_threshold
        )
        self.model_path = (
            model_path or config.entity_resolution.xgboost_model_path
        )

        # These are initialized during train() or load()
        self.model: Optional[xgb.XGBClassifier] = None
        self.calibrated_model: Optional[CalibratedClassifierCV] = None
        self.explainer: Optional[shap.TreeExplainer] = None

    def train(
        self,
        X_train: np.ndarray,
        y_train: np.ndarray,
        X_val: Optional[np.ndarray] = None,
        y_val: Optional[np.ndarray] = None,
    ):
        """
        Train the entity resolution model with cost-sensitive learning.
        
        Cost-sensitive: scale_pos_weight is set so that false positives
        (merging two different businesses) are penalized 5x more than
        false negatives (missing a true match).
        
        Rationale: A false merge corrupts the UBID graph and is expensive
        to undo. A missed link can always be caught later.
        """
        # Calculate imbalance ratio
        n_positive = np.sum(y_train == 1)
        n_negative = np.sum(y_train == 0)
        natural_ratio = n_negative / max(n_positive, 1)

        # Apply additional cost penalty: false positives cost 5x more
        # XGBoost's scale_pos_weight adjusts the positive class weight
        # To make FP more costly, we REDUCE scale_pos_weight below natural
        fp_cost = config.entity_resolution.false_positive_cost
        fn_cost = config.entity_resolution.false_negative_cost
        adjusted_weight = (natural_ratio * fn_cost) / fp_cost

        logger.info(
            f"Training with cost-sensitive weights: "
            f"scale_pos_weight={adjusted_weight:.3f} "
            f"(FP cost: {fp_cost}, FN cost: {fn_cost})"
        )

        self.model = xgb.XGBClassifier(
            n_estimators=300,
            max_depth=6,
            learning_rate=0.05,
            scale_pos_weight=adjusted_weight,
            min_child_weight=5,
            subsample=0.8,
            colsample_bytree=0.8,
            reg_alpha=0.1,          # L1 regularization
            reg_lambda=1.0,         # L2 regularization
            eval_metric="aucpr",    # Area under Precision-Recall curve
            use_label_encoder=False,
            random_state=42,
            n_jobs=-1,
        )

        eval_set = [(X_train, y_train)]
        if X_val is not None and y_val is not None:
            eval_set.append((X_val, y_val))

        self.model.fit(
            X_train, y_train,
            eval_set=eval_set,
            verbose=True,
        )

        # ── Platt Scaling Calibration ────────────────────────────────
        # Ensures that model output p=0.92 truly means 92% probability
        # of being a match, not just an arbitrary score threshold.
        logger.info("Calibrating probabilities with Platt scaling...")
        self.calibrated_model = CalibratedClassifierCV(
            estimator=self.model,
            method="sigmoid",  # Platt scaling
            cv=5,              # 5-fold cross-validation
        )
        self.calibrated_model.fit(X_train, y_train)

        # ── SHAP Explainer ───────────────────────────────────────────
        logger.info("Initializing SHAP TreeExplainer...")
        self.explainer = shap.TreeExplainer(self.model)

        logger.info("Training complete")

    def predict(
        self,
        features: np.ndarray,
        record_a_id: str = "",
        record_b_id: str = "",
    ) -> LinkageResult:
        """
        Predict whether two records are a match with full explainability.
        
        Returns a LinkageResult containing:
        - Calibrated confidence probability
        - Three-tier decision (auto-link / review / reject)
        - SHAP feature attributions
        - Human-readable explanation
        """
        if self.calibrated_model is None or self.explainer is None:
            raise RuntimeError("Model not trained. Call train() or load() first.")

        # Reshape for single prediction
        X = features.reshape(1, -1)

        # Get calibrated probability
        calibrated_prob = self.calibrated_model.predict_proba(X)[0][1]

        # Get raw XGBoost score (for reference)
        raw_score = float(self.model.predict_proba(X)[0][1])

        # Three-tier routing decision
        if calibrated_prob >= self.auto_link_threshold:
            decision = LinkDecision.AUTO_LINK
        elif calibrated_prob >= self.review_threshold:
            decision = LinkDecision.REVIEW
        else:
            decision = LinkDecision.REJECT

        # SHAP values for explainability
        shap_values = self.explainer.shap_values(X)
        # For binary classification, shap_values may be a list [neg, pos]
        if isinstance(shap_values, list):
            shap_vals = shap_values[1][0]  # Positive class
        else:
            shap_vals = shap_values[0]

        # Build named feature/SHAP dictionaries
        feature_dict = {
            name: float(features[i])
            for i, name in enumerate(FEATURE_NAMES)
        }
        shap_dict = {
            name: float(shap_vals[i])
            for i, name in enumerate(FEATURE_NAMES)
        }

        # Top contributors sorted by absolute SHAP value
        top_contributors = sorted(
            [
                {
                    "feature": name,
                    "value": float(features[i]),
                    "shap_impact": float(shap_vals[i]),
                    "direction": "match" if shap_vals[i] > 0 else "non-match",
                }
                for i, name in enumerate(FEATURE_NAMES)
            ],
            key=lambda x: abs(x["shap_impact"]),
            reverse=True,
        )[:5]  # Top 5

        # Generate human-readable explanation
        explanation = self._generate_explanation(
            decision, calibrated_prob, top_contributors
        )

        return LinkageResult(
            record_a_id=record_a_id,
            record_b_id=record_b_id,
            decision=decision,
            confidence=float(calibrated_prob),
            raw_score=raw_score,
            feature_values=feature_dict,
            shap_values=shap_dict,
            explanation=explanation,
            top_contributors=top_contributors,
        )

    def predict_batch(
        self,
        feature_matrix: np.ndarray,
        pair_ids: List[Tuple[str, str]],
    ) -> List[LinkageResult]:
        """Predict for a batch of candidate pairs."""
        results = []
        for i in range(feature_matrix.shape[0]):
            rec_a_id, rec_b_id = pair_ids[i]
            result = self.predict(
                feature_matrix[i],
                record_a_id=rec_a_id,
                record_b_id=rec_b_id,
            )
            results.append(result)

            if (i + 1) % 100 == 0:
                logger.info(f"Predicted {i + 1}/{len(pair_ids)} pairs")

        # Summary statistics
        auto_links = sum(1 for r in results if r.decision == LinkDecision.AUTO_LINK)
        reviews = sum(1 for r in results if r.decision == LinkDecision.REVIEW)
        rejects = sum(1 for r in results if r.decision == LinkDecision.REJECT)

        logger.info(
            f"Batch results: {auto_links} auto-links, "
            f"{reviews} reviews, {rejects} rejects "
            f"(total: {len(results)})"
        )

        return results

    def _generate_explanation(
        self,
        decision: LinkDecision,
        confidence: float,
        top_contributors: List[Dict[str, Any]]
    ) -> str:
        """Generate a human-readable explanation for a linkage decision."""
        lines = []

        if decision == LinkDecision.AUTO_LINK:
            lines.append(
                f"✅ AUTO-LINKED with {confidence:.1%} confidence."
            )
        elif decision == LinkDecision.REVIEW:
            lines.append(
                f"🔍 SENT FOR REVIEW with {confidence:.1%} confidence."
            )
        else:
            lines.append(
                f"❌ REJECTED with {confidence:.1%} confidence."
            )

        lines.append("Top contributing factors:")
        for i, contrib in enumerate(top_contributors[:3], 1):
            feature_label = contrib["feature"].replace("_", " ").title()
            direction = "↑ supports match" if contrib["direction"] == "match" \
                else "↓ suggests different entities"
            lines.append(
                f"  {i}. {feature_label}: "
                f"value={contrib['value']:.3f}, "
                f"impact={contrib['shap_impact']:+.3f} ({direction})"
            )

        return "\n".join(lines)

    def save(self, path: Optional[str] = None):
        """Save the trained model to disk."""
        save_path = path or self.model_path
        os.makedirs(os.path.dirname(save_path) or ".", exist_ok=True)
        self.model.save_model(save_path)
        logger.info(f"Model saved to {save_path}")

    def load(self, path: Optional[str] = None):
        """Load a trained model from disk."""
        load_path = path or self.model_path
        self.model = xgb.XGBClassifier()
        self.model.load_model(load_path)

        # Re-initialize calibration wrapper around loaded model
        self.calibrated_model = CalibratedClassifierCV(
            estimator=self.model,
            method="sigmoid",
            cv="prefit",
        )

        self.explainer = shap.TreeExplainer(self.model)
        logger.info(f"Model loaded from {load_path}")


# ─── Demo / Training Script ─────────────────────────────────────────────────

def demo_training_and_prediction():
    """
    Demonstrate the full entity resolution pipeline with synthetic data.
    Uses Karnataka-specific business record formats.
    """
    print("=" * 70)
    print("UdyamGraph — Entity Resolution Classifier Demo")
    print("=" * 70)

    np.random.seed(42)

    # Generate synthetic training data
    print("\n📊 Generating synthetic training data...")
    n_samples = 2000
    X_train = np.random.rand(n_samples, NUM_FEATURES).astype(np.float32)

    # Create labels: high similarity features → match
    y_train = np.zeros(n_samples, dtype=np.int32)
    for i in range(n_samples):
        # Strong indicator: PAN token match
        if X_train[i, 0] > 0.8:  # pan_token_match
            y_train[i] = 1
        # Moderate indicator: high name + address similarity
        elif X_train[i, 3] > 0.85 and X_train[i, 8] > 0.8:
            y_train[i] = 1
        # Weak indicator: name match + pincode match
        elif X_train[i, 3] > 0.9 and X_train[i, 9] > 0.8:
            y_train[i] = 1 if np.random.random() > 0.3 else 0

    print(f"   Positive (match): {np.sum(y_train == 1)}")
    print(f"   Negative (non-match): {np.sum(y_train == 0)}")

    # Train classifier
    print("\n🎓 Training XGBoost classifier with cost-sensitive learning...")
    classifier = EntityResolutionClassifier()
    classifier.train(X_train, y_train)

    # Demonstrate predictions
    print("\n" + "=" * 70)
    print("PREDICTION EXAMPLES")
    print("=" * 70)

    # Example 1: Strong match (shared PAN, similar name)
    print("\n── Case 1: Strong Match ────────────────────────────────")
    print("   Commercial Taxes: UBIQUITY INNOVATIONS PVT LTD (PAN: AABCU1234R)")
    print("   Factories Board:  UBIQUITY INNOVATIONS PRIVATE LIMITED (PAN: AABCU1234R)")

    features_match = np.array([
        1.0,   # pan_token_match — EXACT MATCH
        0.0,   # gstin_token_match — different GSTINs
        0.0,   # udyam_token_match
        0.92,  # name_jaro_winkler — very similar names
        0.88,  # name_levenshtein_norm
        0.75,  # name_token_overlap — shared tokens
        0.94,  # name_sbert_cosine — semantic similarity
        0.85,  # trade_name_jaro_winkler
        0.78,  # address_sbert_cosine — similar address
        1.0,   # pincode_match — same pincode
        1.0,   # district_match — Bangalore Urban
        0.95,  # city_jaro_winkler
        0.0,   # phone_match
        0.0,   # email_match
        1.0,   # legal_status_match — both Pvt Ltd
        0.5,   # business_type_match
    ], dtype=np.float32)

    result = classifier.predict(
        features_match,
        record_a_id="29AABCU1234R1Z5",
        record_b_id="KA-BLR-FAC-2020-00456"
    )
    print(f"\n{result.explanation}")

    # Example 2: Uncertain (similar name, no PAN match)
    print("\n── Case 2: Uncertain Match ─────────────────────────────")
    print("   Shops Registration: KRISHNA TRADERS (no PAN)")
    print("   Commercial Taxes:   KRISHNA TRADING COMPANY (different pincode)")

    features_uncertain = np.array([
        0.0,   # pan_token_match — no PAN available
        0.0,   # gstin_token_match
        0.0,   # udyam_token_match
        0.78,  # name_jaro_winkler — similar but not identical
        0.65,  # name_levenshtein_norm
        0.50,  # name_token_overlap
        0.72,  # name_sbert_cosine
        0.70,  # trade_name_jaro_winkler
        0.45,  # address_sbert_cosine — different addresses
        0.0,   # pincode_match — DIFFERENT pincodes
        1.0,   # district_match — same district
        0.80,  # city_jaro_winkler
        0.0,   # phone_match
        0.0,   # email_match
        0.5,   # legal_status_match
        0.8,   # business_type_match — both Trading
    ], dtype=np.float32)

    result2 = classifier.predict(
        features_uncertain,
        record_a_id="SE-BLR-2021-78901",
        record_b_id="29BDHPK5678L1Z3"
    )
    print(f"\n{result2.explanation}")

    # Example 3: Clear non-match
    print("\n── Case 3: Clear Non-Match ─────────────────────────────")
    print("   Record A: ABC PHARMA PVT LTD, Mysuru")
    print("   Record B: XYZ TEXTILES, Hubli")

    features_reject = np.array([
        0.0,   # pan_token_match — different PANs
        0.0,   # gstin_token_match
        0.0,   # udyam_token_match
        0.15,  # name_jaro_winkler — very different names
        0.10,  # name_levenshtein_norm
        0.0,   # name_token_overlap
        0.12,  # name_sbert_cosine
        0.10,  # trade_name_jaro_winkler
        0.08,  # address_sbert_cosine — different cities
        0.0,   # pincode_match
        0.0,   # district_match — different districts!
        0.20,  # city_jaro_winkler
        0.0,   # phone_match
        0.0,   # email_match
        0.0,   # legal_status_match
        0.0,   # business_type_match
    ], dtype=np.float32)

    result3 = classifier.predict(
        features_reject,
        record_a_id="29AAKFA9876B1Z2",
        record_b_id="29BBTXZ1234C2Z7"
    )
    print(f"\n{result3.explanation}")

    # Summary
    print("\n" + "=" * 70)
    print("DECISION SUMMARY")
    print("=" * 70)
    for r in [result, result2, result3]:
        icon = {"auto_link": "✅", "review": "🔍", "reject": "❌"}
        print(
            f"  {icon[r.decision.value]} "
            f"{r.record_a_id} ↔ {r.record_b_id}: "
            f"{r.decision.value} (confidence: {r.confidence:.1%})"
        )


if __name__ == "__main__":
    demo_training_and_prediction()
