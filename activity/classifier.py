"""
UdyamGraph — Activity Classification Engine
==============================================

Classifies businesses as ACTIVE / DORMANT / CLOSED / UNCERTAIN
based on signals collected from Kafka event streams across
Karnataka's department systems.

Architecture:
    Kafka Event Streams → Feature Engineering → GBM Classifier
                                                    ↓
                                              Rule Engine Override
                                                    ↓
                                            Final Classification

Two-layer approach:
1. **Gradient Boosting Model**: Learns complex patterns from multiple
   signals (inspection history, filing recency, consumption data)
2. **Rule Engine Overlay**: Deterministic overrides for unambiguous
   signals (e.g., explicit GST cancellation = CLOSED)

Features are engineered from:
• Inspection renewal history (Factories Board)
• GST return filing recency (Commercial Taxes)
• License renewal status (Shops & Establishments)
• Electricity/water consumption trends (if available)
• Employee provident fund deposits (EPFO signals)
"""

import logging
import numpy as np
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
from enum import Enum

import xgboost as xgb

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import config

logger = logging.getLogger("udyamgraph.activity")


# ─── Activity Status Enum ───────────────────────────────────────────────────

class ActivityStatus(str, Enum):
    ACTIVE = "ACTIVE"           # Business is operating normally
    DORMANT = "DORMANT"         # No recent activity, but not formally closed
    CLOSED = "CLOSED"           # Business has ceased operations
    UNCERTAIN = "UNCERTAIN"     # Insufficient data to determine


@dataclass
class ActivitySignal:
    """A single activity signal from a department event stream."""
    department: str
    signal_type: str        # filing, inspection, renewal, consumption, closure
    signal_date: str        # ISO date
    value: Optional[float] = None   # e.g., consumption amount, filing count
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ActivityClassification:
    """Classification result for a single business entity."""
    ubid_id: str
    status: ActivityStatus
    confidence: float
    ml_prediction: str          # Raw ML model prediction
    ml_confidence: float
    rule_override: Optional[str] = None  # If rule engine overrode ML
    rule_reason: Optional[str] = None
    features: Dict[str, float] = field(default_factory=dict)
    signals_used: int = 0
    classified_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    def to_dict(self) -> Dict[str, Any]:
        result = asdict(self)
        result["status"] = self.status.value
        return result


# ─── Feature Engineering ─────────────────────────────────────────────────────

ACTIVITY_FEATURE_NAMES = [
    # Filing recency features
    "days_since_last_gst_filing",      # Days since last GST return
    "gst_filing_frequency_12m",        # Filings in last 12 months
    "gst_status_active",               # Binary: GST registration active

    # Inspection / renewal features
    "days_since_last_inspection",      # Days since last factory inspection
    "license_renewal_overdue_days",    # Days overdue for renewal (0 if current)
    "renewal_count_3yr",               # How many renewals in last 3 years

    # Registration status features
    "any_active_registration",         # Binary: ≥1 active registration
    "num_active_registrations",        # Count of active registrations
    "num_cancelled_registrations",     # Count of cancelled registrations

    # Consumption signals
    "consumption_trend_slope",         # Trend slope of utility consumption
    "consumption_last_quarter",        # Consumption in last quarter (normalized)

    # Employee / compliance signals
    "days_since_last_epf_deposit",     # Days since last EPF payment
    "has_recent_compliance_filing",    # Binary: any filing in last 6 months

    # Cross-department activity
    "department_activity_score",       # Weighted score across departments
    "days_since_any_activity",         # Days since ANY signal from ANY dept
]

NUM_ACTIVITY_FEATURES = len(ACTIVITY_FEATURE_NAMES)


class ActivityFeatureEngineer:
    """
    Engineers features for activity classification from raw
    Kafka event signals collected across departments.
    """

    def __init__(self, reference_date: Optional[datetime] = None):
        self.reference_date = reference_date or datetime.now(timezone.utc)

    def extract_features(
        self,
        signals: List[ActivitySignal]
    ) -> np.ndarray:
        """
        Extract activity features from a list of signals for one business.

        Args:
            signals: All Kafka-sourced activity signals for this UBID

        Returns:
            numpy array of shape (NUM_ACTIVITY_FEATURES,)
        """
        features = np.full(NUM_ACTIVITY_FEATURES, -1.0, dtype=np.float32)

        if not signals:
            return features

        # Categorize signals
        gst_filings = [s for s in signals if s.signal_type == "filing"
                       and s.department == "commercial_taxes"]
        inspections = [s for s in signals if s.signal_type == "inspection"]
        renewals = [s for s in signals if s.signal_type == "renewal"]
        consumption = [s for s in signals if s.signal_type == "consumption"]
        closures = [s for s in signals if s.signal_type == "closure"]
        epf = [s for s in signals if s.signal_type == "epf_deposit"]
        all_filings = [s for s in signals
                       if s.signal_type in ("filing", "renewal", "inspection")]

        # ── Filing Recency ───────────────────────────────────────────
        if gst_filings:
            latest_filing = max(gst_filings, key=lambda s: s.signal_date)
            features[0] = self._days_since(latest_filing.signal_date)

            twelve_months_ago = self.reference_date - timedelta(days=365)
            features[1] = sum(
                1 for s in gst_filings
                if s.signal_date >= twelve_months_ago.isoformat()
            )

            # Check if any filing indicates active status
            features[2] = 1.0 if any(
                s.metadata.get("gst_status") == "Active"
                for s in gst_filings
            ) else 0.0

        # ── Inspection / Renewal ─────────────────────────────────────
        if inspections:
            latest_inspection = max(inspections, key=lambda s: s.signal_date)
            features[3] = self._days_since(latest_inspection.signal_date)

        if renewals:
            # Check for overdue renewals
            overdue = [
                s for s in renewals
                if s.metadata.get("overdue_days", 0) > 0
            ]
            features[4] = max(
                (s.metadata.get("overdue_days", 0) for s in overdue),
                default=0.0
            )

            three_years_ago = self.reference_date - timedelta(days=1095)
            features[5] = sum(
                1 for s in renewals
                if s.signal_date >= three_years_ago.isoformat()
            )

        # ── Registration Status ──────────────────────────────────────
        active_regs = [
            s for s in signals
            if s.metadata.get("status") in ("Active", "VALID", "RENEWED")
        ]
        cancelled_regs = [
            s for s in signals
            if s.metadata.get("status") in ("Cancelled", "REVOKED", "CLOSED")
        ]
        features[6] = 1.0 if active_regs else 0.0
        features[7] = float(len(active_regs))
        features[8] = float(len(cancelled_regs))

        # ── Consumption ──────────────────────────────────────────────
        if consumption and len(consumption) >= 2:
            sorted_consumption = sorted(consumption, key=lambda s: s.signal_date)
            values = [s.value for s in sorted_consumption if s.value is not None]
            if len(values) >= 2:
                # Simple linear slope
                x = np.arange(len(values), dtype=np.float32)
                slope = np.polyfit(x, values, 1)[0]
                features[9] = float(slope)
                features[10] = float(values[-1]) if values else -1.0

        # ── EPF / Compliance ─────────────────────────────────────────
        if epf:
            latest_epf = max(epf, key=lambda s: s.signal_date)
            features[11] = self._days_since(latest_epf.signal_date)

        six_months_ago = self.reference_date - timedelta(days=180)
        features[12] = 1.0 if any(
            s.signal_date >= six_months_ago.isoformat()
            for s in all_filings
        ) else 0.0

        # ── Cross-Department Score ───────────────────────────────────
        active_depts = set()
        for s in signals:
            if self._days_since(s.signal_date) < 365:
                active_depts.add(s.department)
        features[13] = float(len(active_depts))

        if signals:
            latest_any = max(signals, key=lambda s: s.signal_date)
            features[14] = self._days_since(latest_any.signal_date)

        return features

    def _days_since(self, date_str: str) -> float:
        """Calculate days since a given ISO date string."""
        try:
            dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            delta = self.reference_date - dt
            return max(0.0, float(delta.days))
        except (ValueError, TypeError):
            return -1.0


# ─── Rule Engine ─────────────────────────────────────────────────────────────

class ActivityRuleEngine:
    """
    Deterministic rule engine that overrides ML predictions when
    unambiguous signals are present.
    
    Rules are ordered by priority — first matching rule wins.
    
    Rule rationale: Some signals are so definitive that ML confidence
    is irrelevant. For example, if GST registration is explicitly
    cancelled with a closure date, the business is CLOSED regardless
    of what the model thinks.
    """

    def __init__(self):
        self.rules = [
            # Priority 1: Explicit closure signals
            self._rule_explicit_closure,
            # Priority 2: All registrations cancelled
            self._rule_all_cancelled,
            # Priority 3: Recent activity confirms active
            self._rule_recent_multi_dept_activity,
            # Priority 4: Extended inactivity
            self._rule_extended_inactivity,
        ]

    def evaluate(
        self,
        signals: List[ActivitySignal],
        features: np.ndarray,
    ) -> Optional[Tuple[ActivityStatus, str]]:
        """
        Evaluate rules against signals. Returns (status, reason) if
        a rule fires, or None if ML prediction should be used.
        """
        for rule_fn in self.rules:
            result = rule_fn(signals, features)
            if result is not None:
                return result
        return None

    def _rule_explicit_closure(
        self,
        signals: List[ActivitySignal],
        features: np.ndarray,
    ) -> Optional[Tuple[ActivityStatus, str]]:
        """
        RULE: If any department has filed an explicit closure/cancellation
        with a formal date, classify as CLOSED.
        """
        closures = [
            s for s in signals
            if s.signal_type == "closure"
            and s.metadata.get("formal_closure", False)
        ]

        if closures:
            latest = max(closures, key=lambda s: s.signal_date)
            return (
                ActivityStatus.CLOSED,
                f"Formal closure filed by {latest.department} "
                f"on {latest.signal_date}"
            )
        return None

    def _rule_all_cancelled(
        self,
        signals: List[ActivitySignal],
        features: np.ndarray,
    ) -> Optional[Tuple[ActivityStatus, str]]:
        """
        RULE: If ALL registrations across ALL departments are
        cancelled/revoked, classify as CLOSED.
        """
        if features[7] == 0.0 and features[8] > 0.0:  # No active, some cancelled
            return (
                ActivityStatus.CLOSED,
                f"All {int(features[8])} registrations are cancelled/revoked"
            )
        return None

    def _rule_recent_multi_dept_activity(
        self,
        signals: List[ActivitySignal],
        features: np.ndarray,
    ) -> Optional[Tuple[ActivityStatus, str]]:
        """
        RULE: If activity signals exist from 3+ departments in
        the last 6 months, classify as ACTIVE (high confidence).
        """
        if features[13] >= 3.0 and features[14] < 180:
            return (
                ActivityStatus.ACTIVE,
                f"Active signals from {int(features[13])} departments "
                f"in last 6 months"
            )
        return None

    def _rule_extended_inactivity(
        self,
        signals: List[ActivitySignal],
        features: np.ndarray,
    ) -> Optional[Tuple[ActivityStatus, str]]:
        """
        RULE: If no activity from ANY department in 18+ months AND
        at least one registration exists, classify as DORMANT.
        """
        dormant_threshold = config.activity.dormant_months_threshold * 30
        if features[14] >= dormant_threshold and features[6] == 1.0:
            return (
                ActivityStatus.DORMANT,
                f"No activity for {int(features[14])} days "
                f"(threshold: {dormant_threshold} days) with active registrations"
            )
        return None


# ─── Activity Classifier ────────────────────────────────────────────────────

class ActivityClassifier:
    """
    Gradient Boosting classifier for business activity status,
    with a rule engine overlay for deterministic overrides.
    
    Pipeline:
        signals → feature engineering → GBM prediction → rule override → final status
    """

    STATUS_MAP = {
        0: ActivityStatus.ACTIVE,
        1: ActivityStatus.DORMANT,
        2: ActivityStatus.CLOSED,
        3: ActivityStatus.UNCERTAIN,
    }
    REVERSE_STATUS_MAP = {v: k for k, v in STATUS_MAP.items()}

    def __init__(self, model_path: Optional[str] = None):
        self.model_path = model_path or config.activity.model_path
        self.feature_engineer = ActivityFeatureEngineer()
        self.rule_engine = ActivityRuleEngine()
        self.model: Optional[xgb.XGBClassifier] = None

    def train(
        self,
        X_train: np.ndarray,
        y_train: np.ndarray,
        X_val: Optional[np.ndarray] = None,
        y_val: Optional[np.ndarray] = None,
    ):
        """Train the activity classification model."""
        self.model = xgb.XGBClassifier(
            n_estimators=200,
            max_depth=5,
            learning_rate=0.08,
            num_class=4,
            objective="multi:softprob",
            eval_metric="mlogloss",
            min_child_weight=3,
            subsample=0.8,
            colsample_bytree=0.8,
            use_label_encoder=False,
            random_state=42,
            n_jobs=-1,
        )

        eval_set = [(X_train, y_train)]
        if X_val is not None:
            eval_set.append((X_val, y_val))

        self.model.fit(
            X_train, y_train,
            eval_set=eval_set,
            verbose=True,
        )

        logger.info("Activity classifier training complete")

    def classify(
        self,
        ubid_id: str,
        signals: List[ActivitySignal],
    ) -> ActivityClassification:
        """
        Classify activity status for a business entity.
        
        Args:
            ubid_id: The Unified Business ID
            signals: All activity signals from Kafka streams
            
        Returns:
            ActivityClassification with status, confidence, and reasoning
        """
        # Feature engineering
        features = self.feature_engineer.extract_features(signals)

        # ML prediction
        if self.model is not None:
            X = features.reshape(1, -1)
            probabilities = self.model.predict_proba(X)[0]
            ml_class = int(np.argmax(probabilities))
            ml_confidence = float(probabilities[ml_class])
            ml_status = self.STATUS_MAP[ml_class]
        else:
            ml_status = ActivityStatus.UNCERTAIN
            ml_confidence = 0.0

        # Rule engine override
        rule_result = self.rule_engine.evaluate(signals, features)

        if rule_result is not None:
            final_status, rule_reason = rule_result
            return ActivityClassification(
                ubid_id=ubid_id,
                status=final_status,
                confidence=0.99,  # Rule-based = high confidence
                ml_prediction=ml_status.value,
                ml_confidence=ml_confidence,
                rule_override=final_status.value,
                rule_reason=rule_reason,
                features={
                    name: float(features[i])
                    for i, name in enumerate(ACTIVITY_FEATURE_NAMES)
                },
                signals_used=len(signals),
            )

        return ActivityClassification(
            ubid_id=ubid_id,
            status=ml_status,
            confidence=ml_confidence,
            ml_prediction=ml_status.value,
            ml_confidence=ml_confidence,
            features={
                name: float(features[i])
                for i, name in enumerate(ACTIVITY_FEATURE_NAMES)
            },
            signals_used=len(signals),
        )


# ─── Demo ────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    from datetime import date

    print("=" * 70)
    print("UdyamGraph — Activity Classification Engine")
    print("=" * 70)

    classifier = ActivityClassifier()
    ref_date = datetime(2024, 12, 15, tzinfo=timezone.utc)
    classifier.feature_engineer.reference_date = ref_date

    # ── Case 1: Active business ──────────────────────────────────────
    print("\n── Case 1: Active Business ─────────────────────────────")
    print("   UBIQUITY INNOVATIONS PVT LTD")
    print("   Recent GST filings, active factory license, EPF deposits")

    active_signals = [
        ActivitySignal("commercial_taxes", "filing", "2024-11-30",
                       metadata={"gst_status": "Active"}),
        ActivitySignal("commercial_taxes", "filing", "2024-10-31",
                       metadata={"gst_status": "Active"}),
        ActivitySignal("commercial_taxes", "filing", "2024-09-30",
                       metadata={"gst_status": "Active"}),
        ActivitySignal("factories_board", "inspection", "2024-08-15",
                       metadata={"status": "Active"}),
        ActivitySignal("factories_board", "renewal", "2024-03-15",
                       metadata={"status": "RENEWED", "overdue_days": 0}),
        ActivitySignal("shops_establishments", "renewal", "2024-06-01",
                       metadata={"status": "Active"}),
        ActivitySignal("labour_department", "epf_deposit", "2024-11-15"),
    ]

    result1 = classifier.classify("UBID-KA-A1B2C3D4E5F6", active_signals)
    print(f"\n   Status: {result1.status.value}")
    print(f"   Confidence: {result1.confidence:.1%}")
    if result1.rule_override:
        print(f"   Rule Override: {result1.rule_reason}")
    print(f"   Signals Used: {result1.signals_used}")

    # ── Case 2: Dormant business ─────────────────────────────────────
    print("\n── Case 2: Dormant Business ────────────────────────────")
    print("   KRISHNA TEXTILE MILLS")
    print("   No filings for 20 months, license still valid")

    dormant_signals = [
        ActivitySignal("commercial_taxes", "filing", "2023-03-31",
                       metadata={"gst_status": "Active"}),
        ActivitySignal("factories_board", "renewal", "2022-12-01",
                       metadata={"status": "VALID", "overdue_days": 365}),
    ]

    result2 = classifier.classify("UBID-KA-X9Y8Z7W6V5U4", dormant_signals)
    print(f"\n   Status: {result2.status.value}")
    print(f"   Confidence: {result2.confidence:.1%}")
    if result2.rule_override:
        print(f"   Rule Override: {result2.rule_reason}")

    # ── Case 3: Closed business ──────────────────────────────────────
    print("\n── Case 3: Closed Business ─────────────────────────────")
    print("   DECCAN ELECTRONICS (CLOSED)")
    print("   Formal GST cancellation filed")

    closed_signals = [
        ActivitySignal("commercial_taxes", "closure", "2024-06-15",
                       metadata={"formal_closure": True,
                                "gst_status": "Cancelled"}),
        ActivitySignal("shops_establishments", "closure", "2024-07-01",
                       metadata={"formal_closure": True,
                                "status": "Cancelled"}),
    ]

    result3 = classifier.classify("UBID-KA-P3Q4R5S6T7U8", closed_signals)
    print(f"\n   Status: {result3.status.value}")
    print(f"   Confidence: {result3.confidence:.1%}")
    if result3.rule_override:
        print(f"   Rule Override: {result3.rule_reason}")

    # ── Summary ──────────────────────────────────────────────────────
    print("\n" + "=" * 70)
    print("CLASSIFICATION SUMMARY")
    print("=" * 70)
    icons = {"ACTIVE": "🟢", "DORMANT": "🟡", "CLOSED": "🔴", "UNCERTAIN": "⚪"}
    for r in [result1, result2, result3]:
        override = f" (Rule: {r.rule_reason})" if r.rule_override else ""
        print(
            f"  {icons[r.status.value]} {r.ubid_id}: "
            f"{r.status.value} ({r.confidence:.0%})"
            f"{override}"
        )
