"""
UdyamGraph — Pairwise Feature Engineering
============================================

Extracts features for each candidate pair of business records.
These features are fed to the XGBoost classifier for entity resolution.

Feature Categories:
1. **Identifier Match** — PAN/GSTIN token equality (binary)
2. **Name Similarity** — Jaro-Winkler, cosine, Levenshtein, Sentence-BERT
3. **Address Similarity** — SBERT embedding cosine, pincode match
4. **Contact Match** — Phone/email normalization and comparison
5. **Structural Match** — Legal status, business type agreement
6. **Temporal Signals** — Registration date proximity
"""

import logging
import re
import numpy as np
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass

import jellyfish
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import config

logger = logging.getLogger("udyamgraph.resolution.features")


# ─── Feature Names (for SHAP explainability) ─────────────────────────────────

FEATURE_NAMES = [
    # Identifier features (0-2)
    "pan_token_match",          # Binary: PAN tokens match
    "gstin_token_match",        # Binary: GSTIN tokens match
    "udyam_token_match",        # Binary: Udyam tokens match

    # Name similarity features (3-7)
    "name_jaro_winkler",        # Jaro-Winkler distance [0, 1]
    "name_levenshtein_norm",    # Normalized Levenshtein [0, 1]
    "name_token_overlap",      # Jaccard index of name tokens
    "name_sbert_cosine",        # Sentence-BERT embedding cosine sim
    "trade_name_jaro_winkler",  # Trade name Jaro-Winkler

    # Address similarity features (8-11)
    "address_sbert_cosine",     # Address embedding cosine similarity
    "pincode_match",            # Binary: pincode exact match
    "district_match",           # Binary: district match
    "city_jaro_winkler",        # City name Jaro-Winkler

    # Contact features (12-13)
    "phone_match",              # Binary: phone numbers match
    "email_match",              # Binary: email addresses match

    # Structural features (14-15)
    "legal_status_match",       # Binary: same legal structure
    "business_type_match",      # Binary: same business type
]

NUM_FEATURES = len(FEATURE_NAMES)


# ─── Feature Extractor ──────────────────────────────────────────────────────

class FeatureEngineer:
    """
    Extracts pairwise features for entity resolution.
    
    Uses Sentence-BERT for semantic similarity of business names
    and addresses, combined with string-distance and exact-match
    features for a comprehensive comparison vector.
    """

    def __init__(self, sbert_model_name: Optional[str] = None):
        model_name = sbert_model_name or config.entity_resolution.sbert_model_name
        logger.info(f"Loading Sentence-BERT model: {model_name}")
        self.sbert = SentenceTransformer(model_name)
        logger.info("Sentence-BERT model loaded")

    def extract_features(
        self,
        rec_a: Dict[str, Any],
        rec_b: Dict[str, Any]
    ) -> np.ndarray:
        """
        Extract feature vector for a candidate pair.
        
        Args:
            rec_a: First business record (tokenized)
            rec_b: Second business record (tokenized)
            
        Returns:
            numpy array of shape (NUM_FEATURES,) with feature values
        """
        features = np.zeros(NUM_FEATURES, dtype=np.float32)

        # ── Identifier Features ──────────────────────────────────────
        features[0] = self._exact_match(
            rec_a.get("pan_token"), rec_b.get("pan_token")
        )
        features[1] = self._exact_match(
            rec_a.get("gstin_token"), rec_b.get("gstin_token")
        )
        features[2] = self._exact_match(
            rec_a.get("udyam_token"), rec_b.get("udyam_token")
        )

        # ── Name Similarity Features ────────────────────────────────
        name_a = (rec_a.get("business_name") or "").upper().strip()
        name_b = (rec_b.get("business_name") or "").upper().strip()

        if name_a and name_b:
            features[3] = jellyfish.jaro_winkler_similarity(name_a, name_b)
            features[4] = 1.0 - (
                jellyfish.levenshtein_distance(name_a, name_b)
                / max(len(name_a), len(name_b), 1)
            )
            features[5] = self._token_overlap(name_a, name_b)
            features[6] = self._sbert_cosine(name_a, name_b)

        # Trade name similarity
        trade_a = (rec_a.get("trade_name") or "").upper().strip()
        trade_b = (rec_b.get("trade_name") or "").upper().strip()
        if trade_a and trade_b:
            features[7] = jellyfish.jaro_winkler_similarity(trade_a, trade_b)

        # ── Address Similarity Features ──────────────────────────────
        addr_a = self._build_address_string(rec_a)
        addr_b = self._build_address_string(rec_b)
        if addr_a and addr_b:
            features[8] = self._sbert_cosine(addr_a, addr_b)

        features[9] = self._exact_match(
            rec_a.get("pincode"), rec_b.get("pincode")
        )
        features[10] = self._exact_match(
            rec_a.get("district"), rec_b.get("district")
        )

        city_a = (rec_a.get("city") or "").upper().strip()
        city_b = (rec_b.get("city") or "").upper().strip()
        if city_a and city_b:
            features[11] = jellyfish.jaro_winkler_similarity(city_a, city_b)

        # ── Contact Features ─────────────────────────────────────────
        features[12] = self._phone_match(
            rec_a.get("phone"), rec_b.get("phone")
        )
        features[13] = self._exact_match(
            self._normalize_email(rec_a.get("email")),
            self._normalize_email(rec_b.get("email"))
        )

        # ── Structural Features ──────────────────────────────────────
        features[14] = self._fuzzy_match(
            rec_a.get("legal_status"), rec_b.get("legal_status")
        )
        features[15] = self._fuzzy_match(
            rec_a.get("business_type"), rec_b.get("business_type")
        )

        return features

    def extract_batch(
        self,
        records: List[Dict[str, Any]],
        pairs: List[Tuple[int, int]]
    ) -> np.ndarray:
        """
        Extract features for a batch of candidate pairs.
        
        Returns:
            numpy array of shape (num_pairs, NUM_FEATURES)
        """
        feature_matrix = np.zeros(
            (len(pairs), NUM_FEATURES), dtype=np.float32
        )

        for i, (idx_a, idx_b) in enumerate(pairs):
            feature_matrix[i] = self.extract_features(
                records[idx_a], records[idx_b]
            )

            if (i + 1) % 1000 == 0:
                logger.info(
                    f"Extracted features for {i + 1}/{len(pairs)} pairs"
                )

        return feature_matrix

    # ── Helper Methods ───────────────────────────────────────────────────────

    def _exact_match(
        self, val_a: Optional[str], val_b: Optional[str]
    ) -> float:
        """Binary exact match after normalization."""
        if not val_a or not val_b:
            return 0.0
        return 1.0 if val_a.strip().upper() == val_b.strip().upper() else 0.0

    def _fuzzy_match(
        self, val_a: Optional[str], val_b: Optional[str]
    ) -> float:
        """Fuzzy match using Jaro-Winkler for categorical fields."""
        if not val_a or not val_b:
            return 0.0
        return jellyfish.jaro_winkler_similarity(
            val_a.upper().strip(),
            val_b.upper().strip()
        )

    def _token_overlap(self, text_a: str, text_b: str) -> float:
        """Jaccard index of word tokens."""
        tokens_a = set(text_a.split())
        tokens_b = set(text_b.split())
        # Remove common stopwords
        stopwords = {"PVT", "LTD", "PRIVATE", "LIMITED", "AND", "&", "THE"}
        tokens_a -= stopwords
        tokens_b -= stopwords
        if not tokens_a or not tokens_b:
            return 0.0
        intersection = tokens_a & tokens_b
        union = tokens_a | tokens_b
        return len(intersection) / len(union)

    def _sbert_cosine(self, text_a: str, text_b: str) -> float:
        """Sentence-BERT cosine similarity."""
        try:
            embeddings = self.sbert.encode(
                [text_a, text_b],
                convert_to_numpy=True,
                show_progress_bar=False
            )
            sim = cosine_similarity(
                embeddings[0:1], embeddings[1:2]
            )[0][0]
            return float(max(0.0, sim))  # Clamp negative similarities
        except Exception as e:
            logger.warning(f"SBERT encoding failed: {e}")
            return 0.0

    def _phone_match(
        self, phone_a: Optional[str], phone_b: Optional[str]
    ) -> float:
        """Match phone numbers after normalization (strip +91, spaces, dashes)."""
        if not phone_a or not phone_b:
            return 0.0
        norm_a = re.sub(r'[\s\-\+]', '', phone_a)[-10:]
        norm_b = re.sub(r'[\s\-\+]', '', phone_b)[-10:]
        return 1.0 if norm_a == norm_b and len(norm_a) == 10 else 0.0

    def _normalize_email(self, email: Optional[str]) -> Optional[str]:
        """Normalize email for comparison."""
        if not email:
            return None
        return email.lower().strip()

    def _build_address_string(self, record: Dict[str, Any]) -> str:
        """Build a single address string for SBERT embedding."""
        parts = [
            record.get("address_line1", ""),
            record.get("address_line2", ""),
            record.get("city", ""),
            record.get("district", ""),
            record.get("pincode", ""),
        ]
        return " ".join(p for p in parts if p).strip()
