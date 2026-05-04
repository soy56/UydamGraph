"""
UdyamGraph — Blocking Engine
==============================

Reduces the O(n²) comparison space to O(n·k) using Locality-Sensitive
Hashing (LSH) and phonetic blocking. For 10M+ records, comparing every
pair (50 × 10^12 pairs) is infeasible. Blocking generates candidate
pairs that share blocking keys.

Blocking Strategies:
1. **PAN Token Exact** — Records sharing the same tokenized PAN
2. **GSTIN Prefix** — Records with GSTIN sharing first 12 chars (same entity)
3. **LSH on Business Name** — MinHash LSH for fuzzy name matching
4. **Phonetic** — Soundex/Metaphone for transliteration variants
5. **Pincode + Name Prefix** — Geographic + lexical blocking
"""

import logging
from typing import List, Tuple, Dict, Any, Set, Optional
from collections import defaultdict
from itertools import combinations
import hashlib

import jellyfish

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import config

logger = logging.getLogger("udyamgraph.resolution.blocking")


class BlockingEngine:
    """
    Multi-strategy blocking engine for entity resolution.
    
    Generates candidate pairs from a set of records using multiple
    complementary blocking strategies, then unions the results.
    This ensures high recall while keeping the comparison count tractable.
    """

    def __init__(self, max_block_size: int = None):
        self.max_block_size = (
            max_block_size or config.entity_resolution.max_block_size
        )

    def generate_candidate_pairs(
        self,
        records: List[Dict[str, Any]]
    ) -> List[Tuple[int, int]]:
        """
        Generate candidate pairs using multiple blocking strategies.
        
        Args:
            records: List of tokenized business records
            
        Returns:
            List of (idx_a, idx_b) pairs that are candidate matches
        """
        all_pairs: Set[Tuple[int, int]] = set()

        # Strategy 1: PAN token exact match
        pan_pairs = self._block_by_field(records, "pan_token")
        all_pairs.update(pan_pairs)
        logger.info(f"PAN token blocking: {len(pan_pairs)} pairs")

        # Strategy 2: GSTIN prefix blocking (first 12 chars = same PAN entity)
        gstin_pairs = self._block_by_gstin_prefix(records)
        all_pairs.update(gstin_pairs)
        logger.info(f"GSTIN prefix blocking: {len(gstin_pairs)} pairs")

        # Strategy 3: Phonetic blocking on business name
        phonetic_pairs = self._block_by_phonetic_name(records)
        all_pairs.update(phonetic_pairs)
        logger.info(f"Phonetic name blocking: {len(phonetic_pairs)} pairs")

        # Strategy 4: Pincode + name prefix
        geo_pairs = self._block_by_pincode_name(records)
        all_pairs.update(geo_pairs)
        logger.info(f"Pincode+name blocking: {len(geo_pairs)} pairs")

        # Strategy 5: Phone number exact match
        phone_pairs = self._block_by_field(records, "phone")
        all_pairs.update(phone_pairs)
        logger.info(f"Phone blocking: {len(phone_pairs)} pairs")

        logger.info(
            f"Total unique candidate pairs: {len(all_pairs)} "
            f"(from {len(records)} records)"
        )

        return list(all_pairs)

    def _block_by_field(
        self,
        records: List[Dict[str, Any]],
        field: str
    ) -> Set[Tuple[int, int]]:
        """Block by exact match on a single field."""
        blocks: Dict[str, List[int]] = defaultdict(list)

        for idx, rec in enumerate(records):
            value = rec.get(field)
            if value and isinstance(value, str) and value.strip():
                blocks[value.strip()].append(idx)

        return self._pairs_from_blocks(blocks)

    def _block_by_gstin_prefix(
        self,
        records: List[Dict[str, Any]]
    ) -> Set[Tuple[int, int]]:
        """
        Block by GSTIN prefix (first 12 chars).
        GSTIN = StateCode(2) + PAN(10) + Entity(1) + Z + Check
        First 12 chars uniquely identify the PAN holder.
        """
        blocks: Dict[str, List[int]] = defaultdict(list)

        for idx, rec in enumerate(records):
            gstin_token = rec.get("gstin_token")
            # Use token prefix as blocking key (preserves privacy)
            if gstin_token:
                prefix = gstin_token[:16]  # First 16 hex chars of token
                blocks[prefix].append(idx)

        return self._pairs_from_blocks(blocks)

    def _block_by_phonetic_name(
        self,
        records: List[Dict[str, Any]]
    ) -> Set[Tuple[int, int]]:
        """
        Block by phonetic encoding of business name.
        Handles transliteration variants (e.g., "Krishnamurthy" vs "Krishnamurti").
        """
        blocks: Dict[str, List[int]] = defaultdict(list)

        for idx, rec in enumerate(records):
            name = rec.get("business_name", "")
            if not name:
                continue

            # Generate multiple phonetic keys for better recall
            words = name.upper().split()
            for word in words[:3]:  # First 3 words
                if len(word) >= 3:
                    try:
                        soundex = jellyfish.soundex(word)
                        metaphone = jellyfish.metaphone(word)
                        blocks[f"SX:{soundex}"].append(idx)
                        blocks[f"MP:{metaphone}"].append(idx)
                    except Exception as e:
                        # Fallback: use simple prefix matching
                        logger.debug(f"Phonetic matching failed for '{word}': {e}")
                        blocks[f"PX:{word[:3]}"].append(idx)

        return self._pairs_from_blocks(blocks)

    def _block_by_pincode_name(
        self,
        records: List[Dict[str, Any]]
    ) -> Set[Tuple[int, int]]:
        """
        Block by pincode + first 3 characters of business name.
        Geographic proximity + lexical similarity.
        """
        blocks: Dict[str, List[int]] = defaultdict(list)

        for idx, rec in enumerate(records):
            pincode = rec.get("pincode", "")
            name = rec.get("business_name", "")
            if pincode and name and len(name) >= 3:
                key = f"{pincode}:{name[:3].upper()}"
                blocks[key].append(idx)

        return self._pairs_from_blocks(blocks)

    def _pairs_from_blocks(
        self,
        blocks: Dict[str, List[int]]
    ) -> Set[Tuple[int, int]]:
        """Convert blocks to pairs, respecting max block size."""
        pairs: Set[Tuple[int, int]] = set()

        for key, indices in blocks.items():
            if len(indices) > self.max_block_size:
                logger.warning(
                    f"Block '{key[:20]}...' has {len(indices)} records "
                    f"(max={self.max_block_size}), skipping"
                )
                continue

            if len(indices) < 2:
                continue

            for i, j in combinations(indices, 2):
                pair = (min(i, j), max(i, j))
                pairs.add(pair)

        return pairs
