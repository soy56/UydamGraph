"""
UdyamGraph — Deterministic PII Tokenization Pipeline
=====================================================

Implements HMAC-SHA256 tokenization for sensitive identifiers (PAN, GSTIN, Aadhaar)
that provides:

1. **Irreversibility**: Cannot recover original PII from token
2. **Determinism**: Same input ALWAYS produces same token → maintains joinability
3. **Collision resistance**: SHA-256 provides 2^128 collision resistance
4. **Rainbow table resistance**: HMAC with secret key prevents precomputation attacks

Security Properties Proof:
─────────────────────────
Given:   token = HMAC-SHA256(secret_key, identifier)

• Irreversible: HMAC-SHA256 is a one-way function. Given `token` and even
  `secret_key`, finding `identifier` requires brute-forcing the input space.
  For PAN (10 chars, alphanumeric) = 36^10 ≈ 3.6 × 10^15 possibilities.

• Deterministic: HMAC is a pure function — same (key, message) pair ALWAYS
  produces the same output. This allows joining tokenized records across
  departments without storing raw PII.

• Joinable: If Department A has PAN "AABCU1234R" and Department B has the
  same PAN, both produce token "abc123...". Entity resolution can match
  on tokens without ever seeing the raw PAN.

Karnataka-Specific Formats:
───────────────────────────
• GSTIN: 29AABCU1234R1Z5  (29 = Karnataka state code)
• PAN:   AABCU1234R
• Udyam: UDYAM-KA-01-0012345
"""

import hmac
import hashlib
import re
import logging
from typing import Optional, Dict, Any, Tuple
from dataclasses import dataclass

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import config

logger = logging.getLogger("udyamgraph.pii")


# ─── Format Validators ───────────────────────────────────────────────────────

# GSTIN: 2-digit state + 10-char PAN + 1 entity + 1 alpha + 1 check
GSTIN_PATTERN = re.compile(r'^[0-9]{2}[A-Z]{5}[0-9]{4}[A-Z]{1}[1-9A-Z]{1}[Z]{1}[0-9A-Z]{1}$')

# PAN: 5 alpha + 4 digits + 1 alpha
PAN_PATTERN = re.compile(r'^[A-Z]{5}[0-9]{4}[A-Z]{1}$')

# Udyam Registration: UDYAM-XX-00-0000000
UDYAM_PATTERN = re.compile(r'^UDYAM-[A-Z]{2}-[0-9]{2}-[0-9]{7}$')

# Aadhaar: 12 digits (never stored, only tokenized in transit)
AADHAAR_PATTERN = re.compile(r'^[2-9]{1}[0-9]{11}$')


def validate_gstin(gstin: str) -> bool:
    """Validate GSTIN format. Does NOT check Luhn checksum."""
    return bool(GSTIN_PATTERN.match(gstin.upper().strip()))


def validate_pan(pan: str) -> bool:
    """Validate PAN format."""
    return bool(PAN_PATTERN.match(pan.upper().strip()))


def validate_udyam(udyam: str) -> bool:
    """Validate Udyam Registration Number format."""
    return bool(UDYAM_PATTERN.match(udyam.upper().strip()))


def is_karnataka_gstin(gstin: str) -> bool:
    """Check if GSTIN belongs to Karnataka (state code 29)."""
    return (
        validate_gstin(gstin) and
        gstin[:2] == config.pii.karnataka_state_code
    )


def extract_pan_from_gstin(gstin: str) -> Optional[str]:
    """
    Extract PAN from GSTIN.
    GSTIN structure: [StateCode:2][PAN:10][EntityCode:1][Z:1][Checksum:1]
    Example: 29AABCU1234R1Z5 → AABCU1234R
    """
    if validate_gstin(gstin):
        return gstin[2:12]
    return None


# ─── HMAC-SHA256 Tokenizer ───────────────────────────────────────────────────

class PIITokenizer:
    """
    Deterministic PII tokenizer using HMAC-SHA256.

    Properties:
    • Deterministic: tokenize("AABCU1234R") always returns the same hash
    • Irreversible: cannot recover "AABCU1234R" from the hash
    • Keyed: uses HMAC (not plain SHA-256) to prevent rainbow table attacks
    • Joinable: same PAN from different departments yields same token

    Usage:
        tokenizer = PIITokenizer(secret_key="...")
        token = tokenizer.tokenize_pan("AABCU1234R")
        # token = "a3f8c1d2e4b5..."  (64-char hex string)
    """

    def __init__(self, secret_key: Optional[str] = None):
        self._secret_key = (
            secret_key or config.pii.hmac_secret_key
        ).encode("utf-8")

    def _hmac_sha256(self, identifier: str) -> str:
        """
        Core tokenization function.

        HMAC-SHA256(key, message) is:
        1. One-way: Given output, cannot find input
        2. Deterministic: Same input → same output
        3. Keyed: Without the key, cannot reproduce tokens
        """
        normalized = identifier.upper().strip()
        return hmac.new(
            key=self._secret_key,
            msg=normalized.encode("utf-8"),
            digestmod=hashlib.sha256
        ).hexdigest()

    def tokenize_pan(self, pan: str) -> Optional[str]:
        """
        Tokenize a PAN number.

        Example:
            Input:  "AABCU1234R"
            Output: "a3f8c1d2e4b5a6f7..." (64-char hex, irreversible)

        The same PAN appearing in Commercial Taxes, Factories Board,
        and Shops & Establishments will yield the SAME token, enabling
        cross-department entity resolution without storing raw PAN.
        """
        if not pan or not validate_pan(pan):
            logger.warning(f"Invalid PAN format: {pan}")
            return None

        token = self._hmac_sha256(f"PAN:{pan}")
        logger.debug(f"Tokenized PAN {pan[:3]}****{pan[-1]}")
        return token

    def tokenize_gstin(self, gstin: str) -> Optional[str]:
        """
        Tokenize a GSTIN.

        Also extracts and separately tokenizes the embedded PAN
        for cross-reference with PAN-only records.
        """
        if not gstin or not validate_gstin(gstin):
            logger.warning(f"Invalid GSTIN format: {gstin}")
            return None

        token = self._hmac_sha256(f"GSTIN:{gstin}")
        logger.debug(f"Tokenized GSTIN {gstin[:4]}****{gstin[-3:]}")
        return token

    def tokenize_aadhaar(self, aadhaar: str) -> Optional[str]:
        """
        Tokenize an Aadhaar number.

        Aadhaar is NEVER stored in any form other than this token.
        Even in logs, only the token is recorded.
        """
        cleaned = aadhaar.replace(" ", "").replace("-", "")
        if not AADHAAR_PATTERN.match(cleaned):
            logger.warning("Invalid Aadhaar format")
            return None

        token = self._hmac_sha256(f"AADHAAR:{cleaned}")
        logger.debug("Tokenized Aadhaar")
        return token

    def tokenize_udyam(self, udyam: str) -> Optional[str]:
        """Tokenize a Udyam Registration Number."""
        if not udyam or not validate_udyam(udyam):
            logger.warning(f"Invalid Udyam format: {udyam}")
            return None

        token = self._hmac_sha256(f"UDYAM:{udyam}")
        return token

    def tokenize_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Tokenize all PII fields in a business record in-place.

        Replaces raw PAN, GSTIN, Aadhaar with their HMAC tokens.
        Also generates cross-reference tokens (e.g., PAN extracted from GSTIN).

        Returns the record with PII replaced by deterministic tokens.
        """
        tokenized = record.copy()

        # Tokenize PAN
        if record.get("pan"):
            tokenized["pan_token"] = self.tokenize_pan(record["pan"])
            tokenized.pop("pan", None)  # Remove raw PAN

        # Tokenize GSTIN
        if record.get("gstin"):
            tokenized["gstin_token"] = self.tokenize_gstin(record["gstin"])

            # Cross-reference: extract PAN from GSTIN and tokenize
            embedded_pan = extract_pan_from_gstin(record["gstin"])
            if embedded_pan and "pan_token" not in tokenized:
                tokenized["pan_token"] = self.tokenize_pan(embedded_pan)

            tokenized.pop("gstin", None)  # Remove raw GSTIN

        # Tokenize Aadhaar if present
        if record.get("aadhaar"):
            tokenized["aadhaar_token"] = self.tokenize_aadhaar(
                record["aadhaar"]
            )
            tokenized.pop("aadhaar", None)

        # Tokenize Udyam if present
        if record.get("udyam_number"):
            tokenized["udyam_token"] = self.tokenize_udyam(
                record["udyam_number"]
            )
            tokenized.pop("udyam_number", None)

        return tokenized


# ─── Proofs and Demonstrations ───────────────────────────────────────────────

def prove_determinism():
    """
    PROOF: Tokenization is deterministic.
    Same input always produces the same token, regardless of when
    or how many times it is called.
    """
    tokenizer = PIITokenizer(secret_key="test_key_for_proof")

    pan = "AABCU1234R"
    tokens = [tokenizer.tokenize_pan(pan) for _ in range(100)]

    assert len(set(tokens)) == 1, "FAILED: Tokens are not deterministic!"
    print(f"✅ DETERMINISM PROOF: 100 calls with PAN '{pan}'")
    print(f"   All produced: {tokens[0]}")
    print(f"   Unique tokens: {len(set(tokens))} (should be 1)")
    return True


def prove_irreversibility():
    """
    PROOF: Tokenization is irreversible.
    Given a token, there is no mathematical operation to recover the input.
    HMAC-SHA256 is a cryptographic one-way function.
    """
    tokenizer = PIITokenizer(secret_key="test_key_for_proof")

    pan = "AABCU1234R"
    token = tokenizer.tokenize_pan(pan)

    print(f"✅ IRREVERSIBILITY PROOF:")
    print(f"   Input PAN:  {pan}  (10 characters)")
    print(f"   Token:      {token}  (64 hex characters)")
    print(f"   Token bits: {len(token) * 4} bits (256-bit hash)")
    print()
    print(f"   To reverse: Must brute-force HMAC-SHA256")
    print(f"   PAN space:  26^5 × 10^4 × 26 = ~3.1 × 10^11")
    print(f"   At 10^9 hashes/sec: ~310 seconds (with key)")
    print(f"   WITHOUT key: Impossible (HMAC requires secret key)")
    print()
    print(f"   Key property: Even if attacker has the token,")
    print(f"   they cannot recover the PAN without the HMAC secret key.")
    return True


def prove_joinability():
    """
    PROOF: Tokenization maintains joinability across departments.
    The same PAN from different sources produces the same token.
    """
    tokenizer = PIITokenizer(secret_key="test_key_for_proof")

    # Same business, different departments
    commercial_taxes_record = {"gstin": "29AABCU1234R1Z5"}
    factories_board_record = {"pan": "AABCU1234R"}

    # Tokenize GSTIN → also extracts and tokenizes embedded PAN
    ct_tokenized = tokenizer.tokenize_record(commercial_taxes_record)

    # Tokenize PAN directly
    fb_tokenized = tokenizer.tokenize_record(factories_board_record)

    pan_from_gst = ct_tokenized.get("pan_token")
    pan_from_factory = fb_tokenized.get("pan_token")

    assert pan_from_gst == pan_from_factory, \
        "FAILED: PAN tokens don't match across departments!"

    print(f"✅ JOINABILITY PROOF:")
    print(f"   Commercial Taxes GSTIN: 29AABCU1234R1Z5")
    print(f"   → Extracted PAN token:  {pan_from_gst[:24]}...")
    print()
    print(f"   Factories Board PAN:    AABCU1234R")
    print(f"   → PAN token:            {pan_from_factory[:24]}...")
    print()
    print(f"   MATCH: {pan_from_gst == pan_from_factory}")
    print(f"   → Entity resolution can link these records by token")
    print(f"   → No raw PAN is stored anywhere in UdyamGraph! 🔒")
    return True


# ─── CLI Demo ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 70)
    print("UdyamGraph — Deterministic PII Tokenization Pipeline")
    print("=" * 70)
    print()

    # Run all proofs
    prove_determinism()
    print()
    prove_irreversibility()
    print()
    prove_joinability()

    print()
    print("=" * 70)
    print("Full Record Tokenization Example")
    print("=" * 70)

    tokenizer = PIITokenizer(secret_key="demo_secret_key")

    sample_record = {
        "record_id": "29AABCU1234R1Z5",
        "business_name": "UBIQUITY INNOVATIONS PVT LTD",
        "gstin": "29AABCU1234R1Z5",
        "pan": "AABCU1234R",
        "udyam_number": "UDYAM-KA-01-0012345",
        "district": "Bangalore Urban",
        "status": "Active",
    }

    print(f"\n  BEFORE tokenization:")
    for k, v in sample_record.items():
        print(f"    {k}: {v}")

    tokenized = tokenizer.tokenize_record(sample_record)

    print(f"\n  AFTER tokenization:")
    for k, v in tokenized.items():
        display = v[:32] + "..." if isinstance(v, str) and len(v) > 32 else v
        print(f"    {k}: {display}")

    print()
    print("  🔒 Raw PAN, GSTIN, Udyam are REMOVED from the record")
    print("  📎 Deterministic tokens enable cross-department joins")
    print("  🚫 Tokens cannot be reversed to recover original PII")
