"""
UdyamGraph — Zero-Touch Kafka Ingestion
========================================

Reads from 3 Karnataka department systems via their EXISTING interfaces
(APIs, databases, file feeds) WITHOUT requiring any schema changes,
agents, or modifications on source systems.

Architecture:
    Source System → Kafka Connect (JDBC/REST/File) → Kafka Topic → UdyamGraph Consumer

Each department has a dedicated consumer that normalizes data into
the canonical BusinessRecord schema for downstream processing.
"""

import json
import logging
import hashlib
from datetime import datetime, timezone
from typing import Optional, Dict, Any, Generator, List
from dataclasses import dataclass, field, asdict
from enum import Enum

from kafka import KafkaConsumer, TopicPartition
from kafka.errors import KafkaError

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import config

logger = logging.getLogger("udyamgraph.ingestion")


# ─── Canonical Data Model ──────────────────────────────────────────────────────

class DepartmentSource(str, Enum):
    """Karnataka department systems integrated via zero-touch connectors."""
    COMMERCIAL_TAXES = "commercial_taxes"         # GSTIN registry
    FACTORIES_BOARD = "factories_board"           # Factory licenses
    SHOPS_ESTABLISHMENTS = "shops_establishments"  # Shop registrations
    FSSAI = "fssai"                               # Food safety licenses
    DRUG_LICENSING = "drug_licensing"             # Pharma licenses
    LABOUR_DEPT = "labour_department"             # Labour registrations


@dataclass
class BusinessRecord:
    """
    Canonical business record — the common schema all department
    records are normalized into. Source systems are NEVER modified;
    normalization happens entirely within UdyamGraph.
    """
    # Identifiers (raw — will be tokenized by PII layer)
    record_id: str                    # Unique within source system
    department: DepartmentSource      # Source department
    gstin: Optional[str] = None       # e.g., 29AABCU1234R1Z5
    pan: Optional[str] = None         # e.g., AABCU1234R
    udyam_number: Optional[str] = None  # e.g., UDYAM-KA-01-0012345

    # Business details
    business_name: str = ""
    trade_name: Optional[str] = None
    legal_status: Optional[str] = None  # Proprietorship/Partnership/Pvt Ltd/LLP
    business_type: Optional[str] = None  # Manufacturing/Trading/Services

    # Address
    address_line1: Optional[str] = None
    address_line2: Optional[str] = None
    city: Optional[str] = None
    district: Optional[str] = None
    pincode: Optional[str] = None
    state: str = "Karnataka"

    # Contact
    phone: Optional[str] = None
    email: Optional[str] = None

    # Registration details
    registration_date: Optional[str] = None
    last_filing_date: Optional[str] = None
    status: Optional[str] = None  # Active/Suspended/Cancelled

    # Metadata
    ingested_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    raw_payload_hash: Optional[str] = None  # SHA-256 of original payload

    def to_dict(self) -> Dict[str, Any]:
        return {k: v for k, v in asdict(self).items() if v is not None}


# ─── Department-Specific Normalizers ──────────────────────────────────────────

def _normalize_commercial_taxes(raw: Dict[str, Any]) -> BusinessRecord:
    """
    Normalize Commercial Taxes Department (GST) data.

    Source: GSTIN registry — typically accessed via GSTN API or
    read-only replica of state GST database.

    Zero-intrusion: Reads from existing GSTN public/authorized API.
    No agents installed on the tax department infrastructure.
    """
    return BusinessRecord(
        record_id=raw.get("gstin", raw.get("id", "")),
        department=DepartmentSource.COMMERCIAL_TAXES,
        gstin=raw.get("gstin"),                     # 29AABCU1234R1Z5
        pan=raw.get("gstin", "")[2:12] if raw.get("gstin") else None,  # Extract PAN from GSTIN
        business_name=raw.get("legal_name", raw.get("trade_name", "")),
        trade_name=raw.get("trade_name"),
        legal_status=raw.get("constitution_of_business"),
        business_type=raw.get("nature_of_business"),
        address_line1=raw.get("principal_place_address", {}).get("building", ""),
        address_line2=raw.get("principal_place_address", {}).get("street", ""),
        city=raw.get("principal_place_address", {}).get("city", ""),
        district=raw.get("principal_place_address", {}).get("district", ""),
        pincode=raw.get("principal_place_address", {}).get("pincode", ""),
        registration_date=raw.get("registration_date"),
        last_filing_date=raw.get("last_return_filed_date"),
        status=raw.get("gst_status"),  # Active/Suspended/Cancelled
        raw_payload_hash=hashlib.sha256(
            json.dumps(raw, sort_keys=True).encode()
        ).hexdigest()
    )


def _normalize_factories_board(raw: Dict[str, Any]) -> BusinessRecord:
    """
    Normalize Factories Board license data.

    Source: Karnataka Factories Board — typically a legacy Oracle/PostgreSQL
    database accessed via Kafka Connect JDBC Source Connector with
    read-only credentials.

    Zero-intrusion: JDBC connector reads via SELECT queries only.
    No triggers, stored procedures, or schema changes on source DB.
    """
    return BusinessRecord(
        record_id=raw.get("factory_license_no", raw.get("id", "")),
        department=DepartmentSource.FACTORIES_BOARD,
        pan=raw.get("pan_of_occupier"),
        business_name=raw.get("factory_name", ""),
        trade_name=raw.get("factory_name"),
        legal_status=raw.get("ownership_type"),
        business_type="Manufacturing",
        address_line1=raw.get("factory_address", ""),
        city=raw.get("city"),
        district=raw.get("district"),
        pincode=raw.get("pin_code"),
        phone=raw.get("contact_no"),
        email=raw.get("email"),
        registration_date=raw.get("license_issue_date"),
        last_filing_date=raw.get("last_renewal_date"),
        status=_map_factory_status(raw.get("license_status")),
        raw_payload_hash=hashlib.sha256(
            json.dumps(raw, sort_keys=True).encode()
        ).hexdigest()
    )


def _normalize_shops_establishments(raw: Dict[str, Any]) -> BusinessRecord:
    """
    Normalize Shops & Establishments registration data.

    Source: Karnataka Shops & Establishments Act portal — typically
    accessed via REST API (e-Shram / state portal APIs).

    Zero-intrusion: Polls existing public REST endpoints.
    No middleware or agents deployed on the department's servers.
    """
    return BusinessRecord(
        record_id=raw.get("registration_number", raw.get("id", "")),
        department=DepartmentSource.SHOPS_ESTABLISHMENTS,
        pan=raw.get("pan"),
        gstin=raw.get("gstin"),
        business_name=raw.get("establishment_name", raw.get("shop_name", "")),
        trade_name=raw.get("shop_name"),
        legal_status=raw.get("type_of_establishment"),
        business_type=raw.get("nature_of_business"),
        address_line1=raw.get("address", ""),
        city=raw.get("city", raw.get("taluk", "")),
        district=raw.get("district"),
        pincode=raw.get("pincode"),
        phone=raw.get("phone"),
        email=raw.get("email"),
        registration_date=raw.get("date_of_registration"),
        last_filing_date=raw.get("last_renewal_date"),
        status=raw.get("registration_status"),
        raw_payload_hash=hashlib.sha256(
            json.dumps(raw, sort_keys=True).encode()
        ).hexdigest()
    )


def _map_factory_status(status: Optional[str]) -> Optional[str]:
    """Map factory-specific statuses to canonical form."""
    if not status:
        return None
    mapping = {
        "VALID": "Active",
        "RENEWED": "Active",
        "EXPIRED": "Suspended",
        "REVOKED": "Cancelled",
        "PENDING_RENEWAL": "Active",
    }
    return mapping.get(status.upper(), status)


# ─── Normalizer Registry ─────────────────────────────────────────────────────

NORMALIZERS = {
    DepartmentSource.COMMERCIAL_TAXES: _normalize_commercial_taxes,
    DepartmentSource.FACTORIES_BOARD: _normalize_factories_board,
    DepartmentSource.SHOPS_ESTABLISHMENTS: _normalize_shops_establishments,
}


# ─── Kafka Consumer Factory ──────────────────────────────────────────────────

class DepartmentConsumer:
    """
    Zero-touch Kafka consumer for a specific department system.

    This consumer reads from a Kafka topic that is populated by
    Kafka Connect connectors (JDBC Source, REST Source, or FilePulse)
    reading from department systems. The connectors use read-only
    access — no writes, no schema changes, no agents on source systems.

    Architecture:
    ┌──────────────┐    ┌──────────────┐    ┌───────────────┐    ┌──────────────┐
    │  Dept System  │───▶│ Kafka Connect│───▶│  Kafka Topic  │───▶│ This Consumer│
    │  (Untouched)  │    │  (Read-Only) │    │               │    │ (Normalize)  │
    └──────────────┘    └──────────────┘    └───────────────┘    └──────────────┘
    """

    def __init__(
        self,
        department: DepartmentSource,
        topic: str,
        group_id: Optional[str] = None,
    ):
        self.department = department
        self.topic = topic
        self.normalizer = NORMALIZERS.get(department)

        if not self.normalizer:
            raise ValueError(
                f"No normalizer registered for department: {department}"
            )

        # Configure Kafka consumer with department-specific group
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=config.kafka.bootstrap_servers,
            group_id=group_id or f"{config.kafka.group_id}-{department.value}",
            auto_offset_reset=config.kafka.auto_offset_reset,
            enable_auto_commit=config.kafka.enable_auto_commit,
            max_poll_records=config.kafka.max_poll_records,
            session_timeout_ms=config.kafka.session_timeout_ms,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            key_deserializer=lambda m: m.decode("utf-8") if m else None,
        )

        logger.info(
            f"Initialized consumer for {department.value} "
            f"on topic '{topic}' "
            f"(bootstrap: {config.kafka.bootstrap_servers})"
        )

    def consume(
        self, max_records: Optional[int] = None
    ) -> Generator[BusinessRecord, None, None]:
        """
        Consume and normalize records from the department topic.

        Yields BusinessRecord instances normalized from raw department data.
        Commits offsets only after successful processing downstream.
        """
        count = 0
        try:
            for message in self.consumer:
                try:
                    raw_data = message.value
                    record = self.normalizer(raw_data)

                    logger.debug(
                        f"[{self.department.value}] Normalized record: "
                        f"{record.record_id} — {record.business_name}"
                    )

                    yield record
                    count += 1

                    # Manual commit after successful processing
                    self.consumer.commit()

                    if max_records and count >= max_records:
                        logger.info(
                            f"Reached max_records={max_records} for "
                            f"{self.department.value}"
                        )
                        break

                except Exception as e:
                    logger.error(
                        f"[{self.department.value}] Failed to process "
                        f"message at offset {message.offset}: {e}",
                        exc_info=True
                    )
                    # Dead-letter queue pattern: skip and continue
                    continue

        except KafkaError as e:
            logger.error(f"Kafka consumer error: {e}", exc_info=True)
            raise
        finally:
            logger.info(
                f"[{self.department.value}] Consumed {count} records"
            )

    def close(self):
        """Gracefully close the consumer."""
        self.consumer.close()
        logger.info(f"Closed consumer for {self.department.value}")


# ─── Multi-Department Ingestion Orchestrator ──────────────────────────────────

class IngestionOrchestrator:
    """
    Orchestrates ingestion from all configured department systems.
    Runs consumers concurrently and feeds normalized records to
    the entity resolution pipeline.
    """

    # Default topic mapping for Karnataka departments
    DEFAULT_TOPICS = {
        DepartmentSource.COMMERCIAL_TAXES: "dept.commercial-taxes.businesses",
        DepartmentSource.FACTORIES_BOARD: "dept.factories-board.licenses",
        DepartmentSource.SHOPS_ESTABLISHMENTS: "dept.shops-establishments.registrations",
    }

    def __init__(self, topics: Optional[Dict[DepartmentSource, str]] = None):
        self.topics = topics or self.DEFAULT_TOPICS
        self.consumers: Dict[DepartmentSource, DepartmentConsumer] = {}

    def initialize(self):
        """Create consumers for all configured departments."""
        for dept, topic in self.topics.items():
            try:
                self.consumers[dept] = DepartmentConsumer(
                    department=dept,
                    topic=topic
                )
            except Exception as e:
                logger.error(
                    f"Failed to initialize consumer for {dept.value}: {e}"
                )

    def ingest_all(
        self, max_per_department: int = 1000
    ) -> Generator[BusinessRecord, None, None]:
        """
        Round-robin ingest from all department consumers.
        Yields normalized BusinessRecord instances.
        """
        generators = {
            dept: consumer.consume(max_records=max_per_department)
            for dept, consumer in self.consumers.items()
        }

        active = set(generators.keys())
        while active:
            for dept in list(active):
                try:
                    record = next(generators[dept])
                    yield record
                except StopIteration:
                    active.discard(dept)
                    logger.info(f"Exhausted records from {dept.value}")

    def shutdown(self):
        """Gracefully close all consumers."""
        for dept, consumer in self.consumers.items():
            consumer.close()
        logger.info("All department consumers shut down")


# ─── Kafka Connect Configurations (JSON) ──────────────────────────────────────
# These are deployed to Kafka Connect REST API — NO agents on source systems.

CONNECT_CONFIGS = {
    "commercial_taxes_jdbc": {
        # JDBC Source Connector — reads from GST database via SELECT only
        "name": "source-commercial-taxes-jdbc",
        "config": {
            "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
            "connection.url": "jdbc:postgresql://gst-db.karnataka.gov.in:5432/gst_registry",
            "connection.user": "readonly_udyamgraph",
            "connection.password": "${file:/secrets/gst_db_password}",
            "mode": "timestamp+incrementing",
            "incrementing.column.name": "id",
            "timestamp.column.name": "updated_at",
            "table.whitelist": "gst_registrations,gst_returns_summary",
            "topic.prefix": "dept.commercial-taxes.",
            "poll.interval.ms": 60000,  # Poll every 60 seconds
            "batch.max.rows": 1000,
            "transforms": "createKey,extractFields",
            "transforms.createKey.type": "org.apache.kafka.connect.transforms.ValueToKey",
            "transforms.createKey.fields": "gstin",
            "transforms.extractFields.type": "org.apache.kafka.connect.transforms.ReplaceField$Value",
            "transforms.extractFields.renames": "legal_name:business_name",
        }
    },
    "factories_board_jdbc": {
        # JDBC Source Connector — reads from Factories Board Oracle DB
        "name": "source-factories-board-jdbc",
        "config": {
            "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
            "connection.url": "jdbc:oracle:thin:@factories-db.karnataka.gov.in:1521/FACTDB",
            "connection.user": "readonly_udyamgraph",
            "connection.password": "${file:/secrets/factories_db_password}",
            "mode": "timestamp",
            "timestamp.column.name": "LAST_MODIFIED",
            "table.whitelist": "FACTORY_LICENSES,FACTORY_INSPECTIONS",
            "topic.prefix": "dept.factories-board.",
            "poll.interval.ms": 300000,  # 5 minutes
            "batch.max.rows": 500,
        }
    },
    "shops_establishments_rest": {
        # REST Source Connector — polls Shops & Establishments API
        "name": "source-shops-establishments-rest",
        "config": {
            "connector.class": "com.tm.kafka.connect.rest.RestSourceConnector",
            "rest.source.url": "https://seportal.karnataka.gov.in/api/v1/registrations",
            "rest.source.method": "GET",
            "rest.source.headers": "Authorization:Bearer ${file:/secrets/se_api_token},Accept:application/json",
            "rest.source.poll.interval.ms": 120000,  # 2 minutes
            "rest.source.topic.selector": "com.tm.kafka.connect.rest.selector.SimpleTopicSelector",
            "rest.source.destination.topics": "dept.shops-establishments.registrations",
            "rest.http.connection.timeout": 30000,
            "rest.http.read.timeout": 30000,
            # Pagination support
            "rest.source.url.strategy": "com.tm.kafka.connect.rest.url.PagedUrlStrategy",
            "rest.source.url.strategy.page.size": 100,
        }
    },
}


# ─── Usage Example ────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    if sys.stdout.encoding.lower() != 'utf-8':
        sys.stdout.reconfigure(encoding='utf-8')
    logging.basicConfig(level=logging.INFO)

    print("=" * 70)
    print("UdyamGraph — Zero-Touch Ingestion Demo")
    print("=" * 70)
    print()
    print("This module reads from Karnataka department systems via Kafka")
    print("without requiring ANY changes to source systems.")
    print()
    print("Department → Kafka Connect (read-only) → Kafka → UdyamGraph Consumer")
    print()

    # Show Kafka Connect configurations
    for name, cfg in CONNECT_CONFIGS.items():
        print(f"\n📡 Connector: {cfg['name']}")
        print(f"   Class: {cfg['config']['connector.class']}")
        if 'connection.url' in cfg['config']:
            print(f"   Source: {cfg['config']['connection.url']}")
        if 'rest.source.url' in cfg['config']:
            print(f"   Source: {cfg['config']['rest.source.url']}")
        print(f"   → Zero intrusion: read-only access, no schema changes")

    # Demo normalizer with sample data
    print("\n" + "=" * 70)
    print("Sample Normalization — Commercial Taxes (GST)")
    print("=" * 70)

    sample_gst = {
        "gstin": "29AABCU1234R1Z5",
        "legal_name": "UBIQUITY INNOVATIONS PVT LTD",
        "trade_name": "Ubiquity Tech",
        "constitution_of_business": "Private Limited Company",
        "nature_of_business": "IT Services",
        "registration_date": "2019-07-01",
        "last_return_filed_date": "2024-12-31",
        "gst_status": "Active",
        "principal_place_address": {
            "building": "42, Koramangala 4th Block",
            "street": "80 Feet Road",
            "city": "Bengaluru",
            "district": "Bangalore Urban",
            "pincode": "560034"
        }
    }

    record = _normalize_commercial_taxes(sample_gst)
    print(f"\n  Record ID:  {record.record_id}")
    print(f"  GSTIN:      {record.gstin}")
    print(f"  PAN:        {record.pan}")
    print(f"  Name:       {record.business_name}")
    print(f"  District:   {record.district}")
    print(f"  Status:     {record.status}")
    print(f"  Department: {record.department.value}")

    print("\n" + "=" * 70)
    print("Sample Normalization — Factories Board")
    print("=" * 70)

    sample_factory = {
        "factory_license_no": "KA-BLR-FAC-2020-00456",
        "factory_name": "UBIQUITY MANUFACTURING UNIT",
        "pan_of_occupier": "AABCU1234R",
        "ownership_type": "Private Limited Company",
        "factory_address": "Plot 42, KIADB Industrial Area, Peenya",
        "city": "Bengaluru",
        "district": "Bangalore Urban",
        "pin_code": "560058",
        "contact_no": "9876543210",
        "email": "factory@ubiquity.in",
        "license_issue_date": "2020-03-15",
        "last_renewal_date": "2024-03-15",
        "license_status": "RENEWED"
    }

    record2 = _normalize_factories_board(sample_factory)
    print(f"\n  Record ID:  {record2.record_id}")
    print(f"  PAN:        {record2.pan}")
    print(f"  Name:       {record2.business_name}")
    print(f"  Status:     {record2.status}")
    print(f"  Department: {record2.department.value}")
    print()
    print("✅ Both records share PAN 'AABCU1234R' — entity resolution")
    print("   will detect this as a candidate match!")
