"""
UdyamGraph — Neo4j Graph Schema & UBID Management
====================================================

Defines the graph schema for storing Unified Business IDs (UBIDs),
department records, and evidence trails. The graph model enables:

1. **UBID Clusters**: Each unique business gets a UBID node connected
   to all its department records across 40+ systems
2. **Evidence Trails**: Every linkage decision is stored as a
   LinkEvidence node with SHAP explanations for full audit
3. **Cross-Department Queries**: Find all records for a business,
   discover hidden connections, detect anomalies

Graph Schema:
─────────────
    (:UBID {ubid_id, created_at, status, canonical_name})
       │
       ├──[:HAS_RECORD]──▶ (:DeptRecord {dept, dept_id, pan_token, ...})
       │                        │
       │                        ├──[:LINKED_VIA]──▶ (:LinkEvidence {confidence, shap, ...})
       │                        │                         │
       │                        └──[:LINKED_VIA]──────────┘
       │
       └──[:HAS_RECORD]──▶ (:DeptRecord {dept, dept_id, ...})

Constraints:
    - UBID.ubid_id is unique
    - DeptRecord.(dept + dept_id) is unique
    - LinkEvidence.pair_id is unique
"""

import logging
import uuid
import json
from typing import Optional, Dict, Any, List, Tuple
from datetime import datetime, timezone

from neo4j import GraphDatabase, Driver, Session

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import config

logger = logging.getLogger("udyamgraph.graph")


# ─── Schema DDL ──────────────────────────────────────────────────────────────

SCHEMA_CONSTRAINTS = [
    # Unique constraints
    "CREATE CONSTRAINT ubid_unique IF NOT EXISTS FOR (u:UBID) REQUIRE u.ubid_id IS UNIQUE",
    "CREATE CONSTRAINT dept_record_unique IF NOT EXISTS FOR (d:DeptRecord) REQUIRE (d.department, d.dept_record_id) IS UNIQUE",
    "CREATE CONSTRAINT link_evidence_unique IF NOT EXISTS FOR (le:LinkEvidence) REQUIRE le.pair_id IS UNIQUE",
]

SCHEMA_INDEXES = [
    # Performance indexes
    "CREATE INDEX ubid_status IF NOT EXISTS FOR (u:UBID) ON (u.status)",
    "CREATE INDEX ubid_name IF NOT EXISTS FOR (u:UBID) ON (u.canonical_name)",
    "CREATE INDEX dept_record_pan IF NOT EXISTS FOR (d:DeptRecord) ON (d.pan_token)",
    "CREATE INDEX dept_record_gstin IF NOT EXISTS FOR (d:DeptRecord) ON (d.gstin_token)",
    "CREATE INDEX dept_record_dept IF NOT EXISTS FOR (d:DeptRecord) ON (d.department)",
    "CREATE INDEX link_evidence_confidence IF NOT EXISTS FOR (le:LinkEvidence) ON (le.confidence)",
    "CREATE TEXT INDEX dept_record_name IF NOT EXISTS FOR (d:DeptRecord) ON (d.business_name)",
]


# ─── Cypher Query Library ───────────────────────────────────────────────────

class CypherQueries:
    """
    All Cypher queries used by UdyamGraph, organized by operation type.
    Each query is documented with its purpose and example usage.
    """

    # ── UBID Creation ────────────────────────────────────────────────────

    CREATE_UBID = """
    CREATE (u:UBID {
        ubid_id: $ubid_id,
        canonical_name: $canonical_name,
        status: $status,
        created_at: datetime(),
        updated_at: datetime(),
        record_count: 0,
        department_count: 0
    })
    RETURN u
    """

    # ── Department Record Insertion ──────────────────────────────────────

    UPSERT_DEPT_RECORD = """
    MERGE (d:DeptRecord {department: $department, dept_record_id: $dept_record_id})
    ON CREATE SET
        d.business_name = $business_name,
        d.trade_name = $trade_name,
        d.pan_token = $pan_token,
        d.gstin_token = $gstin_token,
        d.legal_status = $legal_status,
        d.business_type = $business_type,
        d.district = $district,
        d.city = $city,
        d.pincode = $pincode,
        d.status = $status,
        d.registration_date = $registration_date,
        d.ingested_at = datetime(),
        d.raw_payload_hash = $raw_payload_hash
    ON MATCH SET
        d.business_name = $business_name,
        d.status = $status,
        d.updated_at = datetime()
    RETURN d
    """

    # ── Link Records to UBID ─────────────────────────────────────────────

    LINK_RECORD_TO_UBID = """
    MATCH (u:UBID {ubid_id: $ubid_id})
    MATCH (d:DeptRecord {department: $department, dept_record_id: $dept_record_id})
    MERGE (u)-[:HAS_RECORD]->(d)
    WITH u
    // Update record and department counts
    SET u.record_count = size([(u)-[:HAS_RECORD]->() | 1]),
        u.department_count = size(apoc.coll.toSet([(u)-[:HAS_RECORD]->(r) | r.department])),
        u.updated_at = datetime()
    RETURN u
    """

    # ── Store Link Evidence ──────────────────────────────────────────────

    CREATE_LINK_EVIDENCE = """
    MATCH (d1:DeptRecord {department: $dept_a, dept_record_id: $record_a_id})
    MATCH (d2:DeptRecord {department: $dept_b, dept_record_id: $record_b_id})
    CREATE (le:LinkEvidence {
        pair_id: $pair_id,
        confidence: $confidence,
        decision: $decision,
        shap_values: $shap_values,
        feature_values: $feature_values,
        explanation_summary: $explanation_summary,
        top_contributors: $top_contributors,
        method: $method,
        decided_by: $decided_by,
        created_at: datetime()
    })
    CREATE (d1)-[:LINKED_VIA]->(le)
    CREATE (d2)-[:LINKED_VIA]->(le)
    RETURN le
    """

    # ── Query: All Records for a UBID ────────────────────────────────────

    GET_UBID_CLUSTER = """
    // Find all department records linked to a specific UBID
    MATCH (u:UBID {ubid_id: $ubid_id})-[:HAS_RECORD]->(d:DeptRecord)
    RETURN u.ubid_id AS ubid,
           u.canonical_name AS business_name,
           u.status AS ubid_status,
           collect({
               department: d.department,
               dept_record_id: d.dept_record_id,
               business_name: d.business_name,
               pan_token: d.pan_token,
               district: d.district,
               status: d.status,
               ingested_at: toString(d.ingested_at)
           }) AS department_records,
           u.record_count AS total_records,
           u.department_count AS departments_covered
    """

    # ── Query: Evidence Trail for a Link ─────────────────────────────────

    GET_EVIDENCE_TRAIL = """
    // Traverse the evidence graph showing WHY two records were linked
    MATCH (d1:DeptRecord)-[:LINKED_VIA]->(le:LinkEvidence)<-[:LINKED_VIA]-(d2:DeptRecord)
    WHERE d1.dept_record_id = $record_id
       OR d2.dept_record_id = $record_id
    RETURN d1.department AS dept_a,
           d1.dept_record_id AS record_a,
           d1.business_name AS name_a,
           d2.department AS dept_b,
           d2.dept_record_id AS record_b,
           d2.business_name AS name_b,
           le.confidence AS confidence,
           le.decision AS decision,
           le.explanation_summary AS explanation,
           le.top_contributors AS top_factors,
           toString(le.created_at) AS linked_at
    ORDER BY le.confidence DESC
    """

    # ── Query: Cross-Department Discovery ────────────────────────────────

    FIND_CROSS_DEPT_CONNECTIONS = """
    // Find businesses registered across multiple departments
    MATCH (u:UBID)-[:HAS_RECORD]->(d:DeptRecord)
    WITH u, collect(DISTINCT d.department) AS departments, count(d) AS record_count
    WHERE size(departments) >= $min_departments
    RETURN u.ubid_id AS ubid,
           u.canonical_name AS business_name,
           departments,
           record_count
    ORDER BY record_count DESC
    LIMIT $limit
    """

    # ── Query: Find by PAN Token ─────────────────────────────────────────

    FIND_BY_PAN_TOKEN = """
    // Find all records and UBIDs associated with a PAN token
    MATCH (d:DeptRecord {pan_token: $pan_token})
    OPTIONAL MATCH (u:UBID)-[:HAS_RECORD]->(d)
    RETURN d.department AS department,
           d.dept_record_id AS dept_record_id,
           d.business_name AS business_name,
           d.status AS dept_status,
           u.ubid_id AS ubid,
           u.canonical_name AS canonical_name
    """

    # ── Query: Unlinked Records ──────────────────────────────────────────

    FIND_UNLINKED_RECORDS = """
    // Find department records not yet assigned to any UBID
    MATCH (d:DeptRecord)
    WHERE NOT (d)<-[:HAS_RECORD]-(:UBID)
    RETURN d.department AS department,
           d.dept_record_id AS dept_record_id,
           d.business_name AS business_name,
           d.pan_token AS pan_token
    ORDER BY d.ingested_at DESC
    LIMIT $limit
    """

    # ── Query: UBID Merge (combine two clusters) ─────────────────────────

    MERGE_UBIDS = """
    // Merge UBID B into UBID A (when discovered to be same entity)
    MATCH (ua:UBID {ubid_id: $ubid_keep})-[:HAS_RECORD]->(da:DeptRecord)
    MATCH (ub:UBID {ubid_id: $ubid_merge})
    // Move all records from B to A
    OPTIONAL MATCH (ub)-[:HAS_RECORD]->(db:DeptRecord)
    FOREACH (rec IN CASE WHEN db IS NOT NULL THEN [db] ELSE [] END |
        MERGE (ua)-[:HAS_RECORD]->(rec)
    )
    // Mark merged UBID as superseded
    SET ub.status = 'MERGED',
        ub.merged_into = $ubid_keep,
        ub.merged_at = datetime()
    // Update counts on surviving UBID
    WITH ua
    SET ua.record_count = size([(ua)-[:HAS_RECORD]->() | 1]),
        ua.department_count = size(apoc.coll.toSet([(ua)-[:HAS_RECORD]->(r) | r.department])),
        ua.updated_at = datetime()
    RETURN ua
    """

    # ── Query: Dashboard Statistics ──────────────────────────────────────

    GET_STATISTICS = """
    // Overall platform statistics
    MATCH (u:UBID) WHERE u.status <> 'MERGED'
    WITH count(u) AS total_ubids
    MATCH (d:DeptRecord)
    WITH total_ubids, count(d) AS total_records
    MATCH (le:LinkEvidence)
    WITH total_ubids, total_records, count(le) AS total_links,
         avg(le.confidence) AS avg_confidence
    RETURN total_ubids, total_records, total_links, avg_confidence
    """


# ─── Graph Manager ──────────────────────────────────────────────────────────

class UBIDGraphManager:
    """
    Manages the Neo4j graph for UBID storage and evidence trails.
    
    Handles schema creation, record insertion, UBID assignment,
    evidence storage, and all query operations.
    """

    def __init__(
        self,
        uri: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        database: Optional[str] = None,
    ):
        self.uri = uri or config.neo4j.uri
        self.username = username or config.neo4j.username
        self.password = password or config.neo4j.password
        self.database = database or config.neo4j.database
        self.driver: Optional[Driver] = None

    def connect(self):
        """Establish connection to Neo4j."""
        self.driver = GraphDatabase.driver(
            self.uri,
            auth=(self.username, self.password),
            max_connection_pool_size=config.neo4j.max_connection_pool_size,
        )
        # Verify connectivity
        self.driver.verify_connectivity()
        logger.info(f"Connected to Neo4j at {self.uri}")

    def initialize_schema(self):
        """Create constraints and indexes (idempotent)."""
        with self.driver.session(database=self.database) as session:
            for constraint in SCHEMA_CONSTRAINTS:
                try:
                    session.run(constraint)
                    logger.info(f"Applied: {constraint[:60]}...")
                except Exception as e:
                    logger.warning(f"Schema DDL warning: {e}")

            for index in SCHEMA_INDEXES:
                try:
                    session.run(index)
                    logger.info(f"Applied: {index[:60]}...")
                except Exception as e:
                    logger.warning(f"Index DDL warning: {e}")

        logger.info("Schema initialization complete")

    def generate_ubid(self) -> str:
        """Generate a new Unified Business ID."""
        return f"UBID-KA-{uuid.uuid4().hex[:12].upper()}"

    def create_ubid(
        self,
        canonical_name: str,
        status: str = "ACTIVE",
    ) -> str:
        """Create a new UBID node and return the UBID ID."""
        ubid_id = self.generate_ubid()

        with self.driver.session(database=self.database) as session:
            session.run(
                CypherQueries.CREATE_UBID,
                ubid_id=ubid_id,
                canonical_name=canonical_name,
                status=status,
            )

        logger.info(f"Created UBID: {ubid_id} for '{canonical_name}'")
        return ubid_id

    def upsert_department_record(
        self,
        record: Dict[str, Any],
    ) -> None:
        """Insert or update a department record in the graph."""
        with self.driver.session(database=self.database) as session:
            session.run(
                CypherQueries.UPSERT_DEPT_RECORD,
                department=record.get("department", ""),
                dept_record_id=record.get("record_id", ""),
                business_name=record.get("business_name", ""),
                trade_name=record.get("trade_name"),
                pan_token=record.get("pan_token"),
                gstin_token=record.get("gstin_token"),
                legal_status=record.get("legal_status"),
                business_type=record.get("business_type"),
                district=record.get("district"),
                city=record.get("city"),
                pincode=record.get("pincode"),
                status=record.get("status"),
                registration_date=record.get("registration_date"),
                raw_payload_hash=record.get("raw_payload_hash"),
            )

    def link_record_to_ubid(
        self,
        ubid_id: str,
        department: str,
        dept_record_id: str,
    ) -> None:
        """Link a department record to a UBID."""
        with self.driver.session(database=self.database) as session:
            session.run(
                CypherQueries.LINK_RECORD_TO_UBID,
                ubid_id=ubid_id,
                department=department,
                dept_record_id=dept_record_id,
            )
        logger.info(
            f"Linked {department}/{dept_record_id} → {ubid_id}"
        )

    def store_link_evidence(
        self,
        record_a_id: str,
        dept_a: str,
        record_b_id: str,
        dept_b: str,
        confidence: float,
        decision: str,
        shap_values: Dict[str, float],
        feature_values: Dict[str, float],
        explanation_summary: str,
        top_contributors: List[Dict[str, Any]],
        method: str = "xgboost_sbert",
        decided_by: str = "auto",
    ) -> None:
        """Store evidence for a linkage decision."""
        pair_id = f"{record_a_id}___{record_b_id}"

        with self.driver.session(database=self.database) as session:
            session.run(
                CypherQueries.CREATE_LINK_EVIDENCE,
                pair_id=pair_id,
                dept_a=dept_a,
                record_a_id=record_a_id,
                dept_b=dept_b,
                record_b_id=record_b_id,
                confidence=confidence,
                decision=decision,
                shap_values=json.dumps(shap_values),
                feature_values=json.dumps(feature_values),
                explanation_summary=explanation_summary,
                top_contributors=json.dumps(top_contributors),
                method=method,
                decided_by=decided_by,
            )

    def get_ubid_cluster(self, ubid_id: str) -> Optional[Dict[str, Any]]:
        """Get all department records linked to a UBID."""
        with self.driver.session(database=self.database) as session:
            result = session.run(
                CypherQueries.GET_UBID_CLUSTER,
                ubid_id=ubid_id,
            )
            record = result.single()
            return dict(record) if record else None

    def get_evidence_trail(self, record_id: str) -> List[Dict[str, Any]]:
        """Get all evidence connecting a record to others."""
        with self.driver.session(database=self.database) as session:
            result = session.run(
                CypherQueries.GET_EVIDENCE_TRAIL,
                record_id=record_id,
            )
            return [dict(r) for r in result]

    def find_by_pan_token(self, pan_token: str) -> List[Dict[str, Any]]:
        """Find all records associated with a PAN token."""
        with self.driver.session(database=self.database) as session:
            result = session.run(
                CypherQueries.FIND_BY_PAN_TOKEN,
                pan_token=pan_token,
            )
            return [dict(r) for r in result]

    def find_cross_department_businesses(
        self,
        min_departments: int = 2,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """Find businesses registered across multiple departments."""
        with self.driver.session(database=self.database) as session:
            result = session.run(
                CypherQueries.FIND_CROSS_DEPT_CONNECTIONS,
                min_departments=min_departments,
                limit=limit,
            )
            return [dict(r) for r in result]

    def get_statistics(self) -> Dict[str, Any]:
        """Get platform-wide statistics."""
        with self.driver.session(database=self.database) as session:
            result = session.run(CypherQueries.GET_STATISTICS)
            record = result.single()
            return dict(record) if record else {}

    def close(self):
        """Close the Neo4j driver."""
        if self.driver:
            self.driver.close()
            logger.info("Neo4j connection closed")


# ─── CLI Demo ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 70)
    print("UdyamGraph — Neo4j Graph Schema & UBID Management")
    print("=" * 70)
    print()

    print("📐 GRAPH SCHEMA DESIGN")
    print("─" * 50)
    print()
    print("  Node Types:")
    print("    • (:UBID)          — Unified Business ID cluster")
    print("    • (:DeptRecord)    — Department-specific record")
    print("    • (:LinkEvidence)  — Entity resolution evidence")
    print()
    print("  Relationships:")
    print("    • (UBID)-[:HAS_RECORD]->(DeptRecord)")
    print("    • (DeptRecord)-[:LINKED_VIA]->(LinkEvidence)")
    print()

    print("📋 CYPHER QUERY EXAMPLES")
    print("─" * 50)
    print()

    print("  1. Find all records for a business (UBID cluster):")
    print("     MATCH (u:UBID {ubid_id: 'UBID-KA-A1B2C3D4E5F6'})")
    print("           -[:HAS_RECORD]->(d:DeptRecord)")
    print("     RETURN u.canonical_name, collect(d.department)")
    print()

    print("  2. Traverse evidence graph:")
    print("     MATCH (d1:DeptRecord)-[:LINKED_VIA]->(le:LinkEvidence)")
    print("           <-[:LINKED_VIA]-(d2:DeptRecord)")
    print("     WHERE d1.dept_record_id = '29AABCU1234R1Z5'")
    print("     RETURN le.confidence, le.explanation_summary")
    print()

    print("  3. Cross-department discovery:")
    print("     MATCH (u:UBID)-[:HAS_RECORD]->(d:DeptRecord)")
    print("     WITH u, collect(DISTINCT d.department) AS depts")
    print("     WHERE size(depts) >= 3")
    print("     RETURN u.canonical_name, depts")
    print()

    print("  4. Find by PAN token:")
    print("     MATCH (d:DeptRecord {pan_token: $pan_token})")
    print("     OPTIONAL MATCH (u:UBID)-[:HAS_RECORD]->(d)")
    print("     RETURN d.department, u.ubid_id")
    print()

    # Demo UBID generation
    mgr = UBIDGraphManager()
    sample_ubids = [mgr.generate_ubid() for _ in range(3)]
    print("🆔 SAMPLE UBID GENERATION")
    print("─" * 50)
    for uid in sample_ubids:
        print(f"    {uid}")
